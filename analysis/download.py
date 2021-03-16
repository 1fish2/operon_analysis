"""Utility to download just the needed files (selected Table columns and some
metadata) for further analysis processing of the Google Cloud Storage output
from a wcEcoli sim workflow run.

This skips wcEcoli seed lines that failed before the requested number of sim
generations. That can happen due to ODE solver instabilities. Turn on swap space
so Out-Of-Memory won't cause sim failures.

TODO(jerry): More error recovery, e.g. timeouts and more retries.

TODO(jerry): Maybe skip downloads that would replace existing files so rerunning
the script will fetch the missing ones. That assumes the GCS files haven't
changed, or cross-check their blob generation numbers to handle that.
"""

import concurrent.futures as cf
import json
import re
import os
import time
from typing import Dict, List

from analysis.storage import CloudStorage


VARIANT = 'wildtype_000000'
METADATA_PATHNAME = os.path.join('metadata', 'metadata.json')
SIMDATA_MODIFIED_SUBPATH = os.path.join('kb', 'simData_Modified.cPickle')

SIM_FILES = [
    'Mass/attributes.json',
    'Mass/cellMass',
    'Mass/dryMass',
    'Main/attributes.json',
    'Main/time',
    'MonomerCounts/attributes.json',
    'MonomerCounts/monomerCounts',
]


def removeprefix(string: str, prefix: str) -> str:
    """Like string.removeprefix(prefix) in Python 3.9."""
    return string[len(prefix):] if string.startswith(prefix) else string


def removesuffix(string: str, suffix: str) -> str:
    """Like string.removesuffix(suffix) in Python 3.9."""
    return string[:-len(suffix)] if string.endswith(suffix) else string


class DownloadSims:
    """Downloader for the needed files of relevant simOut/ dirs."""

    def __init__(self,
                 bucket: str,
                 wcm_workflow_name: str,
                 variant_name: str = VARIANT,
                 local_dir: str = '') -> None:
        """Construct a WCM sim downloader.
        Args:
            bucket: GCS storage bucket.
            wcm_workflow_name: WCM/<workflow> name.
            variant_name: WCM variant; default = 'wildtype_000000'.
            local_dir: the local directory path to download into.
        """
        storage_prefix = os.path.join(bucket, 'WCM', wcm_workflow_name)
        self.variant_name = variant_name
        self.local_dir = os.path.join(local_dir or wcm_workflow_name, '')

        self.storage = CloudStorage(storage_prefix)
        self.simout_dirs: List[str] = []

        self.queue: Dict[str, bool] = {}  # ordered queue of paths to download
        self.count = 0

        # later: download metadata.json and read these fields
        self.generations = 0
        self.init_sims = 0
        self.seed = 0

    def download_file(self, relative_path: str):
        """Download a file from the relative_path (relative to storage_prefix)
        to the same path (relative to the local_dir), and return the local path.
        """
        local_path = os.path.join(self.local_dir, relative_path)
        self.storage.download_file(relative_path, local_path)
        self.count += 1
        print(f'Downloaded: {relative_path}')
        return local_path

    def queue_files(self, sub_dir: str, relative_paths: List[str]) -> None:
        """Queue the given list of files to download from/to a sub_dir."""
        for p in relative_paths:
            self.queue[os.path.join(sub_dir, p)] = True

    def serial_download(self) -> None:
        """Download the queued files in series. Remove successes from the queue."""
        queue = dict(self.queue)

        for path in queue:
            try:
                self.download_file(path)
            except Exception as e:
                print(f'Download failed: {path}: {e!r}')
            else:
                del self.queue[path]

    def parallel_download(self) -> None:
        """Download the queued files in parallel. Remove successes from the queue."""
        fn = self.download_file

        with cf.ThreadPoolExecutor(thread_name_prefix='Downloader-') as executor:
            future_to_path = {executor.submit(fn, path): path for path in self.queue}

            for future in cf.as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    future.result()
                except Exception as e:
                    print(f'Download failed: {path}: {e!r}')
                else:
                    del self.queue[path]

    def download_metadata(self) -> int:
        """Download the workflow output's metadata.json file, read fields, and
        return its number of generations."""
        local_path = self.download_file(METADATA_PATHNAME)

        with open(local_path) as fp:
            metadata = json.load(fp)
            self.generations = metadata['generations']
            self.init_sims = metadata['init_sims']
            self.seed = metadata['seed']

        return self.generations

    def download_simdata_modified(self):
        """Download the variant's simData_Modified.cPickle."""
        path = os.path.join(self.variant_name, SIMDATA_MODIFIED_SUBPATH)
        self.download_file(path)

    def find_successful_seed_dirs(self, max_gen: int):
        """Find all VARIANT/SEED/ subpaths (e.g. 'wildtype_000000/000001/') with
        simulations that succeeded through generation max_gen (e.g. 31).
        """
        # List storage paths 'VARIANT/0*' to find seed subdirs up to '099999'.
        # '0' skips the '/' dir itself and its subdirs like 'kb/'.
        seed_0_star = os.path.join(self.variant_name, '0')
        prefix = self.storage.path_prefix
        seed_paths = [removeprefix(blob.name, prefix) for blob in
                      self.storage.list_blobs(seed_0_star, star=True)]

        # Find the seed subdirs that successfully wrote last-generation output.
        marker = os.path.join(
            f'generation_{max_gen:06d}', '000000', 'simOut',
            'Daughter1_inherited_state.cPickle')
        iterators = [self.storage.list_blobs(os.path.join(seed, marker))
                     for seed in seed_paths]

        # Extract the VARIANT/SEED/ subpaths.
        r = re.compile(re.escape(prefix) + r'(.*)generation_\d{6}')
        dirs = [[r.match(b.name)[1] for b in it] for it in iterators]
        return sum(dirs, [])  # flatten

    def download_all_needed_files(self) -> int:
        """Download all the needed files from this WCM workflow.
        Returns the count of files downloaded."""
        start_secs = time.monotonic()
        print(f'Downloading from {self.storage.url()} to {self.local_dir}')

        generations = self.download_metadata()
        print(f'  {generations=}')

        self.queue_files(self.variant_name, [SIMDATA_MODIFIED_SUBPATH])

        seed_dirs = self.find_successful_seed_dirs(generations - 1)
        for seed_dir in seed_dirs:
            for gen in range(generations):
                sim_out = os.path.join(
                    seed_dir, f'generation_{gen:06d}', '000000', 'simOut')
                self.queue_files(sim_out, SIM_FILES)

        self.parallel_download()
        self.serial_download()  # retry any failures serially
        # TODO(jerry): More retries?

        elapsed_secs = time.monotonic() - start_secs
        print(f'-- Downloaded {self.count} files into {self.local_dir} in'
              f' {elapsed_secs:1.1f} seconds\n')
        return self.count
