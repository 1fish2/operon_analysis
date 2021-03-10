"""Script to download just the needed files from a wcEcoli sim workflow run."""

import json
import re
import os
from typing import List

from analysis.storage import CloudStorage


BUCKET = 'sisyphus-mialydefelice-2'
WORKFLOW_NAME = '20210228.075124__100_Seeds_32_gens_master_branch'
VARIANT = 'wildtype_000000'
METADATA_FILENAME = os.path.join('metadata', 'metadata.json')

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
    def __init__(self, bucket: str,
                 wcm_workflow_name: str,
                 variant_name: str = VARIANT,
                 local_dir: str = '') -> None:
        storage_prefix = os.path.join(bucket, 'WCM', wcm_workflow_name)
        self.variant_name = variant_name
        self.local_dir = local_dir or wcm_workflow_name

        self.storage = CloudStorage(storage_prefix)
        self.simout_dirs: List[str] = []

        # later: download metadata.json and read these fields
        self.generations = 0
        self.init_sims = 0
        self.seed = 0

    def download_file(self, storage_path: str):
        """Download a file to the same relative path in the local_dir, and
        return the local path."""
        local_path = os.path.join(self.local_dir, storage_path)
        print(f'Downloading {storage_path}')
        self.storage.download_file(storage_path, local_path)
        return local_path

    def download_files(self, prefix: str, storage_paths: List[str]):
        for p in storage_paths:
            self.download_file(os.path.join(prefix, p))

    def download_metadata(self) -> int:
        """Download the workflow output's metadata.json file, read fields, and
        return its number of generations."""
        local_name = self.download_file(METADATA_FILENAME)

        with open(local_name) as fp:
            metadata = json.load(fp)
            self.generations = metadata['generations']
            self.init_sims = metadata['init_sims']
            self.seed = metadata['seed']

        return self.generations

    def download_simdata_modified(self):
        """Download the 'VARIANT/kb/simData_Modified.cPickle'."""
        path = os.path.join(self.variant_name, 'kb', 'simData_Modified.cPickle')
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

    def download_all_needed_files(self):
        """Download all the needed files for the current analysis."""
        # TODO(jerry): Parallel downloads would be faster...
        generations = self.download_metadata()
        print(f'{generations=}')
        self.download_simdata_modified()

        seed_dirs = self.find_successful_seed_dirs(generations - 1)
        for seed_dir in seed_dirs:
            for gen in range(generations):
                sim_out = os.path.join(
                    seed_dir, f'generation_{gen:06d}', '000000', 'simOut')
                self.download_files(sim_out, SIM_FILES)
