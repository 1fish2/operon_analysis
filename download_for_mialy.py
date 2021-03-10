from typing import List

from analysis.download import DownloadSims


BUCKET = 'sisyphus-mialydefelice-2'
VARIANT = 'wildtype_000000'

MASTER_WORKFLOWS = [
    '20210228.075124__100_Seeds_32_gens_master_branch',
    '20210301.130041__25_Seeds_32_gens_master_branch_start_at_300',
    '20210302.102113__2_Seeds_32_gens_master_branch_start_at_330',
]
OPERON_WORKFLOWS = [
    '20210304.082648__100_Seeds_32_gens_operon_branch',
    '20210305.122209__30_Seeds_32_gens_operon_branch_start_at_300',
]

LOCAL_MASTER = 'master_branch'
LOCAL_OPERON = 'operon_branch'


def download_workflows(bucket: str, workflows: List[str], to_local_dir: str) -> int:
    count = 0
    for workflow in workflows:
        ds = DownloadSims(bucket=bucket,
                          wcm_workflow_name=workflow,
                          variant_name=VARIANT,
                          local_dir=to_local_dir)
        count += ds.download_all_needed_files()
    return count


def download_master_workflows():
    download_workflows(BUCKET, MASTER_WORKFLOWS, LOCAL_MASTER)


def download_operon_workflows():
    download_workflows(BUCKET, OPERON_WORKFLOWS, LOCAL_OPERON)


def download_all():
    download_master_workflows()

    print('\n' + 80 * '-')

    download_operon_workflows()


if __name__ == '__main__':
    download_all()
