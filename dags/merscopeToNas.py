from __future__ import annotations

import os
from pathlib import Path
import logging

import pendulum
from airflow.sdk import DAG, task
from common.transfer_utils import *

ENV_KEYS = [
    'WORKER_NAME',
    'SOURCE_ROOT', 
    'DEST_REMOTE',
    'DEST_ROOT',
    'RCLONE_CONFIG_PATH',
]

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
    # "execution_timeout": pendulum.duration(hours=6),
}

# Literals
DATA_PATHS_LITERAL = "data_paths" 
OUTPUT_PATHS_LITERAL = "output_paths"

logger = logging.getLogger(__name__)


def build_transfer_dag(cfg:dict): # written this way to turn this into a factory

    WORKER_NAME = cfg['WORKER_NAME']
    SOURCE = cfg['SOURCE_ROOT']
    DEST_REMOTE = cfg['DEST_REMOTE']
    DEST_ROOT = cfg['DEST_ROOT']
    RCLONE_CFG = cfg['RCLONE_CONFIG_PATH']

    with DAG(
        dag_id="merscopeToNas_file_transfer",
        description="Transfers files from MERSCOPE instrument to NAS storage via rclone",
        default_args=DEFAULT_ARGS,
        schedule="0 2 * * *",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=["file-transfer", "merscopeToNas"],
    ) as dag:

        @task(
                task_id=f"check_for_new_files",
                queue=f"merscopeToNas_{WORKER_NAME}",
                multiple_outputs=True)
        def list_new_dir():
            """Returns a list names of experiments to transfer."""
            from pathlib import Path
            data_dirs = list(Path(f"{SOURCE}/merfish_raw_data").glob("*"))
            outs_dirs = list(Path(f"{SOURCE}/merfish_output").glob("*"))
            check = lambda d: (d / "MERSCOPETONAS").exists()

            logger.log(logging.INFO, 'data_dirs\nfolder\tflagged')
            for p in data_dirs:
                logger.log(logging.INFO, f'{p}\t{check(p)}')

            return {
                DATA_PATHS_LITERAL: [str(d) for d in data_dirs if not check(d)],
                OUTPUT_PATHS_LITERAL: [str(d) for d in outs_dirs if not check(d)]
            }

        # splitters ####
        @task
        def get_data_paths(paths_dict):
            return paths_dict[DATA_PATHS_LITERAL]
        @task
        def get_output_paths(paths_dict):
            return paths_dict[OUTPUT_PATHS_LITERAL]
        # splitters ####

        @task.bash(task_id="rclone_transfer_data", queue=f"merscopeToNas_{WORKER_NAME}")
        def transfer_data(path):

            source = f"/source/data/{path} "
            dest = f"{DEST_REMOTE}:{DEST_ROOT}/data/{Path(path).name} "

            return (
                f"rclone copy "
                f"{source} "
                f"{dest} " 
                f"--transfers 4 --checkers 8 "
                f"--config {RCLONE_CFG}"
            )


        @task.bash(task_id="rclone_transfer_output", queue=f"merscopeToNas_{WORKER_NAME}")
        def transfer_output(path):

            source = f"/source/data/{path} "
            dest = f"{DEST_REMOTE}:{DEST_ROOT}/output/{Path(path).name} "

            return (
                f"rclone copy "
                f"{source} "
                f"{dest} " 
                f"--transfers 4 --checkers 8 "
                f"--config {RCLONE_CFG}"
            )

        @task.bash(task_id="rclone_verify", queue=f"merscopeToNas_{WORKER_NAME}")
        def verify():

            return (
                "echo 'This works'"
                # "rclone check "
                # f"{path} "
                # f"{DEST_REMOTE}:{DEST_ROOT}/data/{Path(path).name} "
                # f"--differ {SOURCE}/RCLONE_DIFFER "
                # f"--error {SOURCE}/RCLONE_ERROR "
                # f"--config {RCLONE_CFG}"
            )

        paths = list_new_dir()
        data_paths = get_data_paths(paths)
        output_paths = get_output_paths(paths)

        transfer_data_task = transfer_data.expand(path=data_paths)
        transfer_output_task = transfer_output.expand(path=output_paths)
        verify_task = verify()

        [transfer_data_task, transfer_output_task] >> verify_task
        
        return dag

cfg = {e:os.environ[e] for e in ENV_KEYS if e != ""}
if len(cfg) != len(ENV_KEYS):
    raise EnvironmentError

dags = build_transfer_dag(cfg)

if __name__ == "__main__":
    for _cfg in [cfg]: # Here in case I want to turn this into a factory
        dag = build_transfer_dag(_cfg)
        print(dag)
    # print(
    # f"rclone copy "
    # f". "
    # f"coconut2.ucsd.edu:~/test "
    # # f"--sftp-user=erboone "
    # # f"--sftp-pass=1id3ylhMnEieVWmHZpQk0G5p5SKfojSGij7ziw "
    # f"--transfers 4 --checkers 8 "
    # f"--config /opt/airflow/rclone/rclone.conf "
    # "--verbose "
    # )