from __future__ import annotations

import yaml
import os
import subprocess as sub
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

logger = logging.getLogger(__name__)


def build_transfer_dag(cfg:dict):

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
        max_active_runs=1,
        max_queued_runs=1,
        tags=["file-transfer", "merscopeToNas"],
    ) as dag:

        @task(task_id=f"check_for_new_files", queue=f"merscopeToNas_{WORKER_NAME}")
        def list_new_dir():
            """Returns a list names of experiments to transfer."""
            from pathlib import Path
            data_dirs = Path(f"{SOURCE}/merfish_raw_data").glob("*")
            outs_dirs = Path(f"{SOURCE}/merfish_output").glob("*")
            logger.log(logging.INFO, 'testing logging')
            check = lambda d: (d / "MERSCOPETONAS").exists()


            return (
                [{"data_path": d["Path"]} for d in data_dirs if check(d)],
                [{"output_path": d["Path"]} for d in outs_dirs if check(d)],
            )

        discover = list_new_dir()

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
        def verify(path):

            return (
                "rclone check "
                f"{path} "
                f"{DEST_REMOTE}:{DEST_ROOT}/data/{Path(path).name} "
                f"--differ {SOURCE}/RCLONE_DIFFER "
                f"--error {SOURCE}/RCLONE_ERROR "
                f"--config {RCLONE_CFG}"
            )


        discover >> [transfer_data, transfer_output] >> verify
        
        return dag

cfg = {e:os.environ[e] for e in ENV_KEYS}
dags = build_transfer_dag(cfg)

if __name__ == "__main__":
    for dag in build_transfer_dag():
        dag.test()
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