import yaml


copy_defaults = {
    "transfers": 4,
    "checkers": 8,
    "checksum": "",
    "log-level": "INFO",
    "stats": "5m",
    "config": "/opt/airflow/rclone/rclone.conf"
}

def rclone_copy_command_str(source, dest, rclone_args:dict=copy_defaults)-> str:
    command = [
        "rclone copy",
        source,
        dest
    ]
    for k, v in rclone_args:
        command += f"--{k} {v}"

    return tuple(command)

check_defaults = {
    "differ": "{}/RCLONE_DIFFER",
    "error": "{}/RCLONE_ERROR",
    "config": "/opt/airflow/rclone/rclone.conf"
}

def rclone_check_command_str(source, dest, rclone_args:dict=check_defaults)-> str:
    command = [
        "rclone copy",
        source,
        dest
    ]
    for k, v in rclone_args:
        command += f"--{k} {str(v).format(source)}"

    return tuple(command)
