import os
import subprocess

import pandas as pd

from batch_config import Configuration


def discover_batches_martins21(event_log: pd.DataFrame, config: Configuration) -> pd.DataFrame:
    preprocessed_log_path = config.PATH_BATCH_DETECTION_FOLDER.joinpath("preprocessed_event_log.csv.gz")
    batched_log_path = config.PATH_BATCH_DETECTION_FOLDER.joinpath("batched_event_log.csv")
    # Format event log
    preprocessed_event_log = event_log[[
        config.log_ids.case,
        config.log_ids.activity,
        config.log_ids.start_time,
        config.log_ids.end_time,
        config.log_ids.resource
    ]]
    # Export event log
    preprocessed_event_log.to_csv(
        preprocessed_log_path,
        date_format="%Y-%m-%d %H:%M:%S",
        encoding='utf-8',
        index=False,
        compression='gzip')
    # Run Martins 2021 batching discovery technique
    subprocess.call(
        [config.PATH_R_EXECUTABLE,
         config.PATH_BATCH_DETECTION_SCRIPT,
         preprocessed_log_path,
         batched_log_path,
         "yyyy-mm-dd hh:mm:ss"],
        shell=True
    )
    # Read result event log
    event_log_with_batches = pd.read_csv(batched_log_path)
    event_log_with_batches[config.log_ids.start_time] = pd.to_datetime(event_log_with_batches[config.log_ids.start_time], utc=True)
    event_log_with_batches[config.log_ids.end_time] = pd.to_datetime(event_log_with_batches[config.log_ids.end_time], utc=True)
    # Remove created files
    os.remove(preprocessed_log_path)
    os.remove(batched_log_path)
    # Return event log with batch information
    return event_log_with_batches
