import subprocess

import pandas as pd

from configu import Configuration


def discover_batches_martins21(event_log: pd.DataFrame, config: Configuration) -> pd.DataFrame:
    preprocessed_log_path = "preprocessed_event_log.csv.gz"
    batched_log_path = "batched_event_log.csv"
    timestamp_format = "yyyy-mm-dd hh:mm:ss"
    # Format event log
    preprocessed_event_log = event_log[[
        config.log_ids.case,
        config.log_ids.activity,
        config.log_ids.start_timestamp,
        config.log_ids.end_timestamp,
        config.log_ids.resource
    ]]
    # Export event log
    preprocessed_event_log.to_csv(preprocessed_log_path, encoding='utf-8', index=False, compression='gzip')
    # Run Martins 2021 batching discovery technique
    subprocess.call(
        [config.r_executable,
         config.script_path,
         preprocessed_log_path,
         batched_log_path,
         timestamp_format],
        shell=True)
    # Read result event log
    event_log_with_batches = pd.read_csv(batched_log_path)
    # Return event log with batch information
    return event_log_with_batches
