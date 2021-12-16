import os
import subprocess

import numpy as np
import pandas as pd

from batch_config import Configuration, EventLogIDs
from batch_utils import get_batch_instance_start_time, get_batch_case_enabled_time


def remove_wrong_enabled_time_cases(event_log_with_batches: pd.DataFrame, log_ids: EventLogIDs):
    found = True  # Flag to check for "wrong" cases until all of them are fine
    # ----------------------------------- #
    # --- Process single task batches --- #
    # ----------------------------------- #
    # While at least one case with "wrong" enabled times has been found, launch analysis
    while found:
        found = False
        # Get activity instances of single-task batches
        single_task_batch_events = event_log_with_batches[
            pd.isna(event_log_with_batches['batch_subprocess_type']) & ~pd.isna(event_log_with_batches['batch_type'])
            ]
        # For each single-task batch instance
        for (batch_instance_key, batch_instance) in single_task_batch_events.groupby(['batch_number']):
            batch_instance_start = get_batch_instance_start_time(batch_instance, log_ids)
            batch_case_keys = []
            # Check if any batch case has the enabled time after the batch instance start time
            for (batch_case_key, batch_case) in batch_instance.groupby([log_ids.case]):
                batch_case_enabled = get_batch_case_enabled_time(batch_case, log_ids)
                if batch_instance_start < batch_case_enabled:
                    # The batch instance started before the batch case was enabled -> store key to separate the batch case
                    batch_case_keys += [batch_case_key]
                    found = True  # A batch case with "wrong" enabled time has been found
            if found:
                # Declare as a new batch instance those batch cases with "wrong" enabled time
                new_batch_instance_key = event_log_with_batches['batch_number'].max() + 1
                event_log_with_batches['batch_number'] = np.where(
                    (event_log_with_batches[log_ids.case].isin(batch_case_keys)) &
                    (event_log_with_batches['batch_number'] == batch_instance_key),
                    new_batch_instance_key,
                    event_log_with_batches['batch_number']
                )
    # ---------------------------------- #
    # --- Process subprocess batches --- #
    # ---------------------------------- #
    # While at least one case with "wrong" enabled times has been found, launch analysis
    while found:
        found = False
        # Get activity instances of single-task batches
        subprocess_batch_events = event_log_with_batches[~pd.isna(event_log_with_batches['batch_subprocess_type'])]
        # For each single-task batch instance
        for (batch_instance_key, batch_instance) in subprocess_batch_events.groupby(['batch_subprocess_number']):
            batch_instance_start = get_batch_instance_start_time(batch_instance, log_ids)
            batch_case_keys = []
            # Check if any batch case has the enabled time after the batch instance start time
            for (batch_case_key, batch_case) in batch_instance.groupby([log_ids.case]):
                batch_case_enabled = get_batch_case_enabled_time(batch_case, log_ids)
                if batch_instance_start < batch_case_enabled:
                    # The batch instance started before the batch case was enabled -> store key to separate the batch case
                    batch_case_keys += [batch_case_key]
                    found = True  # A batch case with "wrong" enabled time has been found
            if found:
                # Declare as a new batch instance those batch cases with "wrong" enabled time
                new_batch_instance_key = event_log_with_batches['batch_subprocess_number'].max() + 1
                event_log_with_batches['batch_subprocess_number'] = np.where(
                    (event_log_with_batches[log_ids.case].isin(batch_case_keys)) &
                    (event_log_with_batches['batch_subprocess_number'] == batch_instance_key),
                    new_batch_instance_key,
                    event_log_with_batches['batch_subprocess_number']
                )


def split_batch_with_different_resources(event_log_with_batches: pd.DataFrame, log_ids: EventLogIDs):
    # ----------------------------------- #
    # --- Process single task batches --- #
    # ----------------------------------- #
    single_task_batch_events = event_log_with_batches[
        pd.isna(event_log_with_batches['batch_subprocess_type']) & ~pd.isna(event_log_with_batches['batch_type'])
        ]
    for (batch_instance_key, batch_instance) in single_task_batch_events.groupby(['batch_number']):
        if len(batch_instance[log_ids.resource].unique()) > 1:
            # More than one resource in a single-task batch instance -> split
            for (resource_key, batch_instance_by_resource) in batch_instance.groupby([log_ids.resource]):
                new_batch_instance_key = event_log_with_batches['batch_number'].max() + 1
                event_log_with_batches['batch_number'] = np.where(
                    (event_log_with_batches[log_ids.resource] == resource_key) &
                    (event_log_with_batches['batch_number'] == batch_instance_key),
                    new_batch_instance_key,
                    event_log_with_batches['batch_number']
                )
    # ---------------------------------- #
    # --- Process subprocess batches --- #
    # ---------------------------------- #
    subprocess_batch_events = event_log_with_batches[~pd.isna(event_log_with_batches['batch_subprocess_type'])]
    for (batch_instance_key, batch_instance) in subprocess_batch_events.groupby(['batch_subprocess_number']):
        if len(batch_instance[log_ids.resource].unique()) > 1:
            # More than one resource in a subprocess batch instance -> split
            for (resource_key, batch_instance_by_resource) in batch_instance.groupby([log_ids.resource]):
                new_batch_instance_key = event_log_with_batches['batch_subprocess_number'].max() + 1
                event_log_with_batches['batch_subprocess_number'] = np.where(
                    (event_log_with_batches[log_ids.resource] == resource_key) &
                    (event_log_with_batches['batch_subprocess_number'] == batch_instance_key),
                    new_batch_instance_key,
                    event_log_with_batches['batch_subprocess_number']
                )


def discover_batches_martins21(event_log: pd.DataFrame, config: Configuration) -> pd.DataFrame:
    preprocessed_log_path = config.PATH_BATCH_DETECTION_FOLDER.joinpath("preprocessed_event_log.csv.gz")
    batched_log_path = config.PATH_BATCH_DETECTION_FOLDER.joinpath("batched_event_log.csv")
    # Format event log
    preprocessed_event_log = event_log[[
        config.log_ids.case,
        config.log_ids.activity,
        config.log_ids.enabled_time,
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
    # Read batch event log
    event_log_with_batches = pd.read_csv(batched_log_path)
    # Preprocess batch event log
    event_log_with_batches[config.log_ids.enabled_time] = pd.to_datetime(event_log_with_batches[config.log_ids.enabled_time], utc=True)
    event_log_with_batches[config.log_ids.start_time] = pd.to_datetime(event_log_with_batches[config.log_ids.start_time], utc=True)
    event_log_with_batches[config.log_ids.end_time] = pd.to_datetime(event_log_with_batches[config.log_ids.end_time], utc=True)
    # Remove batch cases with enable time before first batch start time (negative ready batch wt)
    remove_wrong_enabled_time_cases(event_log_with_batches, config.log_ids)
    # Split batch instances with different resources
    split_batch_with_different_resources(event_log_with_batches, config.log_ids)
    # Split subprocess batch instances with different task-level batch type
    # TODO preprocess to split batches with different task_level_type:
    #   - Split them into different batches (each batch one type)
    # Remove all batch instances formed only by one case
    # TODO preprocess to remove batches composed of just one case:
    #   - Remove as batches all formed of just one case instance.
    # Reformat batches to standard
    # TODO rename batch types to two columns "batch_number" and "batch_type":
    #   - Parallel
    #   - Sequential case-based
    #   - Sequential task-based
    #   - Concurrent case-based
    #   - Concurrent task-based
    # Remove created files
    os.remove(preprocessed_log_path)
    os.remove(batched_log_path)
    # Return event log with batch information
    return event_log_with_batches
