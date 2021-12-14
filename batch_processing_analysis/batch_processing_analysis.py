import numpy as np
import pandas as pd
from concurrency_oracle import HeuristicsConcurrencyOracle
from config import Configuration as StartTimeConfiguration

from batch_config import Configuration
from batch_processing_discovery import discover_batches_martins21


def _get_batch_last_enabled(batch_instance: pd.DataFrame, config: Configuration):
    enabled_first_processed = []
    for (case_key, case_batch) in batch_instance.groupby([config.log_ids.case]):
        first_start = case_batch[config.log_ids.start_timestamp].min()
        enabled_first_processed += case_batch.loc[
            case_batch[config.log_ids.start_timestamp] == first_start,
            config.log_ids.enabled_time
        ].values.tolist()
    return max(enabled_first_processed)


def analyze_batches(event_log: pd.DataFrame, config: Configuration):
    ############################
    # --- Discover batches --- #
    ############################
    batched_event_log = discover_batches_martins21(event_log, config)
    ####################################################
    # --- Discover activity instance enabled times --- #
    ####################################################
    concurrency_oracle = HeuristicsConcurrencyOracle(batched_event_log, StartTimeConfiguration())
    for (batch_key, trace) in batched_event_log.groupby([config.log_ids.case]):
        for index, event in trace.iterrows():
            batched_event_log.loc[index, config.log_ids.enabled_time] = concurrency_oracle.enabled_since(trace, event)
    ############################################
    # --- Calculate batching waiting times --- #
    ############################################
    # Create empty batch time columns
    batched_event_log[config.log_ids.batch_total_wt] = 0
    batched_event_log[config.log_ids.batch_creation_wt] = 0
    batched_event_log[config.log_ids.batch_ready_wt] = 0
    batched_event_log[config.log_ids.batch_other_wt] = 0
    # Task-based batches
    task_batched_events = batched_event_log[
        pd.isna(batched_event_log[config.log_ids.batch_subprocess_type]) &
        ~pd.isna(batched_event_log[config.log_ids.batch_type])
        ]
    for (batch_key, batch_instance) in task_batched_events.groupby([config.log_ids.batch_number]):
        batch_last_enabled = _get_batch_last_enabled(batch_instance, config)
        batch_first_start = batch_instance[config.log_ids.start_timestamp].min()
        # Ready WT: The time a particular case waits after the batch is created and not yet started to be processed.
        ready_wt = batch_first_start - batch_last_enabled
        batched_event_log[config.log_ids.batch_ready_wt] = np.where(batched_event_log[config.log_ids.batch_number] == batch_key,
                                                                    ready_wt,
                                                                    batched_event_log[config.log_ids.batch_ready_wt])
        # Process each batch instance case
        for (case_key, case_batch) in batch_instance.groupby([config.log_ids.case]):
            case_first_event = case_batch.loc[
                case_batch[config.log_ids.start_timestamp] == case_batch[config.log_ids.start_timestamp].min()]
            # Total WT: The time a particular case waits before being processed.
            total_wt = case_first_event[config.log_ids.start_timestamp] - case_first_event[config.log_ids.enabled_time]
            batched_event_log[config.log_ids.batch_total_wt] = np.where((batched_event_log[config.log_ids.batch_number] == batch_key) &
                                                                        (batched_event_log[config.log_ids.case] == case_key),
                                                                        total_wt,
                                                                        batched_event_log[config.log_ids.batch_total_wt])
            # Creation WT: The time a particular case waits for the batch to be created.
            creation_wt = batch_last_enabled - case_first_event[config.log_ids.enabled_time]
            batched_event_log[config.log_ids.batch_creation_wt] = np.where((batched_event_log[config.log_ids.batch_number] == batch_key) &
                                                                           (batched_event_log[config.log_ids.case] == case_key),
                                                                           creation_wt,
                                                                           batched_event_log[config.log_ids.batch_creation_wt])
            # Other WT: The time a particular case waits for its order to be processed when other cases in a batch are being processed.
            other_wt = case_first_event[config.log_ids.start_timestamp] - batch_first_start
            batched_event_log[config.log_ids.batch_other_wt] = np.where((batched_event_log[config.log_ids.batch_number] == batch_key) &
                                                                        (batched_event_log[config.log_ids.case] == case_key),
                                                                        other_wt,
                                                                        batched_event_log[config.log_ids.batch_other_wt])
            print()
    # Case-based batches
    case_batched_events = batched_event_log[~pd.isna(batched_event_log[config.log_ids.batch_subprocess_type])]
    for (batch_key, batch_instance) in case_batched_events.groupby([config.log_ids.batch_subprocess_number]):
        print()
    print()
    # Discover activation rules
    pass
