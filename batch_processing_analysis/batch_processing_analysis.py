from datetime import timedelta

import numpy as np
import pandas as pd
from concurrency_oracle import HeuristicsConcurrencyOracle
from config import Configuration as StartTimeConfiguration

from batch_config import Configuration
from batch_processing_discovery import discover_batches_martins21
from batch_utils import get_batch_instance_enabled_time, get_batch_instance_start_time


class BatchProcessingAnalysis:
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Set event log
        self.event_log = event_log
        # Event log with batching information
        self.batch_event_log = event_log.copy()
        # Set configuration
        self.config = config
        # Set log IDs to ease access within class
        self.log_ids = config.log_ids
        # Set concurrency oracle
        start_time_config = StartTimeConfiguration(
            consider_parallelism=True
        )
        self.non_estimated_enabled_time = start_time_config.non_estimated_time
        self.concurrency_oracle = HeuristicsConcurrencyOracle(self.event_log, start_time_config)

    def analyze_batches(self) -> pd.DataFrame:
        # --- Discover activity instance enabled times --- #
        for (batch_key, trace) in self.batch_event_log.groupby([self.log_ids.case]):
            trace_start_time = trace[self.log_ids.start_time].min()
            for index, event in trace.iterrows():
                enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                if enabled_time != self.non_estimated_enabled_time:
                    self.batch_event_log.loc[index, self.log_ids.enabled_time] = enabled_time
                else:
                    self.batch_event_log.loc[index, self.log_ids.enabled_time] = trace_start_time
        # --- Discover batches --- #
        self.batch_event_log = discover_batches_martins21(self.batch_event_log, self.config)
        # --- Calculate batching waiting times --- #
        self._calculate_waiting_times()
        # Discover activation rules
        # TODO discover activation rules
        return self.batch_event_log

    def _calculate_waiting_times(self):
        # Create empty batch time columns
        self.batch_event_log[self.log_ids.batch_total_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_creation_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_ready_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_other_wt] = timedelta(0)
        # Task-based batches
        task_based_batch_events = self.batch_event_log[
            pd.isna(self.batch_event_log[self.log_ids.batch_subprocess_type]) &
            ~pd.isna(self.batch_event_log[self.log_ids.batch_type])
            ]
        self._process_waiting_times(task_based_batch_events, self.log_ids.batch_number)
        # Case-based batches
        case_based_batch_events = self.batch_event_log[~pd.isna(self.batch_event_log[self.log_ids.batch_subprocess_type])]
        self._process_waiting_times(case_based_batch_events, self.log_ids.batch_subprocess_number)

    def _process_waiting_times(self, batch_event, batch_type_key):
        for (batch_key, batch_instance) in batch_event.groupby([batch_type_key]):
            batch_last_enabled = get_batch_instance_enabled_time(batch_instance, self.log_ids)
            batch_first_start = get_batch_instance_start_time(batch_instance, self.log_ids)
            # Ready WT: The time a particular case waits after the batch is created and not yet started to be processed.
            ready_wt = batch_first_start - batch_last_enabled
            self.batch_event_log[self.log_ids.batch_ready_wt] = np.where(self.batch_event_log[batch_type_key] == batch_key,
                                                                         ready_wt,
                                                                         self.batch_event_log[self.log_ids.batch_ready_wt])
            # Process each batch instance case
            for (case_key, case_batch) in batch_instance.groupby([self.log_ids.case]):
                case_first_event = case_batch.loc[case_batch[self.log_ids.start_time] == case_batch[self.log_ids.start_time].min()].iloc[0]
                # Total WT: The time a particular case waits before being processed.
                total_wt = case_first_event[self.log_ids.start_time] - case_first_event[self.log_ids.enabled_time]
                self.batch_event_log[self.log_ids.batch_total_wt] = np.where((self.batch_event_log[batch_type_key] == batch_key) &
                                                                             (self.batch_event_log[self.log_ids.case] == case_key),
                                                                             total_wt,
                                                                             self.batch_event_log[self.log_ids.batch_total_wt])
                # Creation WT: The time a particular case waits for the batch to be created.
                creation_wt = batch_last_enabled - case_first_event[self.log_ids.enabled_time]
                self.batch_event_log[self.log_ids.batch_creation_wt] = np.where(
                    (self.batch_event_log[batch_type_key] == batch_key) &
                    (self.batch_event_log[self.log_ids.case] == case_key),
                    creation_wt,
                    self.batch_event_log[self.log_ids.batch_creation_wt])
                # Other WT: The time a particular case waits for its order to be processed when other cases in a batch are being processed.
                other_wt = case_first_event[self.log_ids.start_time] - batch_first_start
                self.batch_event_log[self.log_ids.batch_other_wt] = np.where((self.batch_event_log[batch_type_key] == batch_key) &
                                                                             (self.batch_event_log[self.log_ids.case] == case_key),
                                                                             other_wt,
                                                                             self.batch_event_log[self.log_ids.batch_other_wt])
