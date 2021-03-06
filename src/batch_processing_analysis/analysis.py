from datetime import timedelta

import pandas as pd
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration as StartTimeConfiguration
from estimate_start_times.config import EventLogIDs as StartTimeEventLogIDs

from .config import Configuration
from .discovery import discover_batches_martins21
from .utils import get_batch_instance_enabled_time, get_batch_instance_start_time, get_naive_batch_case_processing_waiting_times


class BatchProcessingAnalysis:
    """
    Discover the batches in an event log and calculate its waiting times (total, created, ready...).
    """

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
            log_ids=StartTimeEventLogIDs(
                case=self.log_ids.case,
                activity=self.log_ids.activity,
                enabled_time=self.log_ids.enabled_time,
                start_time=self.log_ids.start_time,
                end_time=self.log_ids.end_time,
                resource=self.log_ids.resource,
            ),
            consider_start_times=True
        )
        self.concurrency_oracle = HeuristicsConcurrencyOracle(self.event_log, start_time_config)

    def analyze_batches(self) -> pd.DataFrame:
        # Discover activity instance enabled times
        self.concurrency_oracle.add_enabled_times(self.batch_event_log)
        # Discover batches
        self.batch_event_log = discover_batches_martins21(self.batch_event_log, self.config)
        # Calculate batching waiting times
        self._calculate_waiting_times()
        # Return event log with batches and waiting times
        return self.batch_event_log

    def _calculate_waiting_times(self):
        # Create empty batch time columns
        self.batch_event_log[self.log_ids.batch_pt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_total_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_creation_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_ready_wt] = timedelta(0)
        self.batch_event_log[self.log_ids.batch_other_wt] = timedelta(0)
        # If report checkpoints is true, create columns for report
        if self.config.report_batch_checkpoints:
            self.batch_event_log[self.log_ids.batch_case_enabled] = pd.NaT
            self.batch_event_log[self.log_ids.batch_instance_enabled] = pd.NaT
            self.batch_event_log[self.log_ids.batch_start] = pd.NaT
        # Calculate waiting times
        batch_events = self.batch_event_log[~pd.isna(self.batch_event_log[self.log_ids.batch_id])]
        for (batch_key, batch_instance) in batch_events.groupby([self.log_ids.batch_id]):
            batch_last_enabled = get_batch_instance_enabled_time(batch_instance, self.log_ids)
            batch_first_start = get_batch_instance_start_time(batch_instance, self.log_ids)
            # Ready WT: The time a particular case waits after the batch is created and not yet started to be processed.
            ready_wt = batch_first_start - batch_last_enabled
            # Process each batch instance case
            for (case_key, batch_case) in batch_instance.groupby([self.log_ids.case]):
                case_first_event = batch_case.loc[batch_case[self.log_ids.start_time] == batch_case[self.log_ids.start_time].min()].iloc[0]
                # Total WT: The time a particular case waits before being processed.
                total_wt = case_first_event[self.log_ids.start_time] - case_first_event[self.log_ids.enabled_time]
                # Creation WT: The time a particular case waits for the batch to be created.
                creation_wt = batch_last_enabled - case_first_event[self.log_ids.enabled_time]
                # Other WT: The time a particular case waits for its order to be processed when other cases in a batch are being processed.
                other_wt = case_first_event[self.log_ids.start_time] - batch_first_start
                # Overall PT and WT: Time since the enablement of the batch case until its end.
                batch_pt, batch_wt = get_naive_batch_case_processing_waiting_times(batch_case, self.log_ids)
                self.batch_event_log.loc[
                    (self.batch_event_log[self.log_ids.batch_id] == batch_key) & (self.batch_event_log[self.log_ids.case] == case_key),
                    [self.log_ids.batch_total_wt,
                     self.log_ids.batch_creation_wt,
                     self.log_ids.batch_ready_wt,
                     self.log_ids.batch_other_wt,
                     self.log_ids.batch_pt,
                     self.log_ids.batch_wt]
                ] = [total_wt,
                     creation_wt,
                     ready_wt,
                     other_wt,
                     batch_pt,
                     batch_wt]
                # If report checkpoints is true, add them to the event log
                if self.config.report_batch_checkpoints:
                    self.batch_event_log.loc[
                        (self.batch_event_log[self.log_ids.batch_id] == batch_key) & (self.batch_event_log[self.log_ids.case] == case_key),
                        [self.log_ids.batch_case_enabled,
                         self.log_ids.batch_instance_enabled,
                         self.log_ids.batch_start]
                    ] = [case_first_event[self.log_ids.enabled_time],
                         batch_last_enabled,
                         batch_first_start]
