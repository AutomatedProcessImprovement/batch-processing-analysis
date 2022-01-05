import enum

import pandas as pd

from batch_config import Configuration
from batch_utils import get_batch_instance_start_time, get_batch_case_enabled_time, _get_workload, _get_batch_activities


class ActivationRulesDiscoverer:
    """
    Discover the activation rules of the batches in the event log.
    """

    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Set event log
        self.event_log = event_log
        # Set configuration
        self.config = config
        # Set log IDs to ease access within class
        self.log_ids = config.log_ids
        # Calculate features table
        self.features_table = self._calculate_features_table()

    def _calculate_features_table(self) -> pd.DataFrame:
        """
        Create a DataFrame with the features of the batch-related events, classifying them into events that activate the batch and events
        that does not activate the batch.
        """
        # Event log with events related to batches
        batch_log = self.event_log[~pd.isna(self.event_log[self.log_ids.batch_id])]
        # Register features for each batch instance
        features = []
        for (key, batch_instance) in batch_log.groupby([self.log_ids.batch_id]):
            # Common instants
            activities = _get_batch_activities(batch_instance, self.log_ids)
            batch_type = batch_instance[self.log_ids.batch_type].iloc[0]
            batch_instance_start = get_batch_instance_start_time(batch_instance, self.log_ids)
            batch_instance_first_enabled = (batch_instance
                                            .groupby([self.log_ids.case])
                                            .apply(lambda batch_case: get_batch_case_enabled_time(batch_case, self.log_ids))
                                            .min())
            activity = batch_instance[
                (batch_instance[self.log_ids.start_time] == batch_instance_start) &  # First activity executed
                (batch_instance[self.log_ids.enabled_time] ==  # Having the earliest enabled time if more than one started at the same time
                 batch_instance[batch_instance[self.log_ids.start_time] == batch_instance_start][self.log_ids.enabled_time].min())
                ][self.log_ids.activity].iloc[0]
            resource = batch_instance[self.log_ids.resource].iloc[0]
            case_ids = batch_instance[self.log_ids.case].unique()
            # Features
            instant = batch_instance_start
            num_queue = len(batch_instance[self.log_ids.case].unique())
            t_ready = batch_instance[self.log_ids.batch_ready_wt].iloc[0]
            t_waiting = batch_instance_start - batch_instance_first_enabled
            t_max_flow = (batch_instance_start -
                          self.event_log[self.event_log[self.log_ids.case].isin(case_ids)][self.log_ids.start_time].min())
            day_of_week = batch_instance_start.day_of_week
            day_of_month = batch_instance_start.day
            hour_of_day = batch_instance_start.hour
            minute_of_day = batch_instance_start.minute
            workload = _get_workload(self.event_log, resource, batch_instance_start, self.log_ids)
            features += [{
                self.log_ids.batch_id: key,
                self.log_ids.batch_type: batch_type,
                'activities': activities,
                'firing_activity': activity,
                'instant': instant,
                'num_queue': num_queue,
                't_ready': t_ready,
                't_waiting': t_waiting,
                't_max_flow': t_max_flow,
                'day_of_week': day_of_week,
                'day_of_month': day_of_month,
                'hour_of_day': hour_of_day,
                'minute_of_day': minute_of_day,
                'workload': workload,
                'outcome': BatchOutcome.ACTIVATE
            }]
        return pd.DataFrame(data=features)

    def get_activation_rules(self):
        """
        Infer the activation rules for each batch, and return them in dict form.
        """
        pass


class BatchOutcome(enum.Enum):
    NOT_ACTIVATE = 0
    ACTIVATE = 1
