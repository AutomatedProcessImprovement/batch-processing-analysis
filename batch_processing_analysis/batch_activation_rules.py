import enum

import pandas as pd

from batch_config import Configuration
from batch_utils import get_batch_instance_start_time, get_batch_case_enabled_time, get_workload, get_batch_activities


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
        # Create features table
        self.features_table = self._calculate_features_table()

    def _calculate_features_table(self) -> pd.DataFrame:
        """
        Create a DataFrame with the features of the batch-related events, classifying them into events that activate the batch and events
        that does not activate the batch.

        :return: A Dataframe with the features of the events activating a batch.
        """
        # Event log with events related to batches
        batch_log = self.event_log[~pd.isna(self.event_log[self.log_ids.batch_id])]
        # Register features for each batch instance
        features = []
        for (key, batch_instance) in batch_log.groupby([self.log_ids.batch_id]):
            batch_instance_start = get_batch_instance_start_time(batch_instance, self.log_ids)
            # Get features of the instant activating the batch instance
            features += [
                self._get_features(
                    batch_instance_start,
                    batch_instance,
                    BatchOutcome.ACTIVATE
                )
            ]
            # Create non-activating instants and gets its features
            # 1 - X events in between the ready time of the batch
            # TODO
            # 2 - One event per enablement time of each case
            for enablement_time in batch_instance.groupby([self.log_ids.case]).apply(
                    lambda batch_case: get_batch_case_enabled_time(batch_case, self.log_ids)
            ):
                if enablement_time < batch_instance_start:
                    # Discard the batch cases enabled after the current instant, and then calculate the features of the remaining cases.
                    cases_enabled_before_instant = [
                        case_id
                        for case_id in batch_instance[self.log_ids.case].unique() if get_batch_case_enabled_time(
                            batch_instance[batch_instance[self.log_ids.case] == case_id],
                            self.log_ids
                        ) <= enablement_time
                    ]
                    features += [
                        self._get_features(
                            enablement_time,
                            batch_instance[batch_instance[self.log_ids.case].isin(cases_enabled_before_instant)],
                            BatchOutcome.NOT_ACTIVATE
                        )
                    ]
        return pd.DataFrame(data=features)

    def _get_features(self, instant: pd.Timestamp, batch_instance: pd.DataFrame, outcome) -> dict:
        """
        Get the features to discover activation rules of a specific instant [instant] in a batch instance [batch_instance].

        :param instant: instant of the event to register.
        :param batch_instance: DataFrame with the activity instances of the batch instance.
        :param outcome: BatchOutcome indicating the outcome of this event (either activate or do not activate the batch).

        :return: a dict with the features of this batch instance.
        """
        # Get common values
        activities = get_batch_activities(batch_instance, self.log_ids)
        batch_id = batch_instance[self.log_ids.batch_id].iloc[0]
        batch_type = batch_instance[self.log_ids.batch_type].iloc[0]
        batch_instance_first_enabled = (batch_instance
                                        .groupby([self.log_ids.case])
                                        .apply(lambda batch_case: get_batch_case_enabled_time(batch_case, self.log_ids))
                                        .min())
        batch_instance_last_enabled = (batch_instance
                                       .groupby([self.log_ids.case])
                                       .apply(lambda batch_case: get_batch_case_enabled_time(batch_case, self.log_ids))
                                       .max())
        batch_instance_start = get_batch_instance_start_time(batch_instance, self.log_ids)
        activity = batch_instance[
            (batch_instance[self.log_ids.start_time] == batch_instance_start) &  # First activity executed
            (batch_instance[self.log_ids.enabled_time] ==  # Having the earliest enabled time if more than one started at the same time
             batch_instance[batch_instance[self.log_ids.start_time] == batch_instance_start][self.log_ids.enabled_time].min())
            ][self.log_ids.activity].iloc[0]
        resource = batch_instance[self.log_ids.resource].iloc[0]
        case_ids = batch_instance[self.log_ids.case].unique()
        # Features
        num_queue = len(batch_instance[self.log_ids.case].unique())
        t_ready = instant - batch_instance_last_enabled
        t_waiting = instant - batch_instance_first_enabled
        t_max_flow = (instant -
                      self.event_log[self.event_log[self.log_ids.case].isin(case_ids)][self.log_ids.start_time].min())
        day_of_week = instant.day_of_week
        day_of_month = instant.day
        hour_of_day = instant.hour
        minute_of_day = instant.minute
        workload = get_workload(self.event_log, resource, instant, self.log_ids)
        # Return the features dict
        return {
            self.log_ids.batch_id: batch_id,
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
            'outcome': outcome
        }

    def get_activation_rules(self):
        """
        Infer the activation rules for each batch, and return them in dict form.
        """
        pass


class BatchOutcome(enum.Enum):
    NOT_ACTIVATE = 0
    ACTIVATE = 1
