import enum
import random

import numpy as np
import pandas as pd
import wittgenstein as lw

from batch_config import Configuration, ActivationRulesMode
from batch_utils import get_batch_instance_start_time, get_batch_case_enabled_time, get_workload, get_batch_activities, \
    get_batch_instance_enabled_time


class BatchOutcome(enum.Enum):
    NOT_ACTIVATE = 0
    ACTIVATE = 1


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
            # Get features of non-activating instants
            non_activating_instants = []
            # 1 - X events in between the ready time of the batch
            batch_instance_enabled = get_batch_instance_enabled_time(batch_instance, self.log_ids)
            non_activating_instants += pd.date_range(
                start=batch_instance_enabled,
                end=batch_instance_start,
                periods=self.config.num_batch_ready_negative_events + 2
            )[1:-1].tolist()
            # 2 - Instants per enablement time of each case
            enable_times = [
                instant
                for instant in batch_instance.groupby([self.log_ids.case]).apply(
                    lambda batch_case: get_batch_case_enabled_time(batch_case, self.log_ids)
                )
                if instant < batch_instance_start
            ]
            non_activating_instants += random.sample(enable_times, min(len(enable_times), self.config.num_batch_enabled_negative_events))
            # 3 - Obtain the features per instant
            for instant in non_activating_instants:
                if instant < batch_instance_start:
                    # Discard the batch cases enabled after the current instant, and then calculate the features of the remaining cases.
                    cases_enabled_before_instant = [
                        case_id
                        for case_id in batch_instance[self.log_ids.case].unique() if get_batch_case_enabled_time(
                            batch_instance[batch_instance[self.log_ids.case] == case_id],
                            self.log_ids
                        ) <= instant
                    ]
                    features += [
                        self._get_features(
                            instant,
                            batch_instance[batch_instance[self.log_ids.case].isin(cases_enabled_before_instant)],
                            BatchOutcome.NOT_ACTIVATE
                        )
                    ]
        return pd.DataFrame(data=features)

    def _get_features(self, instant: pd.Timestamp, batch_instance: pd.DataFrame, outcome: BatchOutcome) -> dict:
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
            'minute': minute_of_day,
            'workload': workload,
            'outcome': outcome
        }

    def get_activation_rules(self, mode: ActivationRulesMode = ActivationRulesMode.PER_BATCH_TYPE) -> dict:
        """
        Infer the activation rules for each activity, batch, or batch type, based on [mode].

        :return: dict with the ID for the activity/batch/batch_type as key, and the rules as value.
        """
        # Parse features table to transform its values
        parsed_features_table = self.features_table.copy()
        parsed_features_table['instant'] = parsed_features_table['instant'].astype(np.int64) / 10 ** 9
        parsed_features_table['t_ready'] = parsed_features_table['t_ready'].apply(lambda t: t.total_seconds())
        parsed_features_table['t_waiting'] = parsed_features_table['t_waiting'].apply(lambda t: t.total_seconds())
        parsed_features_table['t_max_flow'] = parsed_features_table['t_max_flow'].apply(lambda t: t.total_seconds())
        parsed_features_table['outcome'] = np.where(parsed_features_table['outcome'] == BatchOutcome.ACTIVATE, 1, 0)
        # Prepare datasets based on the established mode
        if mode == ActivationRulesMode.PER_ACTIVITY:
            group_keys = ['firing_activity']
            batch_groups = parsed_features_table.drop([self.log_ids.batch_id, 'activities'], axis=1).groupby(group_keys)
        elif mode == ActivationRulesMode.PER_BATCH_TYPE:
            group_keys = ['activities', self.log_ids.batch_type]
            batch_groups = parsed_features_table.drop([self.log_ids.batch_id, 'firing_activity'], axis=1).groupby(group_keys)
        elif mode == ActivationRulesMode.PER_BATCH:
            group_keys = ['activities']
            batch_groups = parsed_features_table.drop([self.log_ids.batch_id, 'firing_activity'], axis=1).groupby(group_keys)
        else:
            raise ValueError("Mode to discover activation rules unrecognised!")
        # Calculate activation rules per batch group
        rules = {}
        for (key, batch_group) in batch_groups:
            if len(batch_group) > 30:
                filtered_group = batch_group.drop(group_keys, axis=1)
                if len(filtered_group['outcome'].unique()) > 1:
                    rules[key] = self._get_rules(filtered_group)
                else:
                    print("Not extracting rules from batch {} due to only one outcome in training!".format(key))
            else:
                print("Not extracting rules from batch {} due to low size: {}".format(key, len(batch_group)))
        return rules

    def _get_rules(self, data: pd.DataFrame) -> dict:
        # Create empty model and data copy
        ripper_model = None
        filtered_data = data.copy()
        # Extract rules one by one
        continue_search = True
        while continue_search:
            # Train new model to extract 1 rule
            new_model = lw.RIPPER(max_rules=2)
            new_model.fit(filtered_data, class_feat='outcome')
            # If any rule has been discovered
            if len(new_model.ruleset_.rules) > 0:
                # Measure support
                predictions = new_model.predict(filtered_data.drop(['outcome'], axis=1))
                true_positives = [
                    p and a
                    for (p, a) in zip(predictions, filtered_data['outcome'])
                ]
                support = sum(true_positives) / len(data)
                if support >= self.config.min_rule_support:
                    # If the support is enough, add it to the model and remove its positive cases
                    if ripper_model:
                        ripper_model.add_rule(new_model.ruleset_.rules[0])
                    else:
                        ripper_model = new_model
                    # Retain only non
                    filtered_data = filtered_data[[not prediction for prediction in predictions]]
                else:
                    # If support is not enough, end search
                    continue_search = False
            else:
                # If no rules has been discovered, end search
                continue_search = False
            if ripper_model and len(ripper_model.ruleset_.rules) >= self.config.max_rules:
                # If enough rules has been discovered, end search
                continue_search = False

        if ripper_model:
            predictions = ripper_model.predict(data.drop(['outcome'], axis=1))
            true_positives = [
                p and a
                for (p, a) in zip(predictions, data['outcome'])
            ]
            return {
                'num_obs': len(data),
                'model': ripper_model,
                'confidence': sum(true_positives) / sum(predictions),
                'support': sum(true_positives) / len(data)
            }
        else:
            return {}
