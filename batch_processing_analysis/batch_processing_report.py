import pandas as pd

from batch_config import BatchType, EventLogIDs
from batch_utils import get_batch_activities, get_batch_activities_number_executions


def summarize_batch_waiting_times(event_log: pd.DataFrame, log_ids: EventLogIDs) -> dict:
    """
    Given an event log with batches, batch types, and waiting times identified, return a report containing the waiting times per batch
    grouped by batch type.

    :param event_log: event log containing batch ID, batch type, and total/created/ready/other waiting times.
    :param log_ids: IDs for each of the elements in the event log.

    :return: a dictionary with a tuple of activities as key (identifying the activities forming the batch) and a dictionary with the batch
    waiting times grouped by batch instance type.
    """
    batches_report = {}
    batch_events = event_log[~pd.isna(event_log[log_ids.batch_id])]
    # For each batch instance
    for (instance_key, batch_instance) in batch_events.groupby([log_ids.batch_id]):
        # Get the activities being part of it (batch_key)
        batch_activities = get_batch_activities(batch_instance, log_ids)
        # Get the batch type
        batch_type = batch_instance[log_ids.batch_type].iloc[0]
        # Retrieve stats for this batch type
        if batch_activities not in batches_report:
            batches_report[batch_activities] = _new_batch_stat_structure()
        batch_report = batches_report[batch_activities]
        batch_type_stats = batch_report[batch_type]
        # If not already calculated, get the number of times the activities of this batch were executed (batched or not)
        if batch_report['total_occurrences'] == 0:
            batch_report['total_occurrences'] = get_batch_activities_number_executions(event_log, batch_instance, log_ids)
        # Update num instances
        batch_type_stats['num_instances'] += 1
        for (case_key, batch_case) in batch_instance.groupby([log_ids.case]):
            # First activity of this batch case
            batch_case_activity = batch_case.iloc[0]
            # Update batch stats with this batch case stats
            batch_type_stats['num_cases'] += 1
            batch_type_stats['processing_time'] += [batch_case_activity[log_ids.batch_pt]]
            batch_type_stats['waiting_time'] += [batch_case_activity[log_ids.batch_wt]]
            batch_type_stats['total_wt'] += [batch_case_activity[log_ids.batch_total_wt]]
            batch_type_stats['creation_wt'] += [batch_case_activity[log_ids.batch_creation_wt]]
            batch_type_stats['ready_wt'] += [batch_case_activity[log_ids.batch_ready_wt]]
            batch_type_stats['other_wt'] += [batch_case_activity[log_ids.batch_other_wt]]
    # Calculate frequency of occurrence per batch type w.r.t. total executions
    for batch_activities in batches_report:
        batch_report = batches_report[batch_activities]
        batched_occurrences = 0
        for batch_type in [BatchType.parallel,
                           BatchType.task_sequential,
                           BatchType.task_concurrent,
                           BatchType.case_sequential,
                           BatchType.case_concurrent]:
            batch_report[batch_type]['freq_occurrence'] = batch_report[batch_type]['num_cases'] / batch_report['total_occurrences']
            batched_occurrences += batch_report[batch_type]['num_cases']
        batch_report['batched_total_occurrences'] = batched_occurrences
        batch_report['batched_freq_occurrence'] = batched_occurrences / batch_report['total_occurrences']
    # Return batch stats
    return batches_report


def _new_batch_stat_structure():
    return {
        'total_occurrences': 0,
        'batched_total_occurrences': 0,
        'batched_freq_occurrence': 0.0,
        BatchType.parallel: {
            'freq_occurrence': 0.0,
            'num_instances': 0,
            'num_cases': 0,
            'processing_time': [],
            'waiting_time': [],
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.task_sequential: {
            'freq_occurrence': 0.0,
            'num_instances': 0,
            'num_cases': 0,
            'processing_time': [],
            'waiting_time': [],
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.task_concurrent: {
            'freq_occurrence': 0.0,
            'num_instances': 0,
            'num_cases': 0,
            'processing_time': [],
            'waiting_time': [],
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.case_sequential: {
            'freq_occurrence': 0.0,
            'num_instances': 0,
            'num_cases': 0,
            'processing_time': [],
            'waiting_time': [],
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.case_concurrent: {
            'freq_occurrence': 0.0,
            'num_instances': 0,
            'num_cases': 0,
            'processing_time': [],
            'waiting_time': [],
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
    }
