import pandas as pd

from batch_config import BatchType, EventLogIDs


def summarize_batch_waiting_times(event_log: pd.DataFrame, log_ids: EventLogIDs) -> dict:
    batches_report = {}
    batch_events = event_log[~pd.isna(event_log[log_ids.batch_id])]
    # For each batch instance
    for (instance_key, batch_instance) in batch_events.groupby([log_ids.batch_id]):
        # Get the activities being part of it (batch_key)
        batch_activities = tuple(
            sorted(
                batch_instance.groupby([log_ids.case]).get_group(batch_instance[log_ids.case].iloc[0])[
                    log_ids.activity].values
            )
        )
        # Get the batch type
        batch_type = batch_instance[log_ids.batch_type].iloc[0]
        # Retrieve stats for this batch type
        if batch_activities not in batches_report:
            batches_report[batch_activities] = _new_batch_stat_structure()
        batch_type_stats = batches_report[batch_activities][batch_type]
        # Update num instances
        batch_type_stats['num_instances'] += 1
        for (case_key, batch_case) in batch_instance.groupby([log_ids.case]):
            # First activity of this batch case
            batch_case_activity = batch_case.iloc[0]
            # Update batch stats with this batch case stats
            batch_type_stats['num_cases'] += 1
            batch_type_stats['total_wt'] += [batch_case_activity[log_ids.batch_total_wt]]
            batch_type_stats['creation_wt'] += [batch_case_activity[log_ids.batch_creation_wt]]
            batch_type_stats['ready_wt'] += [batch_case_activity[log_ids.batch_ready_wt]]
            batch_type_stats['other_wt'] += [batch_case_activity[log_ids.batch_other_wt]]
    # Return batch stats
    return batches_report


def _new_batch_stat_structure():
    return {
        BatchType.parallel: {
            'num_instances': 0,
            'num_cases': 0,
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.task_sequential: {
            'num_instances': 0,
            'num_cases': 0,
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.task_concurrent: {
            'num_instances': 0,
            'num_cases': 0,
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.case_sequential: {
            'num_instances': 0,
            'num_cases': 0,
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
        BatchType.case_concurrent: {
            'num_instances': 0,
            'num_cases': 0,
            'total_wt': [],
            'creation_wt': [],
            'ready_wt': [],
            'other_wt': []
        },
    }
