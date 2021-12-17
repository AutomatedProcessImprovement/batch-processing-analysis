import pandas as pd

from batch_config import Configuration, EventLogIDs, BatchType
from batch_processing_analysis import BatchProcessingAnalysis


def main():
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/ConsultaDataMining201618.csv.gz"
    config = Configuration()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[config.log_ids.start_time] = pd.to_datetime(event_log[config.log_ids.start_time], utc=True)
    event_log[config.log_ids.end_time] = pd.to_datetime(event_log[config.log_ids.end_time], utc=True)
    # Run main analysis
    batch_analyzer = BatchProcessingAnalysis(event_log, config)
    batch_event_log = batch_analyzer.analyze_batches()
    report_batches_analysis(batch_event_log, config.log_ids)
    print()


def report_batches_analysis(batch_event_log: pd.DataFrame, log_ids: EventLogIDs):
    batches = {}
    batch_events = batch_event_log[~pd.isna(batch_event_log[log_ids.batch_id])]
    for (instance_key, batch_instance) in batch_events.groupby([log_ids.batch_id]):
        # Batch instance formed by at least more than one batch case
        first_batch_case = batch_instance.groupby([log_ids.case]).get_group(batch_instance[log_ids.case].iloc[0])
        batch_activities = tuple(sorted(first_batch_case[log_ids.activity].values))
        batch_type = batch_instance[log_ids.batch_type].iloc[0]
        update_batch_stats(batches, batch_activities, batch_type, batch_instance, log_ids)
    # Return batches report
    return batches


def update_batch_stats(batches: dict,
                       batch_activity: tuple[str],
                       batch_type: str,
                       batch_instance: pd.DataFrame,
                       log_ids: EventLogIDs):
    # Retrieve stats for this batch
    if batch_activity not in batches:
        batches[batch_activity] = {
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
    batch_stats = batches[batch_activity][batch_type]
    # Update num instances
    batch_stats['num_instances'] += 1
    for (case_key, batch_case) in batch_instance.groupby([log_ids.case]):
        # Batch case of this activity
        batch_case_activity = batch_case.iloc[0]
        # Update batch stats with this batch case stats
        batch_stats['num_cases'] += 1
        batch_stats['total_wt'] += [batch_case_activity[log_ids.batch_total_wt]]
        batch_stats['creation_wt'] += [batch_case_activity[log_ids.batch_creation_wt]]
        batch_stats['ready_wt'] += [batch_case_activity[log_ids.batch_ready_wt]]
        batch_stats['other_wt'] += [batch_case_activity[log_ids.batch_other_wt]]


if __name__ == '__main__':
    main()
