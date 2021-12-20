from datetime import timedelta

import pandas as pd
from numpy import mean

from batch_config import Configuration
from batch_processing_analysis import BatchProcessingAnalysis
from batch_processing_report import summarize_batch_waiting_times


def main():
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/ConsultaDataMining201618.csv.gz"
    config = Configuration()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[config.log_ids.start_time] = pd.to_datetime(event_log[config.log_ids.start_time], utc=True)
    event_log[config.log_ids.end_time] = pd.to_datetime(event_log[config.log_ids.end_time], utc=True)
    # Run main analysis
    batch_event_log = BatchProcessingAnalysis(event_log, config).analyze_batches()
    batch_report = summarize_batch_waiting_times(batch_event_log, config.log_ids)
    for batch_activities in batch_report:
        print("\n\nBatch formed by activities: {}".format(batch_activities))
        for batch_type in batch_report[batch_activities]:
            batch_stats = batch_report[batch_activities][batch_type]
            if batch_stats['num_instances'] > 0:
                print("\tBatch type: {}".format(batch_type))
                print("\t\tNum batch instances: {}".format(batch_stats['num_instances']))
                print("\t\tNum batch cases: {}".format(batch_stats['num_cases']))
                print("\t\tAverage overall processing time: {} sec".format(avg_duration(batch_stats['processing_time'])))
                print("\t\tAverage overall waiting time: {} sec".format(avg_duration(batch_stats['waiting_time'])))
                print("\t\tCTE: {}".format(cte(batch_stats['processing_time'], batch_stats['waiting_time'])))
                print("\t\tAverage total wt: {} sec".format(avg_duration(batch_stats['total_wt'])))
                print("\t\tAverage creation wt: {} sec".format(avg_duration(batch_stats['creation_wt'])))
                print("\t\tAverage ready wt: {} sec".format(avg_duration(batch_stats['ready_wt'])))
                print("\t\tAverage other wt: {} sec".format(avg_duration(batch_stats['other_wt'])))


def avg_duration(durations: list):
    return mean(durations)


def cte(processing_times: list, waiting_times: list):
    return sum(processing_times, timedelta(0)) / (sum(processing_times, timedelta(0)) + sum(waiting_times, timedelta(0)))


if __name__ == '__main__':
    main()
