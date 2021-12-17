import pandas as pd

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
    print()


if __name__ == '__main__':
    main()
