import pandas as pd

from batch_config import Configuration
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
    batch_analyzer.analyze_batches()
    print()


if __name__ == '__main__':
    main()
