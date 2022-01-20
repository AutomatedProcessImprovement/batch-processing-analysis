##############################################
### BAMA (Batch Activity Mining Algorithm) ###
### https://github.com/nielsmartin/bama    ###
### Credits: Niels Martin                  ###
##############################################

# Install development version
# install.packages('lubridate')
# install.packages('stringr')
# install.packages('tidyr')
# install.packages('arules')
# install.packages('arulesSequences')
# install.packages('bupaR')
# install.packages('nielsmartin-bama-bbcc130', repos = NULL, type = "source")

# Packages
library(bamalog)
library(readr)
library(dplyr)
library(lubridate)

args <- commandArgs(trailingOnly = TRUE)


################################
### Batch discovery pipeline ###
################################

# Parameters
input_log_path <- args[1]  # Path to the input log
output_log_path <- args[2]  # Path to write the log with batch information
seq_tolerated_gap <- as.numeric(args[3])  # Number of seconds for the allowed gap between sequential cases
subsequence_method <- args[4]  # Method to extract the subsequences: "all" or "freq"
case_id <- args[5]  # name of the column to identify the case
activity <- args[6]  # name of the column to identify the activity
enabled_time <- args[7]  # name of the column to identify the enablement timestamp
start_time <- args[8]  # name of the column to identify the start timestamp
end_time <- args[9]  # name of the column to identify the end timestamp
resource <- args[10]  # name of the column to identify the resource


# Read CSV
event_log <- read_csv(input_log_path)
names(event_log) <- c("case_id", "activity", "enabled", "start", "complete", "resource")
event_log[, "arrival"] <- NA
event_log[["resource"]][is.na(event_log[["resource"]])] <- "NOT_SET"


# Create seq_tolerated_gap_list (gap of 0 seconds is allowed)
seq_tolerated_gap_list <- seq_tolerated_gap_list_generator(task_log = event_log, 
                                                           seq_tolerated_gap_value = seq_tolerated_gap)

if (subsequence_method == "freq") {
    # Using frequent sequence mining instead
    subsequence_list <- identify_frequent_sequences(event_log, 0)
    subsequence_type = "mine"
} else {
    # Using all subsequences
    subsequence_list <- enumerate_subsequences(event_log, 0)
    subsequence_type = "enum"
}

# Detect batching behavior
result_log <- detect_batching(task_log = event_log,
                              act_seq_tolerated_gap_list = seq_tolerated_gap_list,
                              timestamp_format = "yyyy-mm-dd hh:mm:ss",
                              numeric_timestamps = FALSE,
                              log_and_model_based = TRUE,
                              subsequence_list = subsequence_list,
                              subsequence_type = subsequence_type,
                              within_case_seq_tolerated_gap = seq_tolerated_gap,
                              between_cases_seq_tolerated_gap = seq_tolerated_gap,
                              show_progress = F)

result_log[, "arrival"] <- NULL
names(result_log) <- c(case_id, activity, enabled_time, start_time, end_time, resource,
                       "batch_number", "batch_type", "batch_subprocess_number", "batch_subprocess_type")
write.csv(result_log, output_log_path, quote = FALSE, row.names = FALSE, fileEncoding = "UTF-8")
