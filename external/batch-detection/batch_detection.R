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
input_log_path <- args[1]
output_log_path <- args[2]
timestamp_format <- args[3]


# Read CSV
event_log <- read_csv(input_log_path)
names(event_log) <- c("case_id", "activity", "arrival", "start", "complete", "resource")
event_log[["resource"]][is.na(event_log[["resource"]])] <- "NOT_SET"


# Create seq_tolerated_gap_list (gap of 0 seconds is allowed)
seq_tolerated_gap_list <- seq_tolerated_gap_list_generator(task_log = event_log, 
                                                           seq_tolerated_gap_value = 0)

subsequence_list <- enumerate_subsequences(event_log, 0)
# Use the following line for using frequent sequence mining instead
# subsequence_list <- identify_frequent_sequences(event_log, 0)

# Detect batching behavior
result_log <- detect_batching(task_log = event_log,
                              act_seq_tolerated_gap_list = seq_tolerated_gap_list,
                              timestamp_format = timestamp_format,
                              numeric_timestamps = FALSE,
                              log_and_model_based = TRUE,
                              subsequence_list = subsequence_list,
                              subsequence_type = "enum",
                              # use `mine` to use frequence sequence mining
                              # subsequence_type = "mine",
                              within_case_seq_tolerated_gap = 0,
                              between_cases_seq_tolerated_gap = 0,
                              show_progress = F)

names(result_log) <- c("case_id", "Activity", "enabled_time", "start_time", "end_time", "Resource",
                       "batch_number", "batch_type", "batch_subprocess_number", "batch_subprocess_type")
write.csv(result_log, output_log_path, quote = FALSE, row.names = FALSE)
