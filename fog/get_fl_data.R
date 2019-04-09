#!/usr/bin/env Rscript
# Create a table to store forward-looking data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "fl_data"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.fl_data
            (file_name text, last_update timestamp with time zone,
                     category text, prop_fl_sents float8, num_sentences integer)")

    rs <- dbGetQuery(pg, "CREATE INDEX ON bgt.fl_data (file_name)")
}

rs <- dbDisconnect(pg)

# Make a function to count forward-looking words ----
add_fl_data <- function(file_name) {
    # Function to get word count data for all utterances in a call

    library(RPostgreSQL)
    library(dplyr, warn.conflicts = FALSE)
    pg <- dbConnect(PostgreSQL())

    the_file_name <- file_name
    rm(file_name)

    dbExecute(pg, sprintf("DELETE FROM bgt.fog WHERE file_name='%s'",
                          the_file_name))

    dbExecute(pg, "SET search_path TO streetevents, public")

    calls <- tbl(pg, "calls")
    speaker_data <- tbl(pg, "speaker_data")

    latest_calls <-
        calls %>%
        group_by(file_name) %>%
        summarize(last_update = max(last_update, na.rm = TRUE))

    raw_data <-
        speaker_data %>%
        inner_join(latest_calls, by = c("file_name", "last_update")) %>%
        mutate(sents = unnest(sent_tokenize(speaker_text)),
               category = if_else(role=='Analyst', 'anal', 'comp') %||% '_' %||% context) %>%
        select(file_name, last_update, speaker_name, speaker_number, sents, category) %>%
        filter(speaker_name != 'Operator', file_name == the_file_name) %>%
        compute(name = "raw_data")

    dbExecute(pg, "
        INSERT INTO bgt.fl_data (file_name, last_update, category, prop_fl_sents,
                                            num_sentences)
            SELECT file_name, last_update, category, prop_fl_sents(array_agg(sents)),
                array_length(array_agg(sents), 1) AS num_sentences
            FROM raw_data
            GROUP BY file_name, last_update, category")


    # Get tone data. Data is JSON converted to text.

    rs <- dbDisconnect(pg)

}

# Get list of files to process ----
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())

calls <- tbl(pg, sql("SELECT *  FROM streetevents.calls"))

processed <- tbl(pg, sql("SELECT * FROM bgt.fl_data"))

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update = max(last_update, na.rm = TRUE))

file_names <-
    calls %>%
    inner_join(latest_calls) %>%
    filter(event_type==1L) %>%
    anti_join(processed) %>%
    select(file_name) %>%
    distinct() %>%
    collect(n=Inf)

# Apply function to get data on forward-looking words ----
# Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, add_fl_data, mc.cores=12))
