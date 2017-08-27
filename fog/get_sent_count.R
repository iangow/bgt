# Create a table to store the data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "sent_counts"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.sent_counts
            (file_name text, last_update timestamp without time zone,
             category text, num_sentences integer)")

    rs <- dbGetQuery(pg, "CREATE INDEX ON bgt.sent_counts (file_name)")
}
rs <- dbDisconnect(pg)

# Make a function to run regressions ----
get_sent_count <- function(file_name) {
    # Function to get statistics for within-call regressions
    # of fog of answers on fog of questions.

    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())

    # Get fog data
    reg_data <- dbGetQuery(pg, paste0("
        SET work_mem='1GB';

        INSERT INTO bgt.sent_counts (file_name, last_update, category, num_sentences)
        WITH raw_data AS (
            SELECT file_name, last_update,
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context
                    AS category, speaker_text
            FROM streetevents.speaker_data
            WHERE file_name='", file_name, "' AND speaker_name != 'Operator'),

        call_text AS (
            SELECT file_name, last_update, category,
                string_agg(speaker_text, ' ') AS all_text
            FROM raw_data
            GROUP BY file_name, last_update, category)

        SELECT file_name, last_update, category, sent_count(all_text) AS num_sentences
        FROM call_text"))

    dbDisconnect(pg)
}

# Get list of files to process ----
library(dplyr)
pg <- src_postgres()

calls <- tbl(pg, sql("SELECT *  FROM streetevents.calls"))

processed <- tbl(pg, sql("SELECT * FROM bgt.sent_counts"))

file_names <-
    calls %>%
    filter(event_type==1L) %>%
    anti_join(processed) %>%
    select(file_name) %>%
    distinct() %>%
    as.data.frame(n=Inf)

# Apply function to get sentence count data data ----
# Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, get_sent_count, mc.cores=8))
