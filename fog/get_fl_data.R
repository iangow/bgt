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
    pg <- dbConnect(PostgreSQL())

    dbGetQuery(pg,
               sprintf("DELETE FROM bgt.fog WHERE file_name='%s'", file_name))

    # Get tone data. Data is JSON converted to text.
    rs <- dbGetQuery(pg, paste0("

        WITH latest_calls AS (
            SELECT file_name, max(last_update) AS last_update
            FROM streetevents.calls
            GROUP BY file_name),

        raw_data AS (
            SELECT file_name, last_update, speaker_name, speaker_number,
                unnest(sent_tokenize(speaker_text)) AS sents,
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END)
                    || '_' || context AS category
            FROM streetevents.speaker_data
            INNER_JOIN lastest_calls
            USING (file_name, last_update)
            WHERE speaker_name != 'Operator' AND file_name = '", file_name, "')
        INSERT INTO bgt.fl_data (file_name, last_update, category, prop_fl_sents, num_sentences)
        SELECT file_name, last_update, category, prop_fl_sents(array_agg(sents)),
            array_length(array_agg(sents), 1) AS num_sentences
        FROM raw_data
        GROUP BY file_name, last_update, category"))

    rs <- dbDisconnect(pg)

}

# Get list of files to process ----
library(dplyr)
pg <- src_postgres()

calls <- tbl(pg, sql("SELECT *  FROM streetevents.calls"))

processed <- tbl(pg, sql("SELECT * FROM bgt.fl_data"))

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update = max(last_update))

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
