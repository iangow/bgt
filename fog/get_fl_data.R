# Create a table to store forward-looking data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "fl_data"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.fl_data
            (file_name text, last_update timestamp without time zone,
                     category text, prop_fl_sents float8, num_sentences integer)")

    rs <- dbGetQuery(pg, "CREATE INDEX ON bgt.fl_data (file_name)")
}

rs <- dbDisconnect(pg)

# Make a function to count forward-looking words ----
add_fl_data <- function(file_name) {
    # Function to get word count data for all utterances in a call

    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())

    # Get tone data. Data is JSON converted to text.
    rs <- dbGetQuery(pg, paste0("

        WITH raw_data AS (
            SELECT file_name, last_update, speaker_name, speaker_number,
                unnest(sent_tokenize(speaker_text)) AS sents,
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END)
                    || '_' || context AS category
            FROM streetevents.speaker_data
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

file_names <-
    calls %>%
    filter(call_type==1L) %>%
    anti_join(processed) %>%
    select(file_name) %>%
    distinct() %>%
    as.data.frame(n=Inf)

# Apply function to get data on forward-looking words ----
# Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, add_fl_data, mc.cores=12))
