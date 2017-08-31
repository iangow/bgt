# Create a table to store long-word data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "long_words"))) {

    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.long_words
            (file_name text,
             last_update timestamp with time zone,
             category text,
             long_words text[])")

    rs <- dbGetQuery(pg, "CREATE INDEX ON bgt.long_words (file_name)")
}

rs <- dbDisconnect(pg)

# Make a function to count long words ----
# Need to run getLongWords.sql first.
addLongWordData <- function(file_name) {
    # Function to get word count data for all utterances in a call

    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())

    rs <- dbGetQuery(pg, "SET work_mem='1GB'")

    # Get tone data. Data is JSON converted to text.
    tone_raw <- dbGetQuery(pg, paste0("
        INSERT INTO bgt.long_words
        WITH raw_data AS (
            SELECT file_name, last_update,
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END)
                 || '_' || context AS category, speaker_text
            FROM streetevents.speaker_data
            WHERE speaker_name != 'Operator' AND file_name ='", file_name, "'),

        long_words AS (
            SELECT file_name, last_update, category,
                unnest(getLongWords(speaker_text)) AS long_words
            FROM raw_data)

        SELECT file_name, last_update, category, array_agg(long_words)
        FROM long_words
        GROUP BY file_name, last_update, category"))

    rs <- dbDisconnect(pg)
}

# Get list of files to process ----
# Get a list of file names for which we need to get tone data.
library(dplyr)
pg <- src_postgres()

calls <- tbl(pg, sql("SELECT *  FROM streetevents.calls"))

processed <- tbl(pg, sql("SELECT * FROM bgt.long_words"))

file_names <-
    calls %>%
    filter(event_type==1L) %>%
    anti_join(processed) %>%
    select(file_name) %>%
    distinct() %>%
    collect(n=Inf)

# Apply function to get data on long words ----
# Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, addLongWordData, mc.cores=8,
                             mc.preschedule=FALSE))
