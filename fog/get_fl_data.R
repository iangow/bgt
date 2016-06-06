# Create a table to store forward-looking data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "fl_data"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.fl_data
            (file_name text, category text, prop_fl_sents float8)")

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
            SELECT file_name, speaker_name, speaker_number,
                unnest(sent_tokenize(speaker_text)) AS sents,
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END)
                    || '_' || context AS category
            FROM streetevents.speaker_data
            WHERE speaker_name != 'Operator' AND file_name = '", file_name, "')
        INSERT INTO bgt.fl_data
        SELECT file_name, category, prop_fl_sents(array_agg(sents))
        FROM raw_data
        GROUP BY file_name, category"))

    rs <- dbDisconnect(pg)

}

# Get list of files to process ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

# Get a list of file names for which we need to get tone data.
file_names <-  dbGetQuery(pg, "
    SELECT DISTINCT file_name
    FROM streetevents.calls
    WHERE call_type=1 AND file_name NOT IN (SELECT file_name FROM bgt.fl_data)")

# Apply function to get data on forward-looking words ----
# Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, add_fl_data, mc.cores=8))
rs <- dbDisconnect(pg)
