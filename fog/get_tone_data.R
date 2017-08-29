# Create a table to store sent_counts data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "tone_data"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.tone_data
            (
              file_name text,
              last_update timestamp with time zone,
              category text,
              word_count integer,
              litigious integer,
              positive integer,
              uncertainty integer,
              negative integer,
              modal_strong integer,
              modal_weak integer);

        CREATE INDEX ON bgt.tone_data (file_name);")
}
rs <- dbDisconnect(pg)

# Make a function to calculate tone variables ----
# Need to run word_count.sql first.
addToneData <- function(file_name) {
    # Function to get word count data for all utterances in a call
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())

    # Get tone data. Data is JSON converted to text.
    tone_raw <- dbGetQuery(pg, paste0("
        WITH
        raw_data AS (
            SELECT file_name, last_update,
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) ||
                        '_' || context AS category,
                speaker_text
            FROM streetevents.speaker_data
            WHERE speaker_name != 'Operator' AND file_name ='", file_name, "')
        SELECT file_name, last_update, category,
            tone_count(string_agg(speaker_text, ' '))::text AS tone_count,
            word_count(string_agg(speaker_text, ' ')) AS word_count
        FROM raw_data
        GROUP BY file_name, last_update, category"))

    # Convert JSON-as-text to records where each key becomes a column
    # PostgreSQL 9.4 offers json_to_record, but I'm on 9.3.
    require(jsonlite)
    if (dim(tone_raw)[1]>0) {
        tone_data <- as.data.frame(do.call(rbind,
                                       lapply(tone_raw$tone_count, fromJSON)))

        # Convert JSON data from numeric to integers
        for (i in names(tone_data)) {
            tone_data[,i] <- as.integer(tone_data[,i])
        }

        # Combine converted JSON data with other fields
        tone_data <- cbind(subset(tone_raw, TRUE, select=-tone_count), tone_data)

        # Put data back in database.
        dbWriteTable(pg, c("bgt", "tone_data"), tone_data,
                     append=TRUE, row.names=FALSE)

    }
    rs <- dbDisconnect(pg)
}

# Get list of files to process ----
# Get a list of file names for which we need to get tone data.
pg <- dbConnect(PostgreSQL())

file_names <-  dbGetQuery(pg, "
    WITH files AS (
        SELECT file_name, last_update
        FROM streetevents.calls
        WHERE event_type=1
        EXCEPT
        SELECT file_name, last_update
        FROM bgt.tone_data)
    SELECT DISTINCT file_name
    FROM files
")
rs <- dbDisconnect(pg)

# Apply function to get tone data ----
# Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, addToneData, mc.cores=24))
