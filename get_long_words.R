

# Need to run word_count.sql first.
addLongWordData <- function(file_name) {
    # Function to get word count data for all utterances in a call
    
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())
    
    rs <- dbGetQuery(pg, "SET work_mem='1GB'")
    # Get tone data. Data is JSON converted to text.
    tone_raw <- dbGetQuery(pg, paste0("
        INSERT INTO bgt.long_words
        WITH raw_data AS (
            SELECT file_name, 
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END)
                 || '_' || context AS category, speaker_text
            FROM streetevents.speaker_data
            WHERE speaker_name != 'Operator' AND file_name ='", file_name, "'),
            
        long_words AS (
            SELECT file_name, category, unnest(getLongWords(speaker_text)) AS long_words
            FROM raw_data)

        SELECT file_name, category, array_agg(long_words)
        FROM long_words
        GROUP BY file_name, category"))
    
    rs <- dbDisconnect(pg)
    
}

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

rs <- dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.long_words")

rs <- dbGetQuery(pg, "
    CREATE TABLE bgt.long_words 
        (file_name text, 
         category text, 
         long_words text[])")

rs <- dbDisconnect(pg)
pg <- dbConnect(PostgreSQL())
# Get a list of file names for which we need to get tone data.
file_names <-  dbGetQuery(pg, "
    SELECT DISTINCT file_name
    FROM streetevents.crsp_link
    WHERE file_name NOT IN (SELECT file_name FROM bgt.long_words)")

# Apply function to get tone data. Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, addLongWordData, mc.cores=8, mc.preschedule=FALSE))
rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='1GB';
    CREATE INDEX ON bgt.word_counts (file_name)")
rs <- dbDisconnect(pg)

