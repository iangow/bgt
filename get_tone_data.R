

# Need to run word_count.sql first.
addToneData <- function(file_name) {
    # Function to get word count data for all utterances in a call
    
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())
    
    # Get tone data. Data is JSON converted to text.
    tone_raw <- dbGetQuery(pg, paste0("
        WITH raw_data AS (
            SELECT file_name, 
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context AS category,
                speaker_text
            FROM streetevents.speaker_data
            WHERE speaker_name != 'Operator' AND file_name ='", file_name, "')
        SELECT file_name, category, 
            tone_count(string_agg(speaker_text, ' ')) AS tone_count,
            word_count(string_agg(speaker_text, ' ')) AS word_count
        FROM raw_data
        GROUP BY file_name, category"))
    
    # Convert JSON-as-text to records where each key becomes a column
    # PostgreSQL 9.4 offers json_to_record, but I'm on 9.3.
    require(RJSONIO)
    tone_data <- as.data.frame(do.call(rbind,
                                       lapply(tone_raw$tone_count, fromJSON)))
    
    # Convert JSON data from numeric to integers
    for (i in names(tone_data)) {
        tone_data[,i] <- as.integer(tone_data[,i])
    }
    
    # Combine converted JSON data with other fields
    tone_data <- cbind(subset(tone_raw, TRUE, select=-tone_count), tone_data)
    
    # Put data back in database.
    dbWriteTable(pg, c("bgt", "word_counts"), tone_data,
                 append=TRUE, row.names=FALSE)
    
    rs <- dbDisconnect(pg)
    
}

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.word_counts")

# Get a list of file names for which we need to get tone data.
file_names <-  dbGetQuery(pg, "
    SELECT DISTINCT file_name
    FROM streetevents.calls
    WHERE call_type=1 -- AND file_name NOT IN (SELECT file_name FROM bgt.word_counts)")

# Apply function to get tone data. Run on 12 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, addToneData, mc.cores=8))
rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='1GB';
    CREATE INDEX ON bgt.word_counts (file_name)")
rs <- dbDisconnect(pg)

