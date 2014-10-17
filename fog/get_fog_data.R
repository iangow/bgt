
# Create a table to store the data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
    
rs <- dbGetQuery(pg, "
    DROP TABLE IF EXISTS bgt.fog;

    CREATE TABLE bgt.fog 
        (file_name text, category text, fog float8,
            num_words integer,
            percent_complex float8,
            num_sentences integer)")

rs <- dbDisconnect(pg)

# Make a function to run regressions ----
get_fog_data <- function(file_name) {
    # Function to get statistics for within-call regressions
    # of fog of answers on fog of questions.
    
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())
    
    # Get fog data
    reg_data <- dbGetQuery(pg, paste0("
        INSERT INTO bgt.fog
        WITH raw_data AS (
            SELECT file_name, 
                (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context
                    AS category, speaker_text
            FROM streetevents.speaker_data
            WHERE file_name='", file_name, "' AND speaker_name != 'Operator'),
        
        call_text AS (
            SELECT file_name, category, string_agg(speaker_text, ' ') AS all_text
            FROM raw_data
            GROUP BY file_name, category)
            
        SELECT file_name, category, (fog_data(all_text)).*
        FROM call_text"))
    
    dbDisconnect(pg)
}

# Get list of files and run regressions ------

pg <- dbConnect(PostgreSQL())

# Get a list of file names for which we need to get tone data.
file_names <-  dbGetQuery(pg, "
    SELECT DISTINCT file_name
    FROM streetevents.calls
    WHERE call_type=1 AND file_name NOT IN (SELECT file_name FROM bgt.fog)")

# Apply function to get tone data. Run on 12 cores.
library(parallel)
# system.time(temp <- lapply(file_names$file_name[1271:3265], get_fog_reg_data))
system.time(temp <- mclapply(file_names$file_name, get_fog_data, mc.cores=8))
rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='1GB';
    CREATE INDEX ON bgt.fog (file_name)")
rs <- dbDisconnect(pg)
