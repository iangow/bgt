
# Create a table to store the data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "fog_speaker"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.fog_speaker
            (file_name text,
             last_update timestamp without time zone,
             context text,
             speaker_number integer,
             fog float8, num_words integer, percent_complex float8,
             num_sentences integer, fog_original float8,
             num_sentences_original integer)")

    rs <- dbGetQuery(pg, "CREATE INDEX ON bgt.fog_speaker (file_name)")
}
rs <- dbDisconnect(pg)

# Make a function to get fog data ----
# addData funcion ----
# A function that calls the parameterized query to process each file
addData <- function(file_name) {
    library("RPostgreSQL")
    sql <- paste(readLines("fog/get_fog_speaker_data.sql"),
                 collapse="\n")

    pg <- dbConnect(PostgreSQL())
    dbGetQuery(pg, sprintf(sql, file_name, file_name))
    dbDisconnect(pg)
}

# Get list of files without fog data ----
library(dplyr)
pg <- src_postgres()

calls <- tbl(pg, sql("SELECT *  FROM streetevents.calls"))

processed <- tbl(pg, sql("SELECT * FROM bgt.fog_speaker"))

file_names <-
    calls %>%
    filter(call_type==1L) %>%
    anti_join(processed) %>%
    select(file_name) %>%
    distinct() %>%
    as.data.frame(n=-1)

# Apply function to get fog data ----
# Run on 12 cores.
pg <- dbConnect(PostgreSQL())
library(parallel)
system.time(temp <- mclapply(file_names$file_name,
                             addData, mc.cores=4))

rs <- dbDisconnect(pg)
