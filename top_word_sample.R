library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

temp <- dbGetQuery(pg, "
    SET work_mem='10GB';

    WITH 

    raw AS (
      SELECT context, speaker_text
      FROM streetevents.speaker_data
      LIMIT 100000),
    
    by_context AS (
      SELECT context, 
        top_words(word_counts(string_agg(speaker_text, ' ')), 1000)
      FROM raw
      GROUP BY context)
    
    SELECT context, unnest(top_words) AS word
    FROM by_context;")

rs <- dbDisconnect(pg)

write.csv(temp, file="~/Dropbox/research/BuGT/data/words.csv", row.names=FALSE)
