library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

temp <- dbGetQuery(pg, "
    WITH 
    
    raw_data AS (
        SELECT a.*, b.call_date
        FROM streetevents.speaker_data AS a
        INNER JOIN streetevents.calls AS b
        USING (file_name)
        WHERE employer='Polyone Corporatio' 
            AND speaker_text ~ 'goodwill impair' AND context='qa' 
            AND role != 'Analyst'
        LIMIT 1),

    sents AS (
        SELECT *, unnest(sent_tokenize(speaker_text)) AS sentence
        FROM raw_data)
    SELECT employer, sentence, (fog_data(sentence)).*
    FROM sents
    WHERE sentence ~ 'goodwill impair'
    ORDER BY fog DESC")
temp

dbDisconnect(pg)