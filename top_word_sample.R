library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

system.time(temp <- dbGetQuery(pg, "
    SET work_mem='10GB';

    SELECT sic2,
        top_words(long_words, 100)
    FROM bgt.word_freq"))

rs <- dbDisconnect(pg)

write.csv(temp, file="~/Dropbox/research/BuGT/data/words.csv", row.names=FALSE)
