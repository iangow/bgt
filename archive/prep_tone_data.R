library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
    
tone <- dbGetQuery(pg, "SELECT * FROM bgt.word_counts")

library(dplyr)

get_summ <- function(var) {
    temp <- as.data.frame(tapply(X = tone[, var], list(tone$category), FUN = sum))
    names(temp) <- var
    return(temp)
}

summ <- do.call("cbind", lapply(names(tone)[3:9], get_summ))

summ[, -1] <- summ[, -1]/summ[, 1] * 100
