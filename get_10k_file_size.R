
getFileSize <- function(file_name) {
    library(RCurl)
    url = paste0("ftp://ftp.sec.gov/", file_name)
    xx = getURL(url, nobody=1L, header=1L, userpwd="anonymous:igow@hbs.edu")
    temp <- strsplit(xx, "\r\n")
    as.integer(gsub("Content-Length:\\s+", "", temp[[1]][1]))
}

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

file_list <- dbGetQuery(pg, "
                        SELECT *
                        FROM filings.filings
                        WHERE form_type='10-K'
                        LIMIT 100")

file_size <- unlist(lapply(file_list$file_name[2], getFileSize))

