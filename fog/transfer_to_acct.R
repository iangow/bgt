library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

fog_data <- dbGetQuery(pg,"
    SELECT c.permno, b.call_date::date, 
        a.*, d.*
    FROM bgt.fog_recast AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    INNER JOIN streetevents.crsp_link AS c
    USING (file_name)
    INNER JOIN bgt.within_call_data AS d
    USING (file_name)
")

# library(foreign)
# rs <- write.dta(dataframe =fog_data, file ="data/fog_data.dta")
save(fog_data, file="data/fog_data.Rdata")
system("/Applications/StatTransfer12/st data/fog_data.Rdata data/fog_data.sas7bdat -y")

dbDisconnect(pg)