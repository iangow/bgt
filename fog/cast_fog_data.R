# 
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
library(reshape2)

sql <- paste(readLines("fog/fog_aggregate.sql"), collapse="\n")
dbGetQuery(pg, sql)

# Recast data so that three rows expand into 3*K columns
fog.data <- dbGetQuery(pg, "SELECT * FROM bgt.fog_aggregated")

alt.fog.data <- dbGetQuery(pg, "
    SELECT *, 0.4*(num_complex_words/num_sentences FROM bgt.fog_aggregated")

cast.data <- NULL
for (i in setdiff(names(fog.data), c("file_name", "category"))) {
  temp <- cast(fog.data, file_name ~ category, value=i)
  new.names <- paste(i, names(temp)[2:length(names(temp))], sep="_")
               names(temp) <- c("file_name", new.names)
  if (is.null(cast.data)) {
    cast.data <- temp
  } else {
    cast.data <- merge(cast.data, temp, all=TRUE)
  }
}

rs <- dbWriteTable(pg, c("bgt", "fog_recast"), cast.data, 
             overwrite=TRUE, row.names=FALSE)

rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='5GB';
    CREATE INDEX ON bgt.fog_recast (file_name)")

merged.fog.data <- dbGetQuery(pg, "
  SELECT DISTINCT *, 
    0.4 * (100*(num_complex_words_comp_qa+num_complex_words_anal_qa)/
    (num_words_comp_qa+num_words_anal_qa) +
    (num_words_comp_qa+num_words_anal_qa)/(num_sentences_comp_qa+num_sentences_anal_qa)) AS fog_qa
  FROM bgt.ticker_match
  INNER JOIN bgt.fog_recast
  USING (file_name)")

library(foreign)

write.dta(merged.fog.data, file="~/Dropbox/research/BuGT/data/merged_fog_data.dta", version=11L)
head(merged.fog.data)
dim(merged.fog.data)
table(merged.fog.data$datadate)
