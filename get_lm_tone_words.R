
category <- c("positive", "negative", "uncertainty", 
                "litigious", "modal_strong", "modal_weak")

base_url <- "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists"

url <- file.path(base_url, 
                 c("LoughranMcDonald_Positive.csv",
                   "LoughranMcDonald_Negative.csv",
                   "LoughranMcDonald_Uncertainty.csv",
                   "LoughranMcDonald_Litigious.csv",
                   "LoughranMcDonald_ModalStrong.csv",
                   "LoughranMcDonald_ModalWeak.csv"))

df <- data.frame(category, url, stringsAsFactors=FALSE)

getWords <- function(url) {
    words <- read.csv(url, as.is=TRUE)    
    paste(words[,1], collapse=",")
}
df$words <- unlist(lapply(df$url, getWords))
library(RPostgreSQL)

pg <- dbConnect(PostgreSQL())
rs <- dbWriteTable(pg, c("bgt", "lm_tone"), df, row.names=FALSE, overwrite=TRUE)

rs <- dbGetQuery(pg, "
    BEGIN;
    ALTER TABLE bgt.lm_tone ADD COLUMN word_list text[];
    UPDATE bgt.lm_tone SET word_list = regexp_split_to_array(words, ',');
    ALTER TABLE bgt.lm_tone DROP COLUMN words;
    COMMIT;")
rs <- dbDisconnect(pg)