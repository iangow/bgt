
category <- c("positive", "negative", "uncertainty", 
                "litigious", "modal_strong", "modal_weak")
url <- c("http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists/LoughranMcDonald_Positive.csv",
          "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists/LoughranMcDonald_Negative.csv",
          "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists/LoughranMcDonald_Uncertainty.csv",
          "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists/LoughranMcDonald_Litigious.csv",
          "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists/LoughranMcDonald_ModalStrong.csv",
          "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists/LoughranMcDonald_ModalWeak.csv")

df <- data.frame(category, url, stringsAsFactors=FALSE)

getWords <- function(url) {
    words <- read.csv(url, as.is=TRUE)    
    paste(words[,1], collapse=",")
}
df$words <- unlist(lapply(df$url, getWords))
library(RPostgreSQL)

pg <- dbConnect(PostgreSQL())
rs <- dbWriteTable(pg, c("bgt", "lm_tone"), df, row.names=FALSE, overwrite=TRUE)

rs <- dbGetQuery(pg, "ALTER TABLE bgt.lm_tone ADD COLUMN word_list text[];")
rs <- dbGetQuery(pg, "UPDATE bgt.lm_tone SET word_list = regexp_split_to_array(words, ',')")
rs <- dbGetQuery(pg, "ALTER TABLE bgt.lm_tone DROP COLUMN words;")
rs <- dbDisconnect(pg)