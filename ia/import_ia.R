library(foreign)

ia_data <- read.dta("/Volumes/2TB/data/BuGT/taqdailywithperm.dta")
ia_data$date <- as.Date(ia_data$date, origin='1960-01-01')
ia_data$last_date <- NULL
ia_data$first_date <- NULL
names(ia_data) <- tolower(names(ia_data))
# library(reshape)
# bins <- subset(ia_data, select=c("symbol", "date", paste("bin", 1:5, sep="")))
# temp <- melt(bins, id=c("symbol", "date"), measured= paste("bin", 1:5, sep=""))

library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
pg <- dbConnect(drv, dbname="crsp")
rs <- dbWriteTable(pg, c("bgt", "ia_daily"), ia_data, overwrite=TRUE, row.names=FALSE)
rm(ia_data)
rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='4GB';    

    ALTER TABLE bgt.ia_daily ADD PRIMARY KEY (permno, date);")

load("~/Dropbox/research/BuGT/data/Measures_MDA.rdata")
rs <- dbWriteTable(pg, c("bgt", "measures_mda"), Measures_MDA,
                       overwrite=TRUE, row.names=FALSE)
rm(Measures_MDA)

load("~/Dropbox/research/BuGT/data/Measures_EarnAnnc.rdata")
rs <- dbWriteTable(pg, c("bgt", "measures_earnannc"), Measures_EarnAnnc,
                 overwrite=TRUE, row.names=FALSE)
rm(Measures_EarnAnnc)

rs <- dbDisconnect(pg)

# for(i in names(ia_data)) {
#   if (tolower(i) != i) {
#     dbGetQuery(pg, paste("ALTER TABLE bgt.ia_daily RENAME \"", i, "\" TO ", tolower(i), sep=""))
#   }
# }

