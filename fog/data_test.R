# Read in old data set
library(foreign)
original <- read.dta("~/Downloads/merged_fog_data.dta")

# Load in new data set
load("~/git/bgt/data/fog_data.Rdata")
getNobs <- function(df) {
    # Function to partly replicate PROC MEANS
    getRow <- function(var.name) {
        return(data.frame(variable=var.name, 
                          n=length(df[, var.name]), 
                          nmiss=sum(is.na(df[, var.name]))))
    }
    
    library(parallel)
    temp <- mclapply(names(df), getRow)
    do.call("rbind", temp)
}

getNobs(fog_data)

# Merge old and new data sets
combined <- merge(original, fog_data, by="file_name", suffixes = c("_old", "_new"))

# Look at correlations between old and new fog variables
cor(combined$fog_comp_pres_old, combined$fog_comp_pres_new, 
    use = "complete.obs")
cor(combined$fog_comp_qa_old, combined$fog_comp_qa_new, use = "complete.obs")
cor(combined$fog_anal_qa_old, combined$fog_anal_qa_new, use = "complete.obs")
