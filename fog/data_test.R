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
temp <- expand.grid(var=c("fog", "num_complex_words", "num_words", "num_sentences"),
                    category=c("comp_pres", "comp_qa", "anal_qa"))
var_list <- paste0(temp$var, "_", temp$category)
rm(temp)

for (var in var_list) {
    cat(var, ":", cor(combined[, paste0(var, "_old")], combined[, paste0(var, "_new")],
                 use = "complete.obs"), "\n")
}
