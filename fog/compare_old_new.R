library(haven)
fog_data_old <- read_sas("data/fog_data.sas7bdat")
fog_data_new <- read_sas("data/fog_data_new.sas7bdat")

old_names <- colnames(fog_data_old)
old_names <- old_names[!grepl("anal_pres", old_names)]
new_names <- colnames(fog_data_new)

setdiff(old_names, new_names)
setdiff(new_names, old_names)
