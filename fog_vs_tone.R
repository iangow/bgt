# Get the data from PostgreSQL
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
fog_w_tone <- dbGetQuery(pg, "
    SET work_mem='10GB';    

    SELECT * 
    FROM bgt.fog_aggregated
    INNER JOIN bgt.word_counts
    USING (file_name, category)")

# Scale tone variables by the number of words
for (var in c("litigious", "positive", "negative", "modal_strong", "modal_weak",
              "uncertainty")) {
    fog_w_tone[, var] <- fog_w_tone[, var]/fog_w_tone$num_words
}

# Regress FOG on the 6 "tone" variables by context
for (cat in c("comp_pres", "comp_qa", "anal_qa")) {
    cat("Context: ", cat, ":\n")
    print(summary(lm(fog ~ litigious + positive + negative 
                            + modal_strong + modal_weak + uncertainty, 
               data=subset(fog_w_tone, category==cat))))
}

corr_table <- function(df) {
    spearman <- cor(df, method = "spearman")
    pearson <- cor(df, method = "pearson")
    return(spearman * upper.tri(spearman) + pearson * !upper.tri(pearson))
}

df <- fog_w_tone[, c("fog", "litigious", "positive", "negative", 
                     "modal_strong", "modal_weak", "uncertainty")]
corr_table(df)