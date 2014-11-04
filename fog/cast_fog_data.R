# 
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

# Recast data so that three rows expand into 3*K columns
fog.data <- dbGetQuery(pg, "
    SELECT * 
    FROM bgt.fog_decomposed
    LEFT JOIN bgt.fl_data
    USING (file_name, category)
    LEFT JOIN bgt.tone_data
    USING (file_name, category)
")

library(reshape)
cast.data <- NULL
for (i in setdiff(names(fog.data), c("file_name", "category"))) {
    temp <- cast(fog.data, file_name ~ category, value=i, fun.aggregate =sum)
    new.names <- paste(i, names(temp)[2:length(names(temp))], sep="_")
    names(temp) <- c("file_name", new.names)
    if (is.null(cast.data)) {
        cast.data <- temp
    } else {
        cast.data <- merge(cast.data, temp, all=TRUE)
    }
}

# Get call-level variants of Loughran and McDonald (2013) and 
# forward-looking variables
fog.call.data <- dbGetQuery(pg, "
    WITH raw_data AS (
        SELECT * 
        FROM bgt.fog_decomposed
        LEFT JOIN bgt.fl_data
        USING (file_name, category)
        LEFT JOIN bgt.tone_data
        USING (file_name, category))
    SELECT file_name,
        CASE WHEN sum(num_sentences)>0 
            THEN sum(num_sentences*prop_fl_sents)/sum(num_sentences) END 
        AS prop_fl_sents,
        sum(litigious)::integer AS litigious,
        sum(positive)::integer AS positive,
        sum(uncertainty)::integer AS uncertainty,
        sum(negative)::integer AS negative,
        sum(modal_strong)::integer AS modal_strong,
        sum(modal_weak)::integer AS modal_weak,
        sum(word_count)::integer AS word_count
    FROM raw_data
    GROUP BY file_name
")

cast.data.plus <- merge(cast.data, fog.call.data, by="file_name")


rs <- dbWriteTable(pg, c("bgt", "fog_recast"), cast.data.plus, 
             overwrite=TRUE, row.names=FALSE)

rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='5GB';
    CREATE INDEX ON bgt.fog_recast (file_name)")

rm(cast.data, fog.call.data, cast.data.plus)

dbDisconnect(pg)
rm(rs, pg, i)
