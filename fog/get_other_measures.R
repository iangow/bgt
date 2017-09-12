library(dplyr, warn.conflicts = FALSE)
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

RPostgreSQL::dbGetQuery(pg, "SET work_mem='3GB'")
syllable_data <-
    tbl(pg, sql("SELECT * FROM streetevents.syllable_data")) %>%
    select(-last_update)

category_sql <- "(CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context"

speaker_data <- tbl(pg, sql("SELECT * FROM streetevents.speaker_data"))

category <-
    speaker_data %>%
    filter(speaker_name != 'Operator') %>%
    mutate(category = sql("(CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context")) %>%
    select(file_name, last_update, context, speaker_number, category)

nchars <- tbl(pg, sql("SELECT * FROM bgt.nchars"))

# Flesch Kincaid: The Flesch-Kincaid Readability Index:
# 0.39 * (number of words /  number of sentences)
# + 11.8 * (number of syllables / number of words) - 15.59
fk_sql <- "CASE WHEN sent_count > 0 AND word_count > 0
              THEN .39 * (word_count /  sent_count) +
               11.8 * (num_syllables / word_count) - 15.59
              END"

# LIX: The LIX Readability Index:
# (number of words /  number of sentences)
# + (number of words over 6 letters * 100/ number of words)
lix_sql <- "CASE WHEN sent_count > 0 AND word_count > 0
    THEN (word_count /  sent_count) + (word_7_count * 100/ word_count) END"

# RIX: The RIX Readability Index:
# (number of words with 7 characters or more) / (number of sentences)
rix_sql <- "CASE WHEN sent_count > 0
               THEN  word_7_count /sent_count END"

# SMOG
# The SMOG Index:
# 1.043 * sqrt(30 * number of words with > two syllables / number of sentences) + 3.1291
smog_sql <- "CASE WHEN sent_count > 0
                THEN 1.043 * sqrt(30 * multisyl_count / sent_count) + 3.1291 END"

# ARI: The Automated Readability Index (ARI):
# 4.71 * (number of characters / number of words)
# + 0.5 * (number of words / number of sentences) - 21.43
ari_sql <- "CASE WHEN word_count > 0 AND sent_count > 0
            THEN 4.71 * (nchars / word_count) + 0.5 * (word_count / sent_count) - 21.43 END"

dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.other_measures")
system.time({
    processed <-
        syllable_data %>%
        inner_join(nchars) %>%
        inner_join(category) %>%
        mutate(monosyl_count=sql("COALESCE((syllable_data->'syllable_counts'->'1')::text::float8, 0)"),
               bisyl_count=sql("COALESCE((syllable_data->'syllable_counts'->'2')::text::float8,0)")) %>%
        mutate(sent_count=sql("(syllable_data->'sent_count')::text::float8"),
               word_count=sql("(syllable_data->'word_count')::text::float8"),
               word_7_count=sql("(syllable_data->'word_7_count')::text::float8"),
               num_syllables=sql("(syllable_data->'num_syllables')::text::float8")) %>%
        mutate(multisyl_count = word_count - monosyl_count - bisyl_count) %>%
        select(-syllable_data, -context, -speaker_number) %>%
        group_by(file_name, last_update, category) %>%
        summarize_all(funs(sum)) %>%
        mutate(fk = sql("CASE WHEN sent_count > 0 AND word_count > 0
              THEN .39 * (word_count /  sent_count) +
               11.8 * (num_syllables / word_count) - 15.59
              END"),
               lix = sql("CASE WHEN sent_count > 0 AND word_count > 0
    THEN (word_count /  sent_count) + (word_7_count * 100/ word_count) END"),
               rix = sql("CASE WHEN sent_count > 0
               THEN  word_7_count /sent_count END"),
               ari = sql("CASE WHEN word_count > 0 AND sent_count > 0
            THEN 4.71 * (nchars / word_count) + 0.5 * (word_count / sent_count) - 21.43 END"),
               smog = sql("CASE WHEN sent_count > 0
                THEN 1.043 * sqrt(30 * multisyl_count / sent_count) + 3.1291 END")) %>%
        compute(indexes=c("file_name", "last_update", "category"),
                name="other_measures", temporary=FALSE)
})

dbGetQuery(pg, "ALTER TABLE other_measures OWNER TO bgt")
dbGetQuery(pg, "ALTER TABLE other_measures SET SCHEMA bgt")

dbDisconnect(pg)
