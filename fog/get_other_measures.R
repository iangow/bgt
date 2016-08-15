Sys.setenv(PGHOST="iangow.me", PGDATABASE="crsp")
pg <- src_postgres()

RPostgreSQL::dbGetQuery(pg$con, "DROP TABLE IF EXISTS bgt.other_measures")
RPostgreSQL::dbGetQuery(pg$con, "SET work_mem='3GB'")
syllable_data <-
    tbl(pg, sql("SELECT * FROM streetevents.syllable_data"))

nchars <-
    tbl(pg, sql("SELECT * FROM bgt.nchars"))

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

system.time({
    processed <-
        syllable_data %>%
        inner_join(nchars) %>%
        mutate(monosyl_count=sql("COALESCE((syllable_data->'syllable_counts'->'1')::text::float8, 0)"),
               bisyl_count=sql("COALESCE((syllable_data->'syllable_counts'->'2')::text::float8,0)")) %>%
        mutate(sent_count=sql("(syllable_data->'sent_count')::text::float8"),
               word_count=sql("(syllable_data->'word_count')::text::float8"),
               word_7_count=sql("(syllable_data->'word_7_count')::text::float8"),
               num_syllables=sql("(syllable_data->'num_syllables')::text::float8")) %>%
        mutate(multisyl_count = word_count - monosyl_count - bisyl_count) %>%
        select(-syllable_data) %>%
        group_by(file_name, last_update, context) %>%
        summarize_each(funs(sum)) %>%
        mutate(fk = sql(fk_sql),
               lix = sql(lix_sql),
               rix = sql(rix_sql),
               ari = sql(ari_sql),
               smog = sql(smog_sql)) %>%
        compute(indexes=c("file_name", "last_update", "context"),
                name="other_measures", temporary=FALSE)
})

RPostgreSQL::dbGetQuery(pg$con, "ALTER TABLE other_measures SET SCHEMA bgt")
