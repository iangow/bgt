# Get fog data ----
library(dplyr)
library(tidyr)
pg <- src_postgres()

tbl_pg <- function(table) {
    tbl(pg, sql(paste0("SELECT * FROM bgt.", table)))
}

fl_data <- tbl_pg("fl_data")
sent_counts <- tbl_pg("sent_counts")
tone_data <- tbl_pg("tone_data") %>% select(file_name, last_update, category, everything())
long_words <- tbl_pg("long_words")
other_measures <- tbl_pg("other_measures")
within_call_data <- tbl_pg("within_call_data")
ticker_match <- tbl_pg("ticker_match")

calls <-
    tbl(pg, sql("SELECT * FROM streetevents.calls"))

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update=max(last_update)) %>%
    inner_join(calls) %>%
    select(file_name, last_update, call_date)

fog <-
    tbl_pg("fog") %>%
    mutate(num_complex_words = percent_complex / 100 * num_words)

fog_jargon_sql <-
    sql("CASE WHEN num_complex_words > 0
        THEN 0.4*percent_complex*num_jargon_words/num_complex_words END")
fog_special_sql <-
    sql("CASE WHEN num_complex_words > 0
        THEN 0.4*percent_complex*(1-num_jargon_words/num_complex_words) END")
fog_words_sent_correct_sql <-
    sql("CASE WHEN num_sentences > 0
        THEN 0.4*num_words/num_sentences END")
fog_words_sent_error_sql <-
    sql("CASE WHEN num_sentences > 0 AND num_sentences_original > 0
        THEN 0.4*(num_words/num_sentences-num_words/num_sentences_original) END")

fog_decomposed <-
    tbl_pg("jargon_words") %>%
    group_by(file_name, category) %>%
    summarize(num_jargon_words=sql("max(num_jargon_words)::float8")) %>%
    inner_join(fog %>% select(-num_sentences)) %>%
    inner_join(sent_counts) %>%
    inner_join(other_measures) %>%
    mutate(num_words=sql("num_words::float8")) %>%
    mutate(fog_jargon=fog_jargon_sql,
           fog_special=fog_special_sql,
           fog_words_sent_correct=fog_words_sent_correct_sql,
           fog_words_sent_error=fog_words_sent_error_sql) %>%
    compute()

call_level_tone_data <-
    tone_data %>%
    select(-category) %>%
    group_by(file_name) %>%
    summarize_each(funs(sum))

fog.data <-
    fl_data %>%
    inner_join(sent_counts) %>%
    rename(num_sentences_fl=num_sentences) %>%
    inner_join(tone_data) %>%
    inner_join(fog_decomposed) %>%
    distinct() %>%
    collect(n=Inf)

# Recast data so that three rows expand into 3*K columns
cast.data <-
    fog.data %>%
    gather(key, value, -file_name, -last_update, -category) %>%
    unite(var, c(key, category), sep="_") %>%
    spread(var, value) %>%
    tbl_df() %>%
    select(-ends_with("anal_pres")) %>%
    inner_join(call_level_tone_data %>%
                   inner_join(within_call_data) %>%
                   select(-last_update) %>%
                   collect(n=Inf))

fog.data %>%
    mutate(same_sent_count = num_sentences_fl==num_sentences_original) %>%
    group_by(same_sent_count) %>%
    summarize(count=n())

# Send data to database ----
rs <-
    RPostgreSQL::dbWriteTable(pg$con, c("bgt", "fog_recast"), cast.data,
                   overwrite=TRUE, row.names=FALSE)

rs <-
    RPostgreSQL::dbGetQuery(pg$con, "
        SET maintenance_work_mem='5GB';
        CREATE INDEX ON bgt.fog_recast (file_name)")

fog_recast <- tbl_pg("fog_recast")

merged.fog.data <-
    tbl(pg, sql("
    SELECT DISTINCT *,
        0.4 * (100*(num_complex_words_comp_qa+num_complex_words_anal_qa)/
        (num_words_comp_qa+num_words_anal_qa) +
        (num_words_comp_qa+num_words_anal_qa)/
            (num_sentences_comp_qa+num_sentences_anal_qa)) AS fog_qa
    FROM bgt.ticker_match
    INNER JOIN bgt.fog_recast
    USING (file_name)"))

merged.fog.data
merged.fog.data %>% summarize(n())

merged.fog.data %>%
    mutate(year=sql("extract(year from datadate)")) %>%
    group_by(year) %>%
    summarize(n()) %>%
    arrange(year) %>%
    print(n=100)

long_words %>%
    inner_join(latest_calls) %>%
    mutate(year=sql("extract(year from call_date)")) %>%
    group_by(year) %>%
    summarize(n()) %>%
    arrange(year) %>%
    print(n=100)

