# Get fog data ----
library(dplyr)
library(tidyr)
pg <- src_postgres()

tbl_pg <- function(table) {
    tbl(pg, sql(paste0("SELECT * FROM bgt.", table)))
}
random_feature <- tbl_pg("random_feature")
fl_data <- tbl_pg("fl_data")
sent_counts <- tbl_pg("sent_counts")
tone_data <- tbl_pg("tone_data") %>% select(file_name, last_update, category, everything())
long_words <- tbl_pg("long_words")
other_measures <- tbl_pg("other_measures")
within_call_data <- tbl_pg("within_call_data")

ticker_match <- tbl(pg, sql("SELECT * FROM streetevents.ticker_match"))
call_files <- tbl(pg, sql("SELECT * FROM streetevents.call_files"))
calls <- tbl(pg, sql("SELECT * FROM streetevents.calls"))

file_size <-
    calls %>%
    inner_join(call_files) %>%
    group_by(file_name, last_update) %>%
    summarize(file_size=max(file_size))

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update=max(last_update)) %>%
    inner_join(calls) %>%
    select(file_name, last_update, call_date)

fog <-
    tbl_pg("fog") %>%
    distinct() %>%
    mutate(num_complex_words = percent_complex / 100 * num_words) %>%
    compute(indexes=c("file_name", "last_update", "category"))

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

jargon_words <-
    tbl_pg("jargon_words") %>%
    group_by(file_name, last_update, category) %>%
    summarize(num_jargon_words=sql("max(num_jargon_words)::float8")) %>%
    ungroup() %>%
    compute(indexes=c("file_name", "last_update", "category"))

cast_df <- function(df) {
    # Recast data so that three rows expand into 3*K columns
    df %>%
    mutate(last_update=sql("last_update::text")) %>%
    collect(n=Inf) %>%
    gather(key, value, -file_name,  -last_update, -category) %>%
    unite(var, c(key, category), sep="_") %>%
    spread(var, value) %>%
    tbl_df() %>%
    select(-ends_with("anal_pres"))
}

fog_decomposed <-
    jargon_words %>%
    inner_join(fog) %>%
    # inner_join(sent_counts) %>%
    mutate(num_words=sql("num_words::float8")) %>%
    mutate(fog_jargon=fog_jargon_sql,
           fog_special=fog_special_sql,
           fog_words_sent_correct=fog_words_sent_correct_sql,
           fog_words_sent_error=fog_words_sent_error_sql) %>%
    compute()

other_measures_cast <-
    other_measures %>%
    select(file_name, last_update, context, fk, lix, rix, ari, smog) %>%
    mutate(category=sql("'anal_' || context")) %>%
    select(-context) %>%
    cast_df()

call_level_tone_data <-
    tone_data %>%
    select(-category) %>%
    group_by(file_name, last_update) %>%
    summarize_each(funs(sum))

call_level_fl_data <-
    fl_data %>%
    mutate(num_fl_sents=prop_fl_sents*num_sentences) %>%
    group_by(file_name, last_update) %>%
    summarize(num_sentences=sum(num_sentences),
              num_fl_sents=sum(num_fl_sents)) %>%
    mutate(prop_fl_sents=num_fl_sents/num_sentences) %>%
    select(-num_sentences, -num_fl_sents)

collect_fix <- function(df) {
    df %>%
        mutate(last_update=sql("last_update::text")) %>%
        collect(n=Inf)
}

fog.data <-
    other_measures_cast %>%
    left_join(cast_df(fog_decomposed)) %>%
    left_join(cast_df(fl_data)) %>%
    left_join(cast_df(tone_data)) %>%
    left_join(cast_df(random_feature)) %>%
    left_join(collect_fix(call_level_tone_data)) %>%
    left_join(collect_fix(within_call_data)) %>%
    left_join(collect_fix(call_level_fl_data)) %>%
    inner_join(collect_fix(file_size))

# TODO: Need to compare num_sentences measures.
# fog.data %>%
#     mutate(same_sent_count = num_sentences_fl==num_sentences_original) %>%
#     group_by(same_sent_count) %>%
#     summarize(count=n())

# Send data to database ----
rs <-
    RPostgreSQL::dbWriteTable(pg$con, c("bgt", "fog_recast"), fog.data,
                   overwrite=TRUE, row.names=FALSE)

rs <-
    RPostgreSQL::dbGetQuery(pg$con, "
        ALTER TABLE bgt.fog_recast ALTER last_update TYPE timestamp without time zone USING last_update::timestamp without time zone")

rs <-
    RPostgreSQL::dbGetQuery(pg$con, "
        SET maintenance_work_mem='5GB';
        CREATE INDEX ON bgt.fog_recast (file_name, last_update)")

fog_recast <- tbl_pg("fog_recast")

merged.fog.data <-
    ticker_match %>%
    inner_join(fog_recast) %>%
    mutate(fog_qa= 0.4 * (100*(num_complex_words_comp_qa+num_complex_words_anal_qa)/
        (num_words_comp_qa+num_words_anal_qa) +
        (num_words_comp_qa+num_words_anal_qa)*1.0/
            (num_sentences_comp_qa+num_sentences_anal_qa)) )

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
