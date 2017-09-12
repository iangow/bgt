# Get fog data ----
library(dplyr, warn.conflicts = FALSE)
library(tidyr)
library(RPostgreSQL)
pg <- src_postgres()

tbl_pg <- function(table) {
    df <- tbl(pg, sql(paste0("SELECT * FROM bgt.", table)))
    assign(table, df, envir = globalenv())
}

base_tables <- c("fog", "random_feature", "fl_data", "sent_counts",
                 "tone_data", "other_measures", "within_call_data",
                 "fog_by_half", "jargon_words")

summ_stat <- function(table_name) {
    get(table_name) %>%
        select(file_name) %>%
        distinct() %>%
        inner_join(calls) %>%
        mutate(year = date_part('year', start_date)) %>%
        group_by(year) %>%
        summarize(count = n()) %>%
        arrange(year) %>%
        collect() %>%
        rename_(.dots = setNames("count", table_name))
}

fog <- tbl_pg("fog")  #
random_feature <- tbl_pg("random_feature") #
fl_data <- tbl_pg("fl_data") #
sent_counts <- tbl_pg("sent_counts") #
tone_data <- tbl_pg("tone_data") #
other_measures <- tbl_pg("other_measures") #
within_call_data <- tbl_pg("within_call_data") #
fog_by_half <- tbl_pg("fog_by_half") #
jargon_words <- tbl_pg("jargon_words") #

call_files <- tbl(pg, sql("SELECT * FROM streetevents.call_files"))
calls <- tbl(pg, sql("SELECT * FROM streetevents.calls"))

rs <- lapply(base_tables, tbl_pg)
sum_stats <- Reduce(left_join, lapply(base_tables, summ_stat))

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
    select(file_name, last_update, start_date)

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

jargon_words_agg <-
    jargon_words %>%
    group_by(file_name, last_update, category) %>%
    summarize(num_jargon_words=sql("max(num_jargon_words)::float8")) %>%
    ungroup() %>%
    compute(indexes=c("file_name", "last_update", "category"))

cast_df <- function(df) {
    # Recast data so that three rows expand into 3*K columns
    df %>%
        collect(n = Inf) %>%
        gather(key, value, -file_name, -last_update, -category) %>%
        unite(var, c(key, category), sep="_") %>%
        spread(var, value) %>%
        tbl_df() %>%
        select(-ends_with("anal_pres"))
}

fog_decomposed <-
    fog %>%
    mutate(num_complex_words = percent_complex / 100 * num_words) %>%
    compute(indexes=c("file_name", "last_update", "category")) %>%
    left_join(jargon_words_agg) %>%
    # inner_join(sent_counts %>% rename(num_sentences_py = num_sentences)) %>%
    mutate(num_words=sql("num_words::float8")) %>%
    mutate(fog_jargon=fog_jargon_sql,
           fog_special=fog_special_sql,
           fog_words_sent_correct=fog_words_sent_correct_sql,
           fog_words_sent_error=fog_words_sent_error_sql) %>%
    compute()

other_measures_cast <-
    other_measures %>%
    select(file_name, last_update, category, fk, lix, rix, ari, smog) %>%
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

fog_early <-
    fog_by_half %>%
    filter(first_half) %>%
    select(-first_half) %>%
    rename(fog_early=fog)

fog_late <-
    fog_by_half %>%
    filter(!first_half) %>%
    select(-first_half) %>%
    rename(fog_late=fog)

fog_early_late <-
    fog_early %>%
    inner_join(fog_late)


fog.data <-
    other_measures_cast %>%
    left_join(cast_df(fog_decomposed)) %>%
    left_join(cast_df(fl_data %>% select(-num_sentences))) %>%
    left_join(cast_df(tone_data)) %>%
    left_join(cast_df(random_feature)) %>%
    left_join(cast_df(fog_early_late)) %>%
    left_join(collect(call_level_tone_data)) %>%
    left_join(collect(within_call_data)) %>%
    left_join(collect(call_level_fl_data)) %>%
    inner_join(collect(file_size))

# Send data to database ----
rs <- dbWriteTable(pg$con, c("bgt", "fog_recast"), fog.data,
                   overwrite=TRUE, row.names=FALSE)

rs <-dbGetQuery(pg$con, "
    SET maintenance_work_mem='5GB';
    CREATE INDEX ON bgt.fog_recast (file_name, last_update)")
