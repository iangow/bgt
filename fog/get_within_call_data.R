library(dplyr)

# Create a table to store the data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "within_call_data"))) {

    rs <- dbGetQuery(pg, "

        CREATE TABLE bgt.within_call_data
            (file_name text,
             last_update timestamp without time zone,
             r_squared float8, num_obs integer,
             constant float8,
             slope float8, mean_analyst_fog float8,
             mean_manager_fog float8)")

    rs <- dbGetQuery(pg, "CREATE INDEX ON bgt.within_call_data (file_name)")
}

rs <- dbDisconnect(pg)

# Make a function to run regressions ----
get_fog_reg_data <- function(file_name) {

    # Function to get statistics for within-call regressions
    # of fog of answers on fog of questions.

    file_name_str <- file_name

    pg <- src_postgres()

    latest_update <-
        tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
        filter(file_name==file_name_str) %>%
        group_by(file_name) %>%
        summarize(last_update=max(last_update))

    empty_res <-
            latest_update %>%
            mutate(r_squared=sql("NULL::float8"),
                   num_obs=sql("NULL::float8"),
                   constant=sql("NULL::float8"),
                   slope=sql("NULL::float8"),
                   mean_analyst_fog=sql("NULL::float8"),
                   mean_manager_fog=sql("NULL::float8")) %>%
            as.data.frame()

    fog_speaker <-
        tbl(pg, sql("SELECT * FROM bgt.fog_speaker")) %>%
        filter(context=='qa') %>%
        inner_join(latest_update) %>%
        filter(file_name==file_name_str)

    nrows <- fog_speaker %>% summarize(n()) %>% collect() %>% .[[1]]

    if (nrows != 0) {
        questions <-
            tbl(pg, sql("SELECT * FROM streetevents.qa_pairs")) %>%
            mutate(question_number=unnest(question_nums),
                   answer_number=unnest(answer_nums)) %>%
            select(file_name, last_update, question_nums, question_number, answer_number)

        fog_questions <-
            questions %>%
            filter(file_name==file_name_str) %>%
            inner_join(fog_speaker %>%
                           rename(question_number=speaker_number)) %>%
            group_by(file_name, last_update, question_nums) %>%
            summarize(percent_complex = sum(percent_complex*num_words)/sum(num_words),
                   num_sentences = sum(num_sentences),
                   num_words = sum(num_words)) %>%
            mutate(fog_questions = 0.4 * (percent_complex + num_words/num_sentences)) %>%
            select(file_name, last_update, question_nums, fog_questions)

        fog_answers <-
            questions %>%
            filter(file_name==file_name_str) %>%
            inner_join(fog_speaker %>%
                           rename(answer_number=speaker_number)) %>%
            group_by(file_name, last_update, question_nums) %>%
            summarize(percent_complex = sum(percent_complex*num_words)/sum(num_words),
                   num_sentences = sum(num_sentences),
                   num_words = sum(num_words)) %>%
            mutate(fog_answers = 0.4 * (percent_complex + num_words/num_sentences)) %>%
            select(file_name, last_update, question_nums, fog_answers)


        reg_data <-
            fog_questions %>%
            inner_join(fog_answers) %>%
            group_by(file_name, last_update) %>%
            rename(y=fog_answers, x=fog_questions)

        reg_results <-
            reg_data %>%
            summarize(r_squared=regr_r2(y, x),
                      num_obs=regr_count(y, x),
                      constant=regr_intercept(y, x),
                      slope=regr_slope(y, x),
                      mean_analyst_fog=regr_avgx(y, x),
                      mean_manager_fog=regr_avgy(y, x)) %>%
            collect() %>%
            as.data.frame()
    } else {
        reg_results <- empty_res
    }

    # Push to database.
    dbWriteTable(pg$con, c("bgt", "within_call_data"), reg_results,
                 append=TRUE, row.names=FALSE)
    dbDisconnect(pg$con)

    return(TRUE)
}

# Get list of files and run regressions ------

pg <- src_postgres()

# Get a list of file names for which we need to get within-call data.
latest_update <-
    tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
    filter(call_type==1L) %>%
    group_by(file_name) %>%
    summarize(last_update=max(last_update))

qa_pairs <-
    tbl(pg, sql("SELECT * FROM streetevents.qa_pairs"))

within_call_data <-
    tbl(pg, sql("SELECT * FROM bgt.within_call_data"))

file_names <-
    qa_pairs %>%
    select(file_name, last_update) %>%
    inner_join(latest_update) %>%
    anti_join(within_call_data %>% select(file_name, last_update)) %>%
    distinct() %>%
    collect(n=Inf)

dbDisconnect(pg$con)

# Apply function to get tone data. Run on several cores. ----
library(parallel)
# system.time(temp <- lapply(file_names$file_name, get_fog_reg_data))
system.time(temp <- mclapply(file_names$file_name, get_fog_reg_data, mc.cores=8))
