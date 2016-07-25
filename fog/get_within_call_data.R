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

    speaker_data <-
        tbl(pg, sql("SELECT * FROM streetevents.speaker_data")) %>%
        filter(context=='qa') %>%
        inner_join(latest_update) %>%
        select(file_name, last_update, speaker_number, speaker_text) %>%
        filter(file_name==file_name_str)

    nrows <- speaker_data %>% summarize(n()) %>% collect() %>% .[[1]]

    if (nrows != 0) {
        questions <-
            tbl(pg, sql("SELECT * FROM streetevents.qa_pairs")) %>%
            mutate(question_number=unnest(question_nums),
                   answer_number=unnest(answer_nums)) %>%
            select(file_name, last_update, question_nums, question_number, answer_number)

        reg_data <-
            questions %>%
            filter(file_name==file_name_str) %>%
            inner_join(speaker_data %>%
                           rename(question_number=speaker_number,
                                  question=speaker_text)) %>%
            inner_join(speaker_data %>%
                           rename(answer_number=speaker_number,
                                  answer=speaker_text)) %>%
            group_by(file_name, last_update, question_nums) %>%
            summarize(fog_questions=fog(string_agg(question, ' ')),
                      fog_answers=fog(string_agg(answer, ' '))) %>%
            select(file_name, last_update, fog_answers, fog_questions) %>%
            compute()

        reg_results <-
            reg_data %>%
            group_by(file_name, last_update) %>%
            summarize(r_squared=regr_r2(fog_questions, fog_answers),
                      num_obs=regr_count(fog_questions, fog_answers),
                      constant=regr_intercept(fog_questions, fog_answers),
                      slope=regr_slope(fog_questions, fog_answers),
                      mean_analyst_fog=regr_avgx(fog_questions, fog_answers),
                      mean_manager_fog=regr_avgy(fog_questions, fog_answers)) %>%
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

# Get a list of file names for which we need to get tone data.


latest_update <-
    tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
    filter(call_type==1L) %>%
    group_by(file_name) %>%
    summarize(last_update=max(last_update))

qa_pairs <-
    tbl(pg, sql("SELECT * FROM streetevents.qa_pairs"))


within_call_data <-
    tbl(pg, sql("SELECT * FROM bgt.within_call_data")) %>%
    select(file_name, last_update)

file_names <-
    qa_pairs %>%
    select(file_name, last_update) %>%
    inner_join(latest_update) %>%
    anti_join(within_call_data) %>%
    collect(n=1000)

dbDisconnect(pg$con)

# Apply function to get tone data. Run on 12 cores.
library(parallel)
# system.time(temp <- lapply(file_names$file_name, get_fog_reg_data))
system.time(temp <- mclapply(file_names$file_name, get_fog_reg_data, mc.cores=8))
