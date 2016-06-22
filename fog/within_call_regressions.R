
# Create a table to store the data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("bgt", "within_call_data"))) {
    rs <- dbGetQuery(pg, "
        CREATE TABLE bgt.within_call_data
            (file_name text, last_update timestamp without time zone,
             r_squared float8, num_obs integer,
             constant float8,
             slope float8, mean_analyst_fog float8,
             mean_manager_fog float8);

        CREATE INDEX ON bgt.within_call_data (file_name)")
}

rs <- dbDisconnect(pg)

# Make a function to run regressions ----
get_fog_reg_data <- function(file_name) {
    # Function to get statistics for within-call regressions
    # of fog of answers on fog of questions.

    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())

    # Get fog data
    reg_data <- dbGetQuery(pg, paste0("
        DELETE FROM bgt.within_call_data
        WHERE file_name='", file_name, "';

        WITH
        latest_call AS (
            SELECT file_name, max(last_update) AS last_update
            FROM streetevents.calls
            WHERE file_name='", file_name, "'
            GROUP BY file_name),

        question_nums AS (
            SELECT file_name, last_update, question_nums,
                UNNEST(question_nums) AS speaker_number
            FROM streetevents.qa_pairs
            INNER JOIN latest_call
            USING (file_name, last_update)),

        questions AS (
            SELECT file_name, last_update, question_nums,
                string_agg(speaker_text, ' ') AS questions
            FROM streetevents.speaker_data
            INNER JOIN question_nums
            USING (file_name, last_update, speaker_number)
            GROUP BY file_name, last_update, question_nums),

        answer_nums AS (
            SELECT file_name, last_update, question_nums,
                UNNEST(answer_nums) AS speaker_number
            FROM streetevents.qa_pairs
            INNER JOIN latest_call
            USING (file_name, last_update)),

        answers AS (
            SELECT file_name, last_update, question_nums,
                string_agg(speaker_text, ' ') AS answers
            FROM streetevents.speaker_data
            INNER JOIN answer_nums
            USING (file_name, last_update, speaker_number)
            GROUP BY file_name, last_update, question_nums)

        SELECT file_name, last_update,
            fog(questions) AS fog_questions,
            fog(answers) AS fog_answers
        FROM answers
        INNER JOIN questions
        USING (file_name, last_update, question_nums)"))

    # Exit if there are no data.
    if (dim(reg_data)[1]==0) {
        dbDisconnect(pg)
        # print("No data")
        return(NA)
    }

    # Run regression and collate statistics
    fitted.model <- lm(fog_answers ~ fog_questions, data=reg_data)
    summ <- summary(fitted.model)

    # Exit if the regression didn't have enough data
    if (dim(summ$coefficients)[1]<2) {
        dbDisconnect(pg)
        #  print("Insufficient data")
        return(NA)
    }

    # Organize regression statistics
    results <-
        data.frame(
            file_name=reg_data$file_name[1],
            last_update=reg_data$last_update[1],
            r_squared=summ$r.squared,
            num_obs=sum(summ$df[1:2]),
            constant=summ$coefficients[1,1],
            slope=summ$coefficients[2,1],
            mean_analyst_fog=mean(reg_data$fog_questions),
            mean_manager_fog=mean(reg_data$fog_answers),
        stringsAsFactors=FALSE)

    # Push to database.
    dbWriteTable(pg, c("bgt", "within_call_data"), results,
                 append=TRUE, row.names=FALSE)

    dbDisconnect(pg)
}
rs <- dbDisconnect(pg)

# Get list of files to process ----

# Get a list of file names for which we need to get
# within-call regression data
pg <- dbConnect(PostgreSQL())

file_names <-  dbGetQuery(pg, "
    SELECT file_name, max(last_update) AS last_update
    FROM streetevents.calls
    WHERE call_type=1
    GROUP BY file_name
    EXCEPT
    SELECT file_name, last_update
    FROM bgt.within_call_data")
rs <- dbDisconnect(pg)

# Apply function to get within-call regression data ----
# Run on 8 cores.
library(parallel)
system.time(temp <- mclapply(file_names$file_name, get_fog_reg_data,
                             mc.cores=8))
