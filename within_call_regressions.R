
# Create a table to store the data ----
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
    
rs <- dbGetQuery(pg, "
    DROP TABLE IF EXISTS bgt.within_call_data;

    CREATE TABLE bgt.within_call_data 
        (file_name text, r_squared float8, num_obs integer,
         constant float8,
         slope float8, mean_analyst_fog float8,
         mean_manager_fog float8)")

rs <- dbDisconnect(pg)

# Make a function to run regressions ----
get_fog_reg_data <- function(file_name) {
    # Function to get statistics for within-call regressions
    # of fog of answers on fog of questions.
    
    library(RPostgreSQL)
    pg <- dbConnect(PostgreSQL())
    
    # Get fog data
    reg_data <- dbGetQuery(pg, paste0("
        SELECT file_name, 
            fog(array_to_string(questions, ' ')) AS fog_questions,
            fog(array_to_string(answers, ' ')) AS fog_answers
        FROM streetevents.qa_pairs
        WHERE file_name='", file_name, "'"))

    # Exit if there are no data.
    if (dim(reg_data)[1]==0) {
        dbDisconnect(pg)
        return(NA)
    }
    
    # Run regression and collate statistics
    fitted.model <- lm(fog_answers ~ fog_questions, data=reg_data)
    summ <- summary(fitted.model)
    
    # Exit if the regression didn't have enough data
    if (dim(summ$coefficients)[1]<2) {
        dbDisconnect(pg)
        return(NA)
    }
    
    # Organize regression statistics
    results <- 
        data.frame(
            file_name=file_name, 
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

# Get list of files and run regressions ------

pg <- dbConnect(PostgreSQL())

# Get a list of file names for which we need to get tone data.
file_names <-  dbGetQuery(pg, "
    SELECT DISTINCT file_name
    FROM streetevents.calls
    WHERE call_type=1 AND file_name NOT IN (SELECT file_name FROM bgt.within_call_data)")

# Apply function to get tone data. Run on 12 cores.
library(parallel)
# system.time(temp <- lapply(file_names$file_name[1271:3265], get_fog_reg_data))
system.time(temp <- mclapply(file_names$file_name, get_fog_reg_data, mc.cores=8))
rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='1GB';
    CREATE INDEX ON bgt.within_call_data (file_name)")
rs <- dbDisconnect(pg)
