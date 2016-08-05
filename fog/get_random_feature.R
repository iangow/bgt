library(dplyr)
library(RPostgreSQL)
pg <- src_postgres()

long_words <- tbl(pg, sql("SELECT * FROM bgt.long_words"))

set.seed(2016)
n_letters <- 6
the_letters <- sample(letters, n_letters, replace = FALSE)
the_regex <- paste0("^[", paste(the_letters, collapse=""), "]")

dbGetQuery(pg$con, "
CREATE OR REPLACE FUNCTION regex_count(text[], text)
RETURNS bigint AS
$CODE$
    WITH words AS (
        SELECT UNNEST($1) AS word)
    SELECT COUNT(*)
    FROM words
    WHERE word ~ $2
$CODE$ LANGUAGE sql IMMUTABLE STRICT")


dbGetQuery(pg$con, "SET work_mem='3GB'")

RPostgreSQL::dbGetQuery(pg$con, "DROP TABLE IF EXISTS bgt.random_feature")

random_feature <-
    long_words %>%
    mutate(match_count=regex_count(long_words, the_regex),
           word_count=array_length(long_words, 1L)) %>%
    mutate(match_prop=match_count * 1.0 / word_count) %>%
    select(-long_words, -match_count, -word_count) %>%
    compute(name="random_feature", temporary=FALSE)

RPostgreSQL::dbGetQuery(pg$con, "ALTER TABLE random_measure SET SCHEMA bgt")

random_feature <- tbl(pg, sql("SELECT * FROM bgt.random_measure"))

random_feature %>%
    filter(category=="anal_qa")

