# 
library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
pg <- dbConnect(drv, dbname="crsp")
library(reshape)

dbGetQuery(pg, "
  -- Code for the ticker match
  SET work_mem='10GB';

  DROP TABLE IF EXISTS bgt.ticker_match;
  
  CREATE TABLE bgt.ticker_match AS
  WITH 
  linktable AS (
    SELECT gvkey, lpermno::integer AS permno, linkdt, linkenddt
    FROM crsp.ccmxpf_linktable 
    WHERE usedflag=1 AND linkprim IN ('C', 'P')),
  calls AS (
    -- Some tickers have *s in them, so I clean them out.
    -- Note that the call data is only from ~2001 through 2010
    -- so I condition on this below when merging with Compustat.
    -- Note that I just yesterday (2013-05-21) got call data
    -- through to 2013.
    SELECT regexp_replace(ticker, '[*]', '', 'g') AS ticker,
    file_name, co_name, call_desc, call_date::date
    FROM streetevents.calls),
  rdqs AS (
    -- Merge tickers from comp.secm with announcement dates
    -- from comp.fundq
    SELECT DISTINCT a.gvkey, b.tic, a.rdq, a.conm, a.datadate
    FROM comp.secm AS b
    RIGHT JOIN comp.fundq AS a
    ON a.gvkey=b.gvkey AND eomonth(b.datadate)=eomonth(a.rdq)),
  ticker_match AS (
    -- Match earnings announcements with calls within three days
    -- and with the same ticker
    SELECT a.*, b.gvkey, b.rdq, b.datadate
    FROM calls AS a
    INNER JOIN rdqs AS b
    ON a.ticker=b.tic AND 
    -- 2012-09-30 is the latest data on comp.secm. Assume that tickers after 
    -- that date are good.
    a.call_date BETWEEN b.rdq AND b.rdq + interval '3 days')
  -- Finally add PERMNO
  SELECT a.*, b.permno
  FROM ticker_match AS a
  LEFT JOIN linktable AS b
  ON a.gvkey=b.gvkey AND 
  a.datadate >= b.linkdt AND
  (a.datadate <= b.linkenddt OR b.linkenddt IS NULL);
  
  CREATE INDEX ON bgt.ticker_match (file_name);
  
  -- Code to aggregate speaker-level fog data
  -- by 'category' (analyst/company, presentation/Q&A)
  DROP TABLE IF EXISTS bgt.fog_aggregated;
  
  CREATE TABLE bgt.fog_aggregated AS
  WITH aggregated AS (
    SELECT file_name, category,
    sum(percent_complex_words) AS percent_complex_words,
    sum(fl_count) AS fl_count,
    sum(num_words) AS num_words, 
    sum(percent_complex_words/100*num_words) AS num_complex_words,
    sum(num_sentences) AS num_sentences
    FROM (
      SELECT *, (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context AS category
      FROM bgt.speakers
      WHERE speaker_name != 'Operator') AS b
    GROUP BY file_name, category
    HAVING sum(num_sentences) > 0 AND sum(num_words) > 0
    ORDER BY file_name, category)
  SELECT file_name, category, num_complex_words,  num_sentences, num_words,
   fl_count::float8/num_sentences AS prop_fl_sentences,
    (num_words::float8/num_sentences + 100*num_complex_words/num_words) *0.4 AS fog
  FROM aggregated;
  
  CREATE INDEX ON bgt.fog_aggregated (file_name);")

# Recast data so that three rows expand into 3*K columns
fog.data <- dbGetQuery(pg, "SELECT * FROM bgt.fog_aggregated")

alt.fog.data <- dbGetQuery(pg, "
    SELECT *, 0.4*(num_complex_words/num_sentences FROM bgt.fog_aggregated")


cast.data <- NULL
for (i in setdiff(names(fog.data), c("file_name", "category"))) {
  temp <- cast(fog.data, file_name ~ category, value=i)
  new.names <- paste(i, names(temp)[2:length(names(temp))], sep="_")
               names(temp) <- c("file_name", new.names)
  if (is.null(cast.data)) {
    cast.data <- temp
  } else {
    cast.data <- merge(cast.data, temp, all=TRUE)
  }
}

rs <- dbWriteTable(pg, c("bgt", "fog_recast"), cast.data, 
             overwrite=TRUE, row.names=FALSE)

rs <- dbGetQuery(pg, "
    SET maintenance_work_mem='5GB';
    CREATE INDEX ON bgt.fog_recast (file_name)")

merged.fog.data <- dbGetQuery(pg, "
  SELECT DISTINCT *, 
    0.4 * (100*(num_complex_words_comp_qa+num_complex_words_anal_qa)/
    (num_words_comp_qa+num_words_anal_qa) +
    (num_words_comp_qa+num_words_anal_qa)/(num_sentences_comp_qa+num_sentences_anal_qa)) AS fog_qa
  FROM bgt.ticker_match
  INNER JOIN bgt.fog_recast
  USING (file_name)")

library(foreign)

write.dta(merged.fog.data, file="~/Dropbox/research/BuGT/data/merged_fog_data.dta", version=11L)
head(merged.fog.data)
dim(merged.fog.data)
table(merged.fog.data$datadate)
