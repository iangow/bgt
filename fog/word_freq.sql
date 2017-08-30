DROP TABLE IF EXISTS bgt.word_freq;

SET work_mem='12GB';

CREATE TABLE bgt.word_freq AS
WITH

latest_calls AS (
    SELECT file_name, max(last_update) AS last_update
    FROM streetevents.calls
    GROUP BY file_name),

link_table AS (
    SELECT a.file_name, a.permno, b.start_date::date, siccd,
        floor(c.siccd/100)::int AS sic2
    FROM streetevents.crsp_link AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    INNER JOIN crsp.stocknames AS c
    ON a.permno=c.permno AND b.start_date >= c.namedt
        AND (b.start_date <= c.nameenddt OR c.nameenddt IS NULL))

SELECT sic2, count_agg(long_words) AS long_words
FROM bgt.long_words
INNER JOIN latest_calls
USING (file_name, last_update)
INNER JOIN link_table
USING (file_name)
GROUP BY sic2;
