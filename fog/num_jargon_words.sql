SET work_mem='18GB';

DROP TABLE IF EXISTS bgt.jargon_words;

CREATE TABLE bgt.jargon_words AS
WITH
latest_calls AS (
    SELECT file_name, max(last_update) AS last_update
    FROM streetevents.calls
    GROUP BY file_name),

link_table AS (
    SELECT a.file_name,
        min(floor(c.siccd/100))::int AS sic2
    FROM streetevents.crsp_link AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    INNER JOIN crsp.stocknames AS c
    ON a.permno=c.permno AND b.start_date >= c.namedt
        AND (b.start_date <= c.nameenddt OR c.nameenddt IS NULL)
    GROUP BY a.file_name)
SELECT file_name, last_update, category,
    array_length(array_overlap(b.long_words,
    top_words(d.long_words, 100)), 1) AS num_jargon_words
FROM bgt.long_words AS b
INNER JOIN latest_calls
USING (file_name, last_update)
INNER JOIN link_table AS c
USING (file_name)
INNER JOIN bgt.word_freq AS d
USING (sic2);

SET maintenance_work_mem='2GB';

CREATE INDEX ON bgt.jargon_words (file_name, category);
