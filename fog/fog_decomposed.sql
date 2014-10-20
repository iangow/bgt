SET work_mem='18GB';

DROP TABLE IF EXISTS bgt.fog_decomposed;

CREATE TABLE bgt.fog_decomposed AS
WITH link_table AS (
    SELECT a.file_name, a.permno, b.call_date::date, siccd,
        floor(c.siccd/100)::int AS sic2
    FROM streetevents.crsp_link AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    INNER JOIN crsp.stocknames AS c
    ON a.permno=c.permno AND b.call_date >= c.namedt 
        AND (b.call_date <= c.nameenddt OR c.nameenddt IS NULL)),
raw_data AS (
    SELECT a.*, 
        (percent_complex/100 * num_words)::integer AS num_complex_words,
        array_length(array_overlap(b.long_words, top_words(d.long_words, 100)), 1) AS num_jargon_words
    FROM bgt.fog AS a
    INNER JOIN bgt.long_words AS b
    USING (file_name, category)
    INNER JOIN link_table AS c
    USING (file_name)
    INNER JOIN bgt.word_freq AS d
    USING (sic2))
SELECT *, 
    CASE WHEN num_complex_words>0 
        THEN 0.4*percent_complex*num_jargon_words/num_complex_words END AS fog_jargon,
    CASE WHEN num_complex_words>0 
        THEN 0.4*percent_complex*(1-num_jargon_words/num_complex_words::float8) END AS fog_special,
    CASE WHEN num_sentences>0 
        THEN 0.4*num_words/num_sentences END AS fog_words_sent
FROM raw_data