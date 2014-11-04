SET work_mem='18GB';

DROP TABLE IF EXISTS bgt.fog_decomposed;

CREATE TABLE bgt.fog_decomposed AS
WITH jargon_words AS (
    SELECT file_name, category, max(num_jargon_words) AS num_jargon_words 
    FROM bgt.jargon_words
    GROUP BY file_name, category),

raw_data AS (
    SELECT a.*, 
        (percent_complex/100 * num_words)::integer AS num_complex_words,
        b.num_jargon_words, c.num_sentences AS num_sentences_alt
    FROM bgt.fog AS a
    LEFT JOIN jargon_words AS b
    USING (file_name, category)
    LEFT JOIN bgt.sent_counts AS c
    USING (file_name, category))

SELECT *, 
    CASE WHEN num_complex_words > 0 
        THEN 0.4*percent_complex*num_jargon_words/num_complex_words END AS fog_jargon,
    CASE WHEN num_complex_words > 0 
        THEN 0.4*percent_complex*(1-num_jargon_words/num_complex_words::float8) END AS fog_special,
    CASE WHEN num_sentences_alt > 0 
        THEN 0.4*num_words/num_sentences_alt END AS fog_words_sent_correct,
    CASE WHEN num_sentences_alt > 0 AND num_sentences > 0 
        THEN 0.4*(num_words::float8/num_sentences-num_words::float8/num_sentences_alt) END AS fog_words_sent_error
FROM raw_data
