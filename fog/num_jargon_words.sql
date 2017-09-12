SET work_mem='18GB';

DROP TABLE IF EXISTS bgt.jargon_words;

CREATE TABLE bgt.jargon_words AS
WITH

SELECT file_name, last_update, category,
    array_length(array_overlap(b.long_words,
    top_words(d.long_words, 100)), 1) AS num_jargon_words
FROM bgt.long_words AS b
INNER JOIN bgt.sics AS c
USING (file_name)
INNER JOIN bgt.word_freq AS d
USING (sic2);

SET maintenance_work_mem='2GB';

CREATE INDEX ON bgt.jargon_words (file_name, category);
