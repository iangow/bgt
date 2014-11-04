SET work_mem='16GB';

-- Code to aggregate speaker-level fog data
-- by "category" (analyst/company, presentation/Q&A)
DROP TABLE IF EXISTS bgt.fog_aggregated;

CREATE TABLE bgt.fog_aggregated AS
WITH aggregated AS (
  SELECT file_name, category,
    sum(num_words) AS num_words, 
    sum(percent_complex_words/100*num_words) AS num_complex_words,
    sum(num_sentences) AS num_sentences
  FROM (
    SELECT *, (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context AS category
    FROM bgt.fog_speaker
    WHERE speaker_name != 'Operator') AS b
  GROUP BY file_name, category
  HAVING sum(num_sentences) > 0 AND sum(num_words) > 0
  ORDER BY file_name, category)
SELECT file_name, category,-- num_complex_words, 
        100*num_complex_words/num_words AS percent_complex,
        num_sentences, num_words,
    (num_words::float8/num_sentences + 100*num_complex_words/num_words) *0.4 AS fog
FROM aggregated;

CREATE INDEX ON bgt.fog_aggregated (file_name);


