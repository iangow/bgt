WITH link_table AS (
    SELECT a.file_name, a.permno, b.call_date::date, siccd,
        floor(c.siccd/100)::int AS sic2
    FROM streetevents.crsp_link AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    INNER JOIN crsp.stocknames AS c
    ON a.permno=c.permno AND b.call_date >= c.namedt 
        AND (b.call_date <= c.nameenddt OR c.nameenddt IS NULL)
    LIMIT 100),
long_words AS (
    SELECT sic2, file_name, getLongWords(string_agg(speaker_text, ' ')) AS long_words
    FROM streetevents.speaker_data
    INNER JOIN link_table
    USING (file_name)  
    GROUP BY sic2, file_name)
SELECT * -- sic2, getFreqDist(array_agg(unnest(long_words))) AS words
FROM long_words
-- GROUP BY sic2   
