CREATE OR REPLACE FUNCTION regex_count(text[], text)
    RETURNS bigint AS
    $CODE$
        WITH words AS (
            SELECT UNNEST($1) AS word)
        SELECT COUNT(*)
        FROM words
        WHERE word ~ $2
    $CODE$ LANGUAGE sql IMMUTABLE STRICT;
