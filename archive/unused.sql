-- 
-- SET work_mem='10GB';
-- 
-- WITH raw AS (
--   SELECT speaker_text
--   FROM streetevents.speaker_data
--   LIMIT 10000)
-- SELECT top_words(count_agg(word_counts(speaker_text)), 40)
-- FROM raw;
-- 
-- WITH raw AS (
--   SELECT context, speaker_text
--   FROM streetevents.speaker_data
--   LIMIT 10000)
-- SELECT context, top_words(word_counts(string_agg(speaker_text, ' ')), 40)
-- FROM raw
-- GROUP BY context;

-- DROP FUNCTION tokenize_bytea(text);
-- 
-- CREATE OR REPLACE FUNCTION tokenize_bytea(raw text)
-- RETURNS bytea AS
-- $CODE$
--     import nltk, marshal
--     from collections import Counter
--     tokens = nltk.word_tokenize(raw.lower().decode('utf-8'))
--     the_dict = dict(Counter(tokens))
--     return marshal.dumps(the_dict)
-- $CODE$ LANGUAGE plpythonu;

-- SELECT tokenize_bytea('This is some text. The word is appears twice.')



-- CREATE OR REPLACE FUNCTION counter_add(orig bytea, inc bytea)
-- RETURNS bytea AS
-- $$
--     from collections import Counter
--     import marshal
--     
--     orig_c = Counter(marshal.loads(orig))
--     inc_c = Counter(marshal.loads(inc))
-- 
--     return marshal.dumps(dict(orig_c + inc_c))
-- $$ LANGUAGE plpythonu;


-- DROP AGGREGATE combine_dicts(bytea) ;
-- 
-- CREATE AGGREGATE combine_dicts(bytea) (
--   SFUNC=counter_add_func,
--   STYPE=bytea,
--   INITCOND = '{0'
-- );



-- CREATE OR REPLACE FUNCTION top_words(freq_dist bytea, num integer)
-- RETURNS text[] AS
-- $$
--     import json, marshal
--     from collections import Counter
--     return [Counter(marshal.loads(freq_dist)).most_common(num)]
-- $$ LANGUAGE plpythonu;
-- 
-- SELECT json_add('{"a":1,"b":2}'::json, '{"a":3,"b":2, "c":3}'::json)




-- 
-- WITH raw AS (
--   SELECT speaker_text, tokenize_bytea(speaker_text)
--   FROM streetevents.speaker_data
--   LIMIT 1000)
-- SELECT top_words(combine_dicts(tokenize_bytea), 40)
-- FROM raw;

-- CREATE OR REPLACE FUNCTION counter_add(json_orig json, json_inc json)
-- RETURNS json AS
-- $$
--     import json
--     from collections import Counter
--     
--     orig = Counter(json.loads(json_orig))
--     inc = Counter(json.loads(json_inc))
-- 
--     return json.dumps(orig + inc)
-- $$ LANGUAGE plpythonu;
-- 
-- 
-- DROP AGGREGATE count_agg(json) ;
-- 
-- CREATE AGGREGATE count_agg(json) (
--   SFUNC=counter_add,
--   STYPE=json,
--   INITCOND = '{"a":0}'
-- );

-- SELECT '{"a":1,"b":2}'::json

-- CREATE OR REPLACE FUNCTION json_value(the_json json, key text)
-- RETURNS int AS
-- $CODE$
--     import json
--     a_dict = json.loads(the_json)
--     return a_dict[key]
-- $CODE$ LANGUAGE plpythonu;
