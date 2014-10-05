CREATE OR REPLACE FUNCTION counter_add(json_orig jsonb, text_inc text[])
  RETURNS jsonb AS
$BODY$
    import json
    from collections import Counter
    
    orig = Counter(json.loads(json_orig))
    inc = Counter(text_inc)

    return json.dumps(orig + inc)
$BODY$
  LANGUAGE plpythonu VOLATILE
  COST 100;

CREATE AGGREGATE count_agg(text[]) (
  SFUNC=counter_add,
  STYPE=jsonb,
  INITCOND='{"a":0}'
);