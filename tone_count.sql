CREATE OR REPLACE FUNCTION tone_count(the_text text)  
RETURNS json AS
$CODE$
    if 're' in SD:
        re = SD['re']
        json = SD['json']
    else:
        import re, json
        SD['re'] = re
        SD['json'] = json

    if SD.has_key("regex_list"):
        regex_list = SD["regex_list"]
        categories = SD["categories"]
    else:

        rv = plpy.execute("SELECT category FROM bgt.lm_tone")
       
        categories = [ (r["category"]) for r in rv]

        # Implement Robin's suggestion to convert *s to regular expressions
        # outside the loop. And a
        plan = plpy.prepare("""
            SELECT word_list
            FROM bgt.lm_tone 
            WHERE category = $1""", ["text"])
        mod_word_list = {}
        for cat in categories:
            rows = list(plpy.cursor(plan, [cat]))
            word_list = rows[0]['word_list']
            mod_word_list[cat] = [word.lower() for word in word_list]

        # Pre-compile regular expressions.
        regex_list = {}
        for key in mod_word_list.keys():
            regex = '\\b(?:' + '|'.join(mod_word_list[key]) + ')\\b'
            regex_list[key] = re.compile(regex)
        SD["regex_list"] = regex_list
        SD["categories"] = categories

    # rest of function
    """Function to return number of matches against a LIWC category in a text"""
    text = re.sub(u'\u2019', "'", the_text).lower()
    the_dict = {category: len(re.findall(regex_list[category], text)) for category in categories}
    return json.dumps(the_dict)
    
$CODE$ LANGUAGE plpythonu;