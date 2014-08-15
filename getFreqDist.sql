DROP FUNCTION getFreqDist(text[]);

CREATE OR REPLACE FUNCTION getFreqDist(words text[]) RETURNS jsonb AS 
$CODE$
    if 'nsyl' in SD:
        re = SD['re']
        nltk = SD['nltk']
        FreqDist = SD['FreqDist']
        json = SD['json']
    else:
        import json, re, nltk
        from nltk import FreqDist

        SD['json'] = json
        SD['re'] = re
        SD['nltk'] = nltk
        SD['FreqDist'] = FreqDist

    freq_dist = FreqDist(words)

    return json.dumps(freq_dist)
$CODE$ LANGUAGE plpythonu;
