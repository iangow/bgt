DROP  FUNCTION IF EXISTS elf(sentences text[]);
 CREATE OR REPLACE FUNCTION elf(sentences text[])
  RETURNS integer[] AS
$BODY$
    if 'nsyl' in SD:
        nsyl = SD['nsyl']
        re = SD['re']
        nltk = SD['nltk']
        dic = SD['dic']
    else:
        import re, nltk
        	
        from nltk.corpus import cmudict 
        dic = cmudict.dict()

        def nsyl(word):  
            if word in dic:
                prons = dic[word]
                num_syls = [len([syl for syl in pron if re.findall('[0-9]', syl)]) for pron in prons]
                return max(num_syls)
            else:
                return 1

        SD['nsyl'] = nsyl 
        SD['re'] = re
        SD['nltk'] = nltk
        SD['dic'] = dic

    def elf(sentence):
        words = [word.lower() 
                    for word in nltk.word_tokenize(sentence) if re.findall('[a-zA-Z]', word)]    
        if len(words)>0:
            return sum([nsyl(word) for word in words]) - len(words)  
        else:
            return 0     
            
    return [elf(sent) for sent in sentences]

$BODY$
  LANGUAGE plpythonu VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION array_sum(an_array integer[])
  RETURNS integer AS
$BODY$
    if an_array is None:
        return None
    return sum(an_array)
$BODY$
  LANGUAGE plpythonu VOLATILE
  COST 100;

DROP FUNCTION IF EXISTS array_avg(integer[]);

  CREATE OR REPLACE FUNCTION array_avg(an_array integer[])
  RETURNS float8 AS
$BODY$
    if 'mean' in SD:
        mean = SD['mean']
    else:
        from numpy import mean
        SD['mean'] = mean
        
    if an_array is None:
        return None
    return mean(an_array)
$BODY$
  LANGUAGE plpythonu VOLATILE
  COST 100;

