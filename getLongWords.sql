CREATE OR REPLACE FUNCTION getLongWords(the_text text) RETURNS text[] AS 
$CODE$
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
                pronunciations = dic[word]
                return max([len([syl for syl in pron if re.findall('[0-9]', syl)]) for pron in pronunciations])

        SD['nsyl'] = nsyl 
        SD['re'] = re
        SD['nltk'] = nltk
        SD['dic'] = dic

    words = [word.lower() for sent in nltk.sent_tokenize(the_text.decode('utf8'))
                for word in nltk.word_tokenize(sent)]
    words = [word for word in words if nsyl(word)>=3]
    return words

$CODE$ LANGUAGE plpythonu;