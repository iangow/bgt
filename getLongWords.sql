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
                num_syls = [len([syl for syl in pron if re.findall('[0-9]', syl)]) 
                                         for pron in pronunciations]
                return max(num_syls)

        SD['nsyl'] = nsyl 
        SD['re'] = re
        SD['nltk'] = nltk
        SD['dic'] = dic

    words = [word.lower() for sent in nltk.sent_tokenize(the_text.decode('utf8'))
                for word in nltk.word_tokenize(sent)]

    # Require words to be more than three characters. Otherwise, "edu"="E-D-U" => 3 syllables
    words = [word for word in words if nsyl(word)>=3 and len(word)>3]
    return words

$CODE$ LANGUAGE plpythonu;
