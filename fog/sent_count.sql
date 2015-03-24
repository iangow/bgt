DROP FUNCTION IF EXISTS sent_count(text);

CREATE OR REPLACE FUNCTION sent_count(raw_text text)
  RETURNS integer AS
$BODY$
    """Function to count the number of sentences in a passage."""
    if 'nltk' in SD:
        nltk = SD['nltk']
        version = SD['version']
        sent_tokenizer = SD['sent_tokenizer']
    else:
        import nltk, sys

        version = sys.version_info.major
        SD["version"] = version

        sent_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
        SD['sent_tokenizer'] = sent_tokenizer

    if version==2:
        text= raw_text.decode('utf-8')
    else:
        text=raw_text

    sents = sent_tokenizer.tokenize(text)
    return len(sents)
$BODY$ LANGUAGE plpythonu;