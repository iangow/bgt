CREATE OR REPLACE FUNCTION public.word_count(
	raw text,
	min_length integer DEFAULT 1)
    RETURNS integer
    LANGUAGE 'plpythonu'

    COST 100
    VOLATILE
    ROWS 0
AS $BODY$

    """ Function to count the number of words in a passage of text.
        Supplying parameter 'min_length' gives number of words with
        at least min_length letters.
    """
    import nltk
    tokens = nltk.word_tokenize(raw.decode('utf-8'))
    return len([word for word in tokens if len(word) >= min_length])

$BODY$;

