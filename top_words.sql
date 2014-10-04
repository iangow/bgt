CREATE OR REPLACE FUNCTION word_counts(raw text)
RETURNS json AS
$CODE$
    import json, re, itertools
    from collections import Counter
    from nltk import sent_tokenize, word_tokenize

    # Use the Natural Language Toolkit to break text into
    # individual words
    text = raw.lower().decode('utf-8')
    token_lists = [word_tokenize(sent) for sent in sent_tokenize(text)]

    # We only want "words" with alphabetical characters.
    # Note that re.match() only looks at the first character
    # of the word.
    if len(token_lists) > 1:
        tokens = list(itertools.chain.from_iterable(token_lists))
    else:
        tokens = token_lists
    tokens = [word for word in tokens if re.match('[a-z]', word)]

    # Construct a counter of the words and return as JSON
    the_dict = Counter(tokens)
    return json.dumps(the_dict)
$CODE$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION top_words(counter jsonb, num integer)
RETURNS text[] AS
$$
    import json
    from collections import Counter
    return [word for word, count
                        in Counter(json.loads(counter)).most_common(num)]
$$ LANGUAGE plpythonu;

