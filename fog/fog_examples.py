#!/usr/bin/env python3
import re

import curses 
from curses.ascii import isdigit 

from nltk.corpus import cmudict 

d = cmudict.dict() 

def nsyl(word): 
    return len([x for x in d.get(word.lower(), [u'a'])[0] if re.search('\d$', x)])

def tokenize(raw):
    import nltk
    tokens = nltk.word_tokenize(raw.decode('utf-8'))
    return [word for word in tokens if re.search('[A-Za-z]', word)] 

def percent_complex(raw):
    nsyls = map(nsyl, tokenize(raw))
    return len([i for i in nsyls if i >=3]) * 1.0/ len([i for i in nsyls if i >0])

def word_count(raw):
    return len(tokenize(raw))

def sent_count(raw):
    import nltk
    sent_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    sents = sent_tokenizer.tokenize(raw.decode('utf-8'))
    return len(sents)

# Get a sample of text to operate on    
import psycopg2 as pg
import pandas as pd
from pandas.io.sql import frame_query 
 
conn = pg.connect(dbname='crsp') 
#, host='localhost', port=5433)
df = frame_query(r"""
  SELECT a.*, b.questions, b.answers 
  FROM bgt.fog AS a
  INNER JOIN streetevents.qa_pairs AS b
  ON a.file_name=b.file_name AND a.speaker_number = ANY(b.question_nums)
  WHERE percent_complex_words > 30 AND num_words > 20 AND 
    fog > 25 AND a.context='qa'
  LIMIT 100
""", con=conn )

for cat in  word_list.keys():
    df[cat] = map(lambda x: liwc_category_match(cat, x), df['speaker_text']) 

df["word_count"] = map(word_count,  df['speaker_text'])
df["sent_count"] = map(sent_count,  df['speaker_text'])
df.drop('speaker_text', 1).to_csv('../data/categories.csv')
