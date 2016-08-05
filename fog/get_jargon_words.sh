#!/usr/bin/env bash
psql -f fog/word_freq.sql
psql -f fog/num_jargon_words.sql
