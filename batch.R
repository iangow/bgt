source("fog/get_nchars.R")
source("fog/get_other_measures.R")
system("psql < fog/word_freq.sql")
system("psql < fog/num_jargon_words.sql")
