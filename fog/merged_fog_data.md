Pr  rocess for creating `merged_fog_data.sas7bdat`

- Scan files for fog using Perl code (`create_fog_data.sh`, which runs `parse_xml_calls.pl`)
- Import fog data using `import_fog_speaker_data.pl`
- Re-arrange and merge fog data: `fog/cast_fog_data.R`
- Upload Stata file `~/Dropbox/research/BuGT/data/merged_fog_data.dta` to ACCT
- Run `import_stata.sas` to produce `merged_fog_data.sas7bdat`


### Forward-looking sentence data

- 

### Tone data

- `get_lm_tone_words.R`: R code to get data from Bill McDonald's website and put it the `bgt.lm_tone` table.
- `tone_count.sql`: Creates PL/Python function to get tone words. Depends on `bgt.lm_tone` table.
- `get_tone_data.R`: Calculates tone variables for earnings conference calls and puts data in `bgt.tone_data`. 

### Jargong word data
`top_words.sql`: Code to create PL/Python functions `word_counts` and `top_words`.
`num_jargon_words.sql`

