# Process for creating `merged_fog_data.sas7bdat`

## External tables used

### StreetEvents

- `streetevents.speaker_data`
- `streetevents.calls`
- `streetevents.crsp_link`
- `streetevents.qa_pairs`

### CRSP and Compustat

- `crsp.ccmxpf_lnkhist`: For PERMNO-GVKEY links.
- `comp.fundq`: For earnings release dates (`rdq`).
- `crsp.stocknames`: For SIC codes.



- `bgt.within_call_data`:
    - Depends on `streetevents.qa_pairs`. 
    - Produced by `within_call_regressions.R`.
- `bgt.fog_recast`
    - Depends on `bgt.fog_recast`.
    - Produced by `fog/cast_fog_data.R`. 


# Details for `bgt.fog_recast`


- Re-arrange and merge fog data: `fog/cast_fog_data.R`
- Upload Stata file `~/Dropbox/research/BuGT/data/merged_fog_data.dta` to ACCT
- Run `import_stata.sas` to produce `merged_fog_data.sas7bdat`


- Run `get_fog_data.R` to produce `bgt.fog`

- Run `fog_decomposed.sql`, which combines `bgt.fog` and `bgt.jargon_words` to produce `bgt.fog_decomposed`.
- Run `cast_fog_data.R` to produce `bgt.fog_recast`.
- Run `create_fog_data.R` to produce `bgt.fog_data`, `fog_data.Rdata` (R format), and `fog_data.sas7bdat` (SAS format).

### Sentence counts

- Run `get_sent_counts.R`. 

### Forward-looking sentence data

- Run `get_fl_data.R`. 
- Relies on `prop_fl_sents` PL/Python function, which is created by `prop_fl_sents.sql`.

### Tone data

- `get_lm_tone_words.R`: R code to get data from Bill McDonald's website and put it the `bgt.lm_tone` table.
- `tone_count.sql`: Creates PL/Python function to get tone words. Depends on `bgt.lm_tone` table.
- `get_tone_data.R`: Calculates tone variables for earnings conference calls and puts data in `bgt.tone_data`. 

### Jargon word data
`top_words.sql`: Code to create PL/Python functions `word_counts` and `top_words`.
`num_jargon_words.sql`

