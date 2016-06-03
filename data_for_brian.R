# Pull together relevant fog data ---
library(dplyr)

pg <- src_postgres()

speaker_data <- tbl(pg, sql("SELECT * FROM streetevents.speaker_data"))

data_for_brian <-
    speaker_data %>%
    mutate(category=sql("(CASE WHEN role='Analyst' THEN 'anal'
                    ELSE 'comp' END) || '_' || context")) %>%
    filter(file_name %in% c('1032590_T', '1036800_T', '1017960_T',
                            '1036160_T', '1032290_T', '1022980_T',
                            '1029090_T', '1018710_T', '1018520_T',
                            '1032330_T')) %>%
    filter(speaker_text !='')

# Calculate fog by passage, and by category ----
fog_by_passage <-
    data_for_brian %>%
    mutate(fog_alt=fog_alt(speaker_text),
           fog_original=fog_original(speaker_text),
           num_words=sql("(fog_data(speaker_text)).num_words")) %>%
    collect()

fog_by_category <-
    data_for_brian %>%
    group_by(file_name, category) %>%
    summarize(speaker_text=string_agg(speaker_text, ' ')) %>%
    mutate(fog_alt=fog_alt(speaker_text),
           fog_original=fog_original(speaker_text),
           num_words=sql("(fog_data(speaker_text)).num_words")) %>%
    collect()

# Save data to Excel ----
library(openxlsx)
wb <- createWorkbook("data_for_brian")
addWorksheet(wb, "fog_by_passage")
writeData(wb, "fog_by_passage", fog_by_passage)
addWorksheet(wb, "fog_by_category")
writeData(wb, "fog_by_category", fog_by_category)
saveWorkbook(wb, "~/Box Sync/BGT/data_for_brian.xlsx", overwrite = TRUE)
