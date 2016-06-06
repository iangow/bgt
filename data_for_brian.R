# Pull together relevant fog data ---
library(dplyr)

pg <- src_postgres()

speaker_data <- tbl(pg, sql("SELECT * FROM streetevents.speaker_data"))

sample <-
    speaker_data %>%
    mutate(category=sql("(CASE WHEN role='Analyst' THEN 'anal'
                    ELSE 'comp' END) || '_' || context")) %>%
    filter(file_name %in% c('1032590_T', '1036800_T', '1017960_T',
                            '1036160_T', '1032290_T', '1022980_T',
                            '1029090_T', '1018710_T', '1018520_T',
                            '1032330_T')) %>%
    filter(speaker_text !='')

# Calculate fog by passage, and by category ----
calc_stats <- function(df) {
    df %>%
    mutate(fog_alt=fog_alt(speaker_text),
           fog_original=fog_original(speaker_text),
           num_words=sql("(fog_data(speaker_text)).num_words"))
}

fog_by_passage <-
    sample %>%
    calc_stats

fog_by_category <-
    sample %>%
    group_by(file_name, category) %>%
    summarize(speaker_text=string_agg(speaker_text, ' ')) %>%
    calc_stats

# Save data to Excel ----
library(openxlsx)
wb <- createWorkbook("sample")
addWorksheet(wb, "fog_by_passage")
writeData(wb, "fog_by_passage", fog_by_passage)
addWorksheet(wb, "fog_by_category")
writeData(wb, "fog_by_category", fog_by_category)
saveWorkbook(wb, "~/Box Sync/BGT/data_for_brian.xlsx", overwrite = TRUE)
