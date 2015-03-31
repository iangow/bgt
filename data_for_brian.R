# Pull together relevant fog data ---
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
rs <- dbGetQuery(pg, "
    DROP VIEW IF EXISTS bgt.data_for_brian;

    CREATE VIEW bgt.data_for_brian AS
    SELECT *, (CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_' || context AS category
    FROM streetevents.speaker_data
    WHERE file_name IN ('1032590_T', '1036800_T', '1017960_T', '1036160_T', '1032290_T',
                        '1022980_T', '1029090_T', '1018710_T', '1018520_T', '1032330_T')
        AND speaker_text !=''")

# Calculate fog by passage, and by category ----
fog_by_passage <- dbGetQuery(pg, "
    SELECT *, fog_alt(speaker_text), fog_original(speaker_text),
        (fog_data(speaker_text)).num_words
    FROM bgt.data_for_brian")

fog_by_category <- dbGetQuery(pg, "
   WITH agg_data AS (
        SELECT file_name, category, string_agg(speaker_text, ' ') AS speaker_text
        FROM bgt.data_for_brian
        GROUP BY file_name, category)

    SELECT file_name, category, fog_alt(speaker_text), fog_original(speaker_text),
        (fog_data(speaker_text)).num_words
    FROM agg_data")

rs <- dbGetQuery(pg, "DROP VIEW IF EXISTS bgt.data_for_brian;")
rs <- dbDisconnect(pg)

# Save data to Excel ----
library("xlsx")
write.xlsx2(fog_by_passage, file="fog_by_passage.xlsx", sheetName="fog_by_passage")
write.xlsx2(fog_by_category, file="fog_by_category.xlsx", sheetName="fog_by_category")
file.rename(from="fog_by_passage.xlsx", to="~/Box Sync/BGT/fog_by_passage.xlsx")
file.rename(from="fog_by_category.xlsx", to="~/Box Sync/BGT/fog_by_category.xlsx")
