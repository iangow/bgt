BEGIN;

DELETE FROM bgt.fog_speaker WHERE file_name='%s';

INSERT INTO bgt.fog_speaker
SELECT file_name, last_update, context, speaker_number,
        (fog_data(speaker_text)).*
FROM streetevents.speaker_data
WHERE file_name='%s' AND speaker_name != 'Operator';

COMMIT;
