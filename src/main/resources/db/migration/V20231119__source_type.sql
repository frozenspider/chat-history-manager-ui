ALTER TABLE chats ADD COLUMN source_type VARCHAR(255);
UPDATE chats SET source_type = (SELECT source_type FROM datasets WHERE datasets.uuid = chats.ds_uuid);
ALTER TABLE chats ALTER source_type SET NOT NULL;
ALTER TABLE datasets DROP COLUMN source_type;
