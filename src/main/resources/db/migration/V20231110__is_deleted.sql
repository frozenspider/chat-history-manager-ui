ALTER TABLE messages ADD COLUMN is_deleted BOOLEAN;
UPDATE messages SET is_deleted = false;
ALTER TABLE messages ALTER is_deleted SET NOT NULL;
UPDATE messages SET message_type = 'regular', is_deleted = true WHERE message_type = 'service_message_deleted';
