ALTER TABLE messages_content ADD COLUMN is_one_time BOOLEAN;
ALTER TABLE messages ADD COLUMN is_blocked BOOLEAN;
