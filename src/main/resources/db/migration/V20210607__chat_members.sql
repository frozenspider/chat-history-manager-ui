CREATE TABLE chat_members (
  ds_uuid             UUID NOT NULL,
  chat_id             BIGINT NOT NULL,
  user_id             BIGINT NOT NULL,
  FOREIGN KEY (ds_uuid, chat_id) REFERENCES chats (ds_uuid, id),
  FOREIGN KEY (ds_uuid, user_id) REFERENCES users (ds_uuid, id)
);

INSERT INTO chat_members SELECT DISTINCT ds_uuid, chat_id, from_id FROM messages;
