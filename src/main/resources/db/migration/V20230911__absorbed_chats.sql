CREATE TABLE absorbed_chats (
  ds_uuid             UUID NOT NULL,
  chat_id             BIGINT NOT NULL,
  absorbed_chat_id    BIGINT NOT NULL,
  FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
  FOREIGN KEY (ds_uuid, chat_id) REFERENCES chats (ds_uuid, id)
);
