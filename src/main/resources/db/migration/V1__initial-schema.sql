CREATE TABLE datasets (
  uuid                UUID NOT NULL PRIMARY KEY,
  alias               VARCHAR(255),
  source_type         VARCHAR(255)
);

CREATE TABLE users (
  ds_uuid             UUID NOT NULL,
  id                  BIGINT NOT NULL,
  first_name          VARCHAR(255),
  last_name           VARCHAR(255),
  username            VARCHAR(255),
  phone_number        VARCHAR(20),
  last_seen_time      TIMESTAMP,
  -- Not in the entity
  is_myself           BOOLEAN NOT NULL,
  PRIMARY KEY (ds_uuid, id),
  FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid)
);

CREATE TABLE chats (
  ds_uuid             UUID NOT NULL,
  id                  BIGINT NOT NULL,
  name                VARCHAR(255),
  type                VARCHAR(255) NOT NULL,
  img_path            VARCHAR(4095),
  PRIMARY KEY (ds_uuid, id),
  FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid)
);

CREATE TABLE messages (
  ds_uuid             UUID NOT NULL,
  chat_id             BIGINT NOT NULL,
  internal_id         IDENTITY NOT NULL PRIMARY KEY,
  source_id           BIGINT,
  message_type        VARCHAR(255) NOT NULL,
  time                TIMESTAMP NOT NULL,
  edit_time           TIMESTAMP,
  from_id             BIGINT NOT NULL,
  forward_from_name   VARCHAR(255),
  reply_to_message_id BIGINT, -- not a reference
  title               VARCHAR(255),
  members             VARCHAR, -- serialized
  duration_sec        INT,
  discard_reason      VARCHAR(255),
  pinned_message_id   BIGINT,
  path                VARCHAR(4095),
  width               INT,
  height              INT,
  FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
  FOREIGN KEY (ds_uuid, chat_id) REFERENCES chats (ds_uuid, id),
  FOREIGN KEY (ds_uuid, from_id) REFERENCES users (ds_uuid, id)
);

CREATE UNIQUE INDEX messages_internal_id ON messages(ds_uuid, chat_id, source_id); -- allows duplicate NULLs

CREATE TABLE messages_text_elements (
  id                  IDENTITY NOT NULL,
  ds_uuid             UUID NOT NULL, -- not necessary, but is there for efficiency
  message_internal_id BIGINT NOT NULL,
  element_type        VARCHAR(255) NOT NULL,
  text                VARCHAR,
  href                VARCHAR(4095),
  hidden              BOOLEAN,
  language            VARCHAR(4095),
  PRIMARY KEY (id),
  FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
  FOREIGN KEY (message_internal_id) REFERENCES messages (internal_id)
);

CREATE INDEX messages_text_elements_idx ON messages_text_elements(ds_uuid, message_internal_id);

CREATE TABLE messages_content (
  id                  IDENTITY NOT NULL,
  ds_uuid             UUID NOT NULL, -- not necessary, but is there for efficiency
  message_internal_id BIGINT NOT NULL,
  element_type        VARCHAR(255) NOT NULL,
  path                VARCHAR(4095),
  thumbnail_path      VARCHAR(4095),
  emoji               VARCHAR(255),
  width               INT,
  height              INT,
  mime_type           VARCHAR(255),
  title               VARCHAR(255),
  performer           VARCHAR(255),
  lat                 DECIMAL(9,6),
  lon                 DECIMAL(9,6),
  duration_sec        INT,
  poll_question       VARCHAR,
  first_name          VARCHAR(255),
  last_name           VARCHAR(255),
  phone_number        VARCHAR(20),
  vcard_path          VARCHAR(4095),
  PRIMARY KEY (id),
  FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
  FOREIGN KEY (message_internal_id) REFERENCES messages (internal_id)
);

CREATE INDEX messages_content_idx ON messages_content(ds_uuid, message_internal_id);
