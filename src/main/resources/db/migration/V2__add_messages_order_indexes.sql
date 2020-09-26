CREATE INDEX messages_order ON messages(time, source_id, internal_id);
CREATE INDEX messages_order_desc ON messages(time DESC, source_id DESC, internal_id DESC);
