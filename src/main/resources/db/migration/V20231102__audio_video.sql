UPDATE messages_content SET element_type = 'video' WHERE element_type = 'animation';
UPDATE messages_content SET element_type = 'video', title = NULL WHERE title = '<Video>';
UPDATE messages_content SET element_type = 'audio', title = NULL WHERE title = '<Audio>';
UPDATE messages_content SET title = NULL WHERE title = '<File>';
