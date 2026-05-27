-- Empty publication created first; 002_schema.sql adds tables to it.
CREATE PUBLICATION cdc_sse_streamer WITH (publish = 'insert, update, delete');
