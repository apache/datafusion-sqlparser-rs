CREATE INDEX orders_note_idx ON ONLY d.orders USING btree (note);
CREATE INDEX ON ONLY orders (note)