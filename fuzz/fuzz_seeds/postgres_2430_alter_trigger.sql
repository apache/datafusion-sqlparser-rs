ALTER TRIGGER old_trigger ON my_table RENAME TO new_trigger;
ALTER TRIGGER my_trigger ON my_table DEPENDS ON EXTENSION my_extension;
ALTER TRIGGER my_trigger ON my_table NO DEPENDS ON EXTENSION my_extension;
ALTER TRIGGER my_trigger ON my_schema.my_table RENAME TO new_trigger
