CREATE STAGE s URL='s3://bucket/o''reilly/' ENDPOINT='https://o''reilly.example.com';
LIST @s PATTERN='.*o''reilly.*';
REMOVE @s PATTERN='.*it''s.*';
