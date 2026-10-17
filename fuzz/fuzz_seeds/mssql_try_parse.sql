SELECT TRY_PARSE(CONCAT(prefix, value) AS DECIMAL(10,2) USING COALESCE(@culture, 'en-US')) FROM example
