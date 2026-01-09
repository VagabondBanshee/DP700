--CREATE STORED PROCEDURE 2
CREATE PROC [fabric_de_series_020].[storedproc2]
@param1 VARCHAR(4000),
@param2 INT
AS
BEGIN
SELECT @param1 AS param1_value, @param2 AS param2_value, getdate() AS timestamp
END