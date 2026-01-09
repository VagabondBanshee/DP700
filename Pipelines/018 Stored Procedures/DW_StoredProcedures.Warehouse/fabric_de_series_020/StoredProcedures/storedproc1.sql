--CREATE STORED PROCEDURE 1
CREATE PROC [fabric_de_series_020].[storedproc1]
@param1 VARCHAR(4000),
@param2 INT
AS
BEGIN
INSERT INTO [fabric_de_series_020].[table1]
SELECT @param1, @param2
END