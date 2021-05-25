-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT license.


DROP TABLE BenchmarkTest
GO
CREATE TABLE BenchmarkTest
(
	TestID int NOT NULL IDENTITY PRIMARY KEY,
	--TestName varchar(25) NOT NULL DEFAULT('Test ' + CAST(TestID AS varchar(10))),
	Description varchar(256) NULL,
	StartTime datetime NOT NULL DEFAULT(getdate()),
	EndTime datetime NULL,
	SuccessFlag bit DEFAULT(0) NULL
)

DROP TABLE BenchmarkTestQuery
GO
CREATE TABLE BenchmarkTestQuery
(
	ExecutionID int NOT NULL IDENTITY PRIMARY KEY,
	TestID int NOT NULL,
	QueryName varchar(256) NOT NULL,
	DurationInMs int NOT NULL,
	EntryTime datetime NOT NULL DEFAULT(getdate()),
	SuccessFlag bit NOT NULL,
	QueryTag nvarchar(255) NULL,
	ErrorMessage nvarchar(4000) NULL
)


IF EXISTS (SELECT * FROM sys.procedures WHERE name = N'usp_logtestpass_start' AND schema_id = SCHEMA_ID('dbo'))
	DROP PROCEDURE usp_logtestpass_start
GO
CREATE PROC [usp_logtestpass_start] @Description [varchar](256), @test_pass_id [int] OUT AS
BEGIN


	INSERT BenchmarkTest(Description)
	VALUES (@Description);

	SET @test_pass_id = @@IDENTITY;
END
GO

CREATE USER TestLogger WITH PASSWORD = '?????'

GRANT SELECT, INSERT, UPDATE ON BenchmarkTest TO TestLogger
GRANT SELECT, INSERT, UPDATE ON BenchmarkTestQuery TO TestLogger
GRANT EXECUTE ON usp_logtestpass_start TO TestLogger
