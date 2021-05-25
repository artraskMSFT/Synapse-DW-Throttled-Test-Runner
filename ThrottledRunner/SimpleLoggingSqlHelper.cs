using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace ThrottledRunner

{

    class SimpleLoggingSqlHelper
    {

        //Static members are not intended to be thread-safe
        /*
         * testPassId member variable
         * LogTestPassStart, LogTestPassEnd
         * GetTestCases
        */

        private const int _noTestActive = -1;

        private static int _testPassId = _noTestActive;

        public static int LogTestPassStart(string connString, string description)
        {
            int newTestPassId = -1;

            try
            {
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = "usp_logtestpass_start";


                        command.Parameters.Add("@Description", SqlDbType.VarChar, 256).Value = description;
                        SqlParameter testPassIdParam = command.Parameters.Add("@test_pass_id", SqlDbType.Int);
                        testPassIdParam.Direction = ParameterDirection.Output;

                        command.CommandTimeout = 60;
                        command.ExecuteNonQuery();


                        if (testPassIdParam.Value != null)
                        {
                            if (int.TryParse(testPassIdParam.Value.ToString(), out newTestPassId))
                            {
                                _testPassId = newTestPassId;
                                Console.WriteLine("Started Logging Test Pass ID {0}", newTestPassId);
                            }
                        }

                        if (newTestPassId == -1)
                        {
                            Console.WriteLine("*********** ERROR Failed to get Test ID for '{0}'", description);
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine("*********** ERROR in TelemetrySqlHelper: " + e.ToString());
            }

            return newTestPassId;
        }

        public static void LogTestPassEnd(string connString, int testPassId, int successFlag)
        {
            string cmdText = String.Format("UPDATE BenchmarkTest SET EndTime = getdate(), SuccessFlag = {0} WHERE TestID = {1}", successFlag, _testPassId);

            try
            {
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = CommandType.Text;
                        command.CommandText = cmdText;
                        
                        command.CommandTimeout = 60;
                        command.ExecuteNonQuery();

                        Console.WriteLine("Ended Logging Test Pass ID {0}", testPassId);
                    }
                }

                _testPassId = _noTestActive;
            }
            catch (SqlException e)
            {
                Console.WriteLine("*********** ERROR in TelemetrySqlHelper: " + e.ToString());
            }

        }


        //To support concurrent tests, these members need to be thread-safe
        //Therefore they are instance methods
        /*
         * LogTestRunStart
         * LogTestRunEnd
        */
        public string LogTestRunStart(string connString, int testPassId)
        {
            //if (_testPassId == _noTestActive)
            //    throw new InvalidOperationException("LogTestRunStart cannot be called if a TestPass has not been started first.");


            return String.Empty;
        }

        public void LogTestRunEnd(string connString, int testPassId, string queryName, int durationinMs, int successFlag, string queryTag, SqlException sqlEx)
        {


            //if (_testPassId == _noTestActive)
            //    throw new InvalidOperationException("LogTestRunEnd cannot be called if a TestPass has not been started first.");

            string errText = "NULL";

            if (sqlEx != null)
            {
                errText = String.Format("'ErrorNumber: {0} | ErrorSeverity: {1} | ErrorState: {2} | ErrorProcedure: {3} | ErrorMessage: {4}'", sqlEx.Number, "?", sqlEx.State, sqlEx.Procedure, sqlEx.Message.Replace("'", "''"));
            }

            string cmdText = String.Format("INSERT BenchmarkTestQuery (TestID, QueryName, DurationInMs, SuccessFlag, ErrorMessage, QueryTag) VALUES ({0}, '{1}', {2}, {3}, {4}, '{5}')",
                                            testPassId,
                                            queryName,
                                            durationinMs,
                                            successFlag,
                                            errText,
                                            queryTag);


            try
            {
                using (SqlConnection conn = new SqlConnection(connString))
                {

                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    using (SqlCommand command = new SqlCommand(cmdText, conn))
                    {

                        command.CommandTimeout = 60;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine("*********** ERROR in TelemetrySqlHelper: " + e.ToString());
            }

            return;
        }

        public static void PersistSystemViews(string connString, int testPassId)
        {
            Console.WriteLine("Persisting selected DMVs to tables in the diag schema");


            string tableSuffix = String.Format("{0}_{1}", System.IO.Path.GetRandomFileName().Replace(".", ""), testPassId);

            string cmdTemplate = @"--CREATE SCHEMA diag
--GO

CREATE TABLE diag.pdw_exec_requests_$sfx$
WITH (	DISTRIBUTION = ROUND_ROBIN,	HEAP)
AS
	SELECT * FROM sys.dm_pdw_exec_requests;


CREATE TABLE diag.pdw_errors_$sfx$
WITH (	DISTRIBUTION = ROUND_ROBIN,	HEAP )
AS
	SELECT * FROM sys.dm_pdw_errors;


CREATE TABLE diag.pdw_request_steps_$sfx$
WITH ( DISTRIBUTION = ROUND_ROBIN,	HEAP )
AS
	SELECT * FROM sys.dm_pdw_request_steps;


CREATE TABLE diag.pdw_sql_requests_$sfx$
WITH (	DISTRIBUTION = ROUND_ROBIN,	HEAP )
AS
	SELECT * FROM sys.dm_pdw_sql_requests;


CREATE TABLE diag.pdw_dms_workers_$sfx$
WITH ( DISTRIBUTION = ROUND_ROBIN,	HEAP)
AS
	SELECT * FROM sys.dm_pdw_dms_workers;
";

            StringBuilder sbCmdText = new StringBuilder(cmdTemplate);
            sbCmdText.Replace("$sfx$", tableSuffix);

            try
            {
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = CommandType.Text;
                        command.CommandText = sbCmdText.ToString();

                        command.CommandTimeout = 120;
                        command.ExecuteNonQuery();

                        Console.WriteLine("Persisted system views with suffix {0}", tableSuffix);
                    }
                }


            }
            catch (SqlException e)
            {
                Console.WriteLine("*********** ERROR in TelemetrySqlHelper: " + e.ToString());
            }

        }
    }
}


