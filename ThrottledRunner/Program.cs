using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Derived from https://github.com/AdamPaternostro/Azure-SQL-DW-Synapse-Test-Case-Runner for specific needs of a 
// fixed size client pool, running randomly selected queries for a fixed time period

namespace ThrottledRunner
{
    class Program
    {

        static int _testId = 0;
        static int completedTasks = 0;
        static Random rand = new Random();

        static string connectionString;
        static string loggingConnectionString;

        static bool appendStatementLabel = true;

        static FileSelectionModeEnum fileSelectionMode = FileSelectionModeEnum.SinglePass;

        static async Task Main(string[] args)
        {
            /*** READ CONFIGURATION ***/
            //Get test target connection string from app.config
            connectionString = ConfigurationManager.ConnectionStrings["targetDatabase"].ConnectionString;

            //Get database connection string from app.config for logging query times 
            loggingConnectionString = ConfigurationManager.ConnectionStrings["telemetryDb"].ConnectionString;

            //Get the number of concurrent test connections to be generated from app.config
            int CONCURRENCY_LEVEL = GetAppSettingAsInt("concurrencyLimit");

            //Get the number of minutes the test should run
            int TEST_DURATION_IN_MINUTES = GetAppSettingAsInt("testDurationInMinutes");

            //Get the local path containing the SQL test script files.  There should only be files with SQL text in this folder.
            string SCRIPT_PATH = ConfigurationManager.AppSettings["scriptFolder"];

            //Get setting indicating whether Synapse DMVs info should be saved after the test
            string SAVE_DMVS = ConfigurationManager.AppSettings["saveSystemViews"];

            //Get setting indicating whether to add text to the end of SQL statements to assist in correlation between driver telemetry and SQL Pool stystem views
            string APPEND_LABEL = ConfigurationManager.AppSettings["appendStatementLabel"];
            if (!String.IsNullOrEmpty(APPEND_LABEL) && APPEND_LABEL == "true")
                appendStatementLabel = true;
            else
                appendStatementLabel = false;

            //Get fileSelectionMode
            switch (ConfigurationManager.AppSettings["fileSelectionMode"])
            {
                case "SinglePass":
                    fileSelectionMode = FileSelectionModeEnum.SinglePass;
                    break;

                case "SequentialLoop":
                    fileSelectionMode = FileSelectionModeEnum.SequentialLoop;
                    break;

                default:
                    fileSelectionMode = FileSelectionModeEnum.Random;
                    break;
            }


            /*** RUN TEST ***/

            //Read the test SQL scripts from local folder
            Dictionary<string, string> sqlScripts = LoadSqlFromFiles(SCRIPT_PATH);

            //Now do the real work
            await MainLoopAsync(CONCURRENCY_LEVEL, TEST_DURATION_IN_MINUTES, sqlScripts);

            //Persist System Views for Analysis
            if (SAVE_DMVS == "true")
                SimpleLoggingSqlHelper.PersistSystemViews(connectionString, _testId);

            //All done
            Console.WriteLine("FINISHED");
            Console.ReadLine();

        }



        private static async Task MainLoopAsync(int concurrencyLevel, int testDurationInMinutes, Dictionary<string, string> sqlScripts)
        {
            //Create header record for test exec times
            _testId = SimpleLoggingSqlHelper.LogTestPassStart(loggingConnectionString, "Concurrent Test");

            //Extract a copy of the filenames (dictionary keys) as an array to be used for randomly picking the next test
            string[] filenames = sqlScripts.Keys.ToArray<string>();

            //This list will hold Task objects to kick off individual queries
            List<Task<string>> executeSQLTasks = new List<Task<string>>();

            //Calculate the time at which the test should end
            DateTime runUntilTime = DateTime.Now.AddMinutes(testDurationInMinutes);

            int fileListIndex = 0;

            //Load the initial set of tasks (up to CONCURRENCY_LEVEL) into a list
            //int nextIndex = 0;
            for (int i = 0; i < concurrencyLevel; i++)
            //while (nextIndex < concurrencyLevel)
            {
                string nextFilename = "";

                if (fileSelectionMode == FileSelectionModeEnum.Random)
                    nextFilename = GetRandomElement(filenames);
                else
                    nextFilename = filenames[fileListIndex];

                string sqlText = GetAndPrepSqlText(new StringBuilder(sqlScripts[nextFilename]));

                executeSQLTasks.Add(ExecuteSQLAsync(nextFilename, sqlText));

                fileListIndex = GetNextFileIndex(fileListIndex, filenames.Length);
                if (fileListIndex == -1)
                    break;

                //nextIndex++;
            }

            /** MAIN TEST LOOP **/
            while (executeSQLTasks.Count > 0 && DateTime.Now.CompareTo(runUntilTime) <= 0)
            {
                try
                {
                    Task<string> sqlTask = await Task.WhenAny(executeSQLTasks);
                    string filename = sqlTask.Result;
                    executeSQLTasks.Remove(sqlTask);

                    completedTasks++;
                    Console.WriteLine("Popped {0}", filename);
                }
                catch (Exception exc) { Console.WriteLine(exc); }

                if (DateTime.Now.CompareTo(runUntilTime) <= 0)
                {
                    string nextFilename = "";

                    //Depending on file selection mode, get the next filename to load into the executeSQLTasks List
                    //For Random and SequentialLoop, we always expect a 'next file'. 
                    //For SinglePass, when the list runs out we set the 'next file' to empty string.
                    switch (fileSelectionMode)
                    {
                        case FileSelectionModeEnum.Random:
                            nextFilename = GetRandomElement(filenames);
                            break;

                        case FileSelectionModeEnum.SequentialLoop:
                        case FileSelectionModeEnum.SinglePass:
                            if (fileListIndex >= 0)
                            {
                                nextFilename = filenames[fileListIndex];
                                fileListIndex = GetNextFileIndex(fileListIndex, filenames.Length);
                            }
                            else
                            {
                                nextFilename = "";
                            }

                            break;
                    }

                    //If the nextFileName is blank, we have nothing more to add to the executeSQLTasks List
                    if (!String.IsNullOrEmpty(nextFilename))
                    {
                        string sqlText = GetAndPrepSqlText(new StringBuilder(sqlScripts[nextFilename]));
                        executeSQLTasks.Add(ExecuteSQLAsync(nextFilename, sqlText));
                    }
                    else
                    {
                        Console.WriteLine("No new file to add to work list. {0} - {1} - {2}.", fileListIndex, nextFilename, fileSelectionMode);
                    }

                    //if (nextIndex <= filenames.Length)
                    //{
                    //    string nextFilename = filenames[nextIndex];
                    //    nextIndex++;

                    //    string sqlText = GetAndPrepSqlText(new StringBuilder(sqlScripts[nextFilename]));

                    //    executeSQLTasks.Add(ExecuteSQLAsync(nextFilename, sqlText));
                    //}

                }
            }
            /** END MAIN TEST LOOP **/

            //Test period is now complete; there are probably a few running tasks still finishing
            Console.WriteLine("TEST TIME COMPLETE, completedTasks = {0}", completedTasks);

            SimpleLoggingSqlHelper.LogTestPassEnd(loggingConnectionString, _testId, 1);

            //Wait for any unfinished tasks
            await Task.WhenAll(executeSQLTasks);

        }

        private static int GetNextFileIndex(int fileListIndex, int length)
        {
            int newIndex = fileListIndex + 1;

            if (newIndex >= length)
            {
                if (fileSelectionMode == FileSelectionModeEnum.SequentialLoop)
                    newIndex = 0;
                else if (fileSelectionMode == FileSelectionModeEnum.SinglePass)
                    newIndex = -1;
            }

            return newIndex;
        }

        private static int GetAppSettingAsInt(string keyName)
        {
            string configValue = ConfigurationManager.AppSettings[keyName];

            if (String.IsNullOrWhiteSpace(configValue))
                throw new ArgumentException(String.Format("App.Config does not contain a valid integer value for '{0}'", keyName));

            int configInt;

            if (!int.TryParse(configValue, out configInt))
                throw new ArgumentException(String.Format("App.Config does not contain a valid integer value for '{0}'", keyName));

            return configInt;
        }


        private static string GetAndPrepSqlText(StringBuilder sb)
        {
            const string CTAS_UNIQUE_TOKEN = "$CTAS-REPLACE$";

            sb.Replace(CTAS_UNIQUE_TOKEN, Path.GetRandomFileName().Replace(".", ""));

            return sb.ToString();
        }


        static string GetRandomElement(string[] arr)
        {
            return arr[rand.Next(arr.Length)];
        }

        static Dictionary<string, string> LoadSqlFromFiles(string scriptPath)
        {
            string[] filenames = Directory.GetFiles(scriptPath);

            Dictionary<string, string> sqlList = new Dictionary<string, string>();

            foreach (string filename in filenames)
            {
                sqlList.Add(Path.GetFileName(filename), File.ReadAllText(filename));
            }

            return sqlList;
        }

        static async Task<string> ExecuteSQLAsync(string filename, string sql)
        {
            return await Task<string>.Run(async () => ExecuteSQL(filename, sql));
        }

        private static string ExecuteSQL(string label, string sql)
        {
            string queryTag = Guid.NewGuid().ToString();

            //This should move to GetAndPrepSqlText?
            if (appendStatementLabel)
            {
                //sql = sql.Replace("~##LABEL##~", queryTag);
                //sql = sql.TrimEnd().TrimEnd(';') + String.Format(" OPTION (LABEL = 'Tag: {0}');", queryTag);
                sql = sql.TrimEnd().TrimEnd(';') + String.Format(" --'Tag: {0}'", queryTag);
            }

            SimpleLoggingSqlHelper telemetryHelper = new SimpleLoggingSqlHelper();
            string testRunId = null;

            Console.WriteLine("BEGIN: " + label);
            SqlConnection connection = new SqlConnection(connectionString);

            try
            {
                //using (SqlConnection connection = new SqlConnection(connectionString))
                //{
                connection.Open();
                testRunId = telemetryHelper.LogTestRunStart(loggingConnectionString, _testId);

                using (SqlCommand command = new SqlCommand(sql, connection))
                {

                    command.CommandTimeout = 0;


                    DateTime startTime = DateTime.Now;
                    command.ExecuteNonQuery();
                    TimeSpan duration = DateTime.Now.Subtract(startTime);
                    
                    telemetryHelper.LogTestRunEnd(loggingConnectionString, _testId, label, (int)duration.TotalMilliseconds, 1, queryTag, null);
                }
                //}
            }
            catch (SqlException e)
            {
                telemetryHelper.LogTestRunEnd(loggingConnectionString, _testId, label, 0, 1, queryTag, e);

                Console.WriteLine("*********** ERROR: " + e.ToString());

                int sleepFor = 1000;
                Console.WriteLine("Pausing for {0} msec", sleepFor);
                System.Threading.Thread.Sleep(sleepFor);
            }
            finally
            {
                connection.Close();
                connection.Dispose();
                Console.WriteLine("END:   " + label );
            }

            return label;
        } // Execute SQL

    }

}

