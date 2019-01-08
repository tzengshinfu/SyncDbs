using System;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.IO;
using System.Linq;

namespace SyncDbs {
    class Program {
        static StreamWriter logWriter;
        static DateTime startTime;
        static string tableSchema;
        static string tableName;
        static string destinationDatabaseName;
        static string previousDestinationDatabaseRecoveryMode;
        static DateTime finishTime;
        static double running_seconds;

        static void Main(string[] args) {
            try {
                startTime = DateTime.Now;
                tableSchema = "";
                tableName = "";
                destinationDatabaseName = "";
                previousDestinationDatabaseRecoveryMode = "";

                logWriter = new StreamWriter(new FileStream("SyncDbs." + startTime.ToString("yyyyMMddhhmmss") + ".log", FileMode.Create));
                logWriter.AutoFlush = true;

                PrintOut(logWriter, startTime.ToString("yyyy-MM-dd hh:mm:ss") + " 同步開始");

                using (var sourceConn = new SqlConnection(ConfigurationManager.ConnectionStrings["SOURCE"].ConnectionString)) {
                    sourceConn.Open();

                    using (var destinationConn = new SqlConnection(ConfigurationManager.ConnectionStrings["DESTINATION"].ConnectionString)) {
                        destinationConn.Open();

                        try {
                            destinationDatabaseName = GetDataBaseName(ConfigurationManager.ConnectionStrings["DESTINATION"].ConnectionString);
                            previousDestinationDatabaseRecoveryMode = GetTableContent(destinationConn,
                                "SELECT recovery_model_desc FROM sys.databases WHERE name = '" + destinationDatabaseName + "';").Rows[0]["recovery_model_desc"].ToString();

                            DisableConstraintsAndTriggers(destinationConn);
                            RunSqlSyntax(destinationConn, "ALTER DATABASE " + destinationDatabaseName + " SET RECOVERY SIMPLE");

                            using (var bulkWriter = new SqlBulkCopy(destinationConn, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity, null)) {
                                bulkWriter.BatchSize = 100000;
                                bulkWriter.BulkCopyTimeout = 0;
                                bulkWriter.EnableStreaming = true;

                                var baseTableNames = GetTableContent(sourceConn,
                                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' " +
                                    " AND TABLE_NAME NOT IN (" + "'" + string.Join("','", ConfigurationManager.AppSettings["BYPASS_TABLES"].Split(',')) + "'" + ") " +
                                    " ORDER BY TABLE_SCHEMA, TABLE_NAME;");

                                var currentProgress = 0;
                                var totalProgresses = baseTableNames.Rows.Count;
                                foreach (DataRow row in baseTableNames.Rows) {
                                    tableSchema = row["TABLE_SCHEMA"].ToString();
                                    tableName = row["TABLE_NAME"].ToString();

                                    var compareTableName = GetTableContent(destinationConn,
                                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" +
                                        tableName + "' AND TABLE_SCHEMA = '" + tableSchema + "';");
                                    if (compareTableName.Rows.Count == 0) {
                                        continue;
                                    }

                                    try {
                                        RunSqlSyntax(destinationConn, "TRUNCATE TABLE \"" + tableSchema + "\".\"" + tableName + "\";");
                                    }
                                    catch (Exception ex) {
                                        RunSqlSyntax(destinationConn, "DELETE FROM \"" + tableSchema + "\".\"" + tableName + "\";");

                                        var logFileName = GetTableContent(destinationConn, "SELECT name FROM sys.database_files WHERE type_desc = 'LOG';");
                                        RunSqlSyntax(destinationConn, "DBCC SHRINKFILE ('" + logFileName.Rows[0]["name"].ToString() + "', 1, TRUNCATEONLY);");
                                    }
                                    finally {
                                        try {
                                            var mappingTable = GetTableContent(sourceConn, "SELECT * FROM \"" + tableSchema + "\".\"" + tableName + "\";");

                                            bulkWriter.DestinationTableName = "\"" + tableSchema + "\"" + "." + "\"" + tableName + "\"";

                                            var computedColumns = GetTableContent(sourceConn,
                                                "SELECT name FROM sys.computed_columns WHERE object_id = OBJECT_ID('\"" + tableSchema + "\".\"" + tableName + "\"');");
                                            if (computedColumns.Rows.Count > 0) {
                                                foreach (DataColumn column in mappingTable.Columns) {
                                                    var isComputedColumn = computedColumns.AsEnumerable().Where(c => c.Field<string>("name") == column.ColumnName).AsDataView().Count;
                                                    if (isComputedColumn == 0) {
                                                        bulkWriter.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                                                    }
                                                }
                                            }

                                            bulkWriter.WriteToServer(mappingTable);
                                            bulkWriter.ColumnMappings.Clear();

                                            currentProgress += 1;
                                            PrintOut(logWriter, DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + " " + tableName +
                                                " 完成(" + currentProgress.ToString() + "/" + totalProgresses.ToString() + ")");
                                        }
                                        catch (Exception ex) {
                                            PrintOut(logWriter, DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + " " + tableName +
                                                " 未完成(" + currentProgress.ToString() + "/" + totalProgresses.ToString() + ")");
                                            PrintOut(logWriter, "異常原因 " + ex.ToString());
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception ex) {
                            PrintOut(logWriter, "異常原因 " + ex.ToString());
                            Console.ReadLine();
                        }
                        finally {
                            EnableConstraintsAndTriggers(destinationConn);
                            RunSqlSyntax(destinationConn, "ALTER DATABASE " + destinationDatabaseName + " SET RECOVERY " + previousDestinationDatabaseRecoveryMode);
                        }
                    }
                }

                finishTime = DateTime.Now;
                running_seconds = new TimeSpan(finishTime.Ticks - startTime.Ticks).TotalSeconds;

                PrintOut(logWriter, finishTime.ToString("yyyy-MM-dd hh:mm:ss") + " 同步完成");
                PrintOut(logWriter, "(跳過未同步:" + (ConfigurationManager.AppSettings["BYPASS_TABLES"] != "" ? ConfigurationManager.AppSettings["BYPASS_TABLES"] : "無") + ")");
                PrintOut(logWriter, "費時:" + GetTimeConsuming(running_seconds));
                Console.ReadLine();
            }
            catch (Exception ex) {
                PrintOut(logWriter, "異常原因 " + ex.ToString());
                Console.ReadLine();
            }
        }

        /// <summary>
        /// 關閉條件約束及觸發器以增進寫入速度
        /// </summary>
        /// <param name="conn">SQL連線</param>
        private static void DisableConstraintsAndTriggers(SqlConnection conn) {
            RunSqlSyntax(conn, "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';");
            RunSqlSyntax(conn, "EXEC sp_MSforeachtable 'ALTER TABLE ? DISABLE TRIGGER ALL';");
        }

        /// <summary>
        /// 開啟條件約束及觸發器
        /// </summary>
        /// <param name="conn">SQL連線</param>
        private static void EnableConstraintsAndTriggers(SqlConnection conn) {
            RunSqlSyntax(conn, "EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL';");
            RunSqlSyntax(conn, "EXEC sp_MSforeachtable 'ALTER TABLE ? ENABLE TRIGGER ALL';");
        }

        /// <summary>
        /// 執行SQL語法
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="sqlSyntax">SQL語法</param>
        /// <returns></returns>
        private static int RunSqlSyntax(SqlConnection conn, string sqlSyntax) {
            var command = new SqlCommand(sqlSyntax, conn);
            command.CommandTimeout = 0;
            return command.ExecuteNonQuery();
        }

        /// <summary>
        /// 取得Table內容
        /// </summary>
        /// <param name="conn">SQL連線</param>
        /// <param name="sqlSyntax">SQL語法</param>
        /// <returns>回傳結果</returns>
        private static DataTable GetTableContent(SqlConnection conn, string sqlSyntax) {
            var dt = new DataTable();
            var command = new SqlCommand(sqlSyntax, conn);
            command.CommandTimeout = 0;
            var reader = command.ExecuteReader();
            dt.Load(reader);

            return dt;
        }

        private static void PrintOut(StreamWriter writer, string message) {
            writer.WriteLine(message);
            Console.WriteLine(message);
        }

        private static string GetTimeConsuming(double seconds) {
            if (seconds / 60 < 1) {
                return seconds.ToString() + "秒";
            }
            else if (seconds / 60 / 60 < 1) {
                return Math.Round(seconds / 60, 2).ToString() + "分";
            }
            else {
                return Math.Round(seconds / 60 / 60, 2).ToString() + "時";
            }
        }

        private static string GetDataBaseName(string connectionString) {
            var connStrGroups = connectionString.Split(';');
            var initialCatalog = connStrGroups.Where(c => c.StartsWith("Initial Catalog") == true).Single();
            var databaseName = initialCatalog.Split('=')[1];

            return databaseName;
        }
    }
}