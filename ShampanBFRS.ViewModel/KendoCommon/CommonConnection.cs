using ShampanBFRS.ViewModel.Utility;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace ShampanBFRS.ViewModel.KendoCommon
{
    public class CommonConnection
    {      
        public SqlConnection con;
        public SqlCommand cmd;
        public SqlDataReader rd;
        public SqlDataAdapter adpt;
        public DataTable dt;
        private IsolationLevel rIsolationLevel;
        private DbConnection dbConnection = null;
        private DbCommand dbCommand = null;
        private DbTransaction dbTransaction = null;
        private SqlTransaction objSqlTransaction;
        public bool IsConfigureDb { get; set; }
        public string ConnectionType = "";
        private bool isIsolation { get; set; }
        public DatabaseType DatabaseType { get; set; }

        public CommonConnection()
        {
            con = new SqlConnection(DatabaseHelper.GetConnectionString());
            if (ConfigurationSettings.AppSettings != null)
            {
                // var connectionType = ConfigurationSettings.AppSettings["DataBaseType"];
                var connectionType = "SQL";
                if (!string.IsNullOrEmpty(connectionType))
                {
                    ConnectionType = connectionType.Trim();
                }

            }
        }
        public CommonConnection(string conStr)
        {
            con = new SqlConnection(conStr);
            if (ConfigurationSettings.AppSettings != null)
            {
                // var connectionType = ConfigurationSettings.AppSettings["DataBaseType"];
                var connectionType = "SQL";
                if (!string.IsNullOrEmpty(connectionType))
                {
                    ConnectionType = connectionType.Trim();
                }

            }
        }

        public CommonConnection(IsolationLevel readCommitted)
        {

            rIsolationLevel = readCommitted;
            isIsolation = true;

            if (ConfigurationSettings.AppSettings != null)
            {
                var connectionType = ConfigurationSettings.AppSettings["DataBaseType"];
                if (!string.IsNullOrEmpty(connectionType))
                {
                    ConnectionType = connectionType.Trim();
                }

            }
            if (ConnectionType != null && ConnectionType == "SQL")
            {
                ConnectionStringSettingsCollection connections = ConfigurationManager.ConnectionStrings;

                this.DatabaseType = DatabaseType.SQL;
                dbConnection = new SqlConnection(connections["SqlConnectionString"].ConnectionString);
                IsConfigureDb = true;
            }
            else
            {
                IsConfigureDb = false;
            }
        }
        public void ExecuteNonQuery(string sql)
        {
            cmd = new SqlCommand(sql, con);
            con.Open();
            cmd.ExecuteNonQuery();
            con.Close();
        }
        public DataTable ExecuteReader(string sql)
        {
            dt = new DataTable();
            con.Open();
            cmd = new SqlCommand(sql, con);
            dt.Load(cmd.ExecuteReader());
            con.Close();
            return dt;

        }
        public DataTable GetDataTable(string sql)
        {
            try
            {
                con.Open();
                dt = new DataTable();
                cmd = new SqlCommand(sql, con);
                adpt = new SqlDataAdapter(cmd);

                adpt.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                if (con != null)
                {
                    con.Dispose();
                }
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }
        }
        public object ExecuteScalar(string commandText)
        {
            object result = null;
            cmd = new SqlCommand();

            try
            {
                cmd.CommandText = commandText;
                cmd.Connection = con;
                con.Open();
                result = cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw ex;
            }

            finally
            {
                if (con.State == ConnectionState.Open)
                {
                    con.Close();
                }
                if (con != null)
                {
                    con.Dispose();
                }
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }

            return result;
        }
        public int ExcuteSqlAfterReturn(string query)
        {
            int rvId = 0;

            try
            {
                //dbCommand = dbConnection.CreateCommand();
                //dbCommand.Transaction = dbTransaction;
                if (dbConnection.State == ConnectionState.Closed)
                {
                    dbConnection.Open();
                }
                dbCommand.CommandText = query + " select @@identity outId ";

                var data = dbCommand.ExecuteScalar();


                if (data != null)
                {
                    rvId = Convert.ToInt32(data);
                }
                else
                {
                    throw new Exception("Return value is null");
                }


            }
            catch (Exception e)
            {
                throw new Exception(e.Message);


            }
            return rvId;
        }
        public int ExcuteSqlAfterReturn(string query, string columnName)
        {
            int rvId = 0;

            try
            {
                query = query + " select @@identity as outId ";

                DbParameter p2 = new SqlParameter("outId", SqlDbType.Int);
                p2.Direction = ParameterDirection.ReturnValue;

                dbCommand = dbConnection.CreateCommand();
                dbCommand.Transaction = dbTransaction;
                dbCommand.Parameters.Add(p2);
                dbCommand.CommandText = query;
                dbCommand.ExecuteNonQuery();
                //dbTransaction.Commit();

                rvId = Convert.ToInt32(dbCommand.Parameters[0].Value);

            }
            catch (Exception e)
            {
                throw new Exception(e.Message);


            }
            return rvId;
        }
        public void BeginTransaction()
        {
            if (isIsolation)
            {
                if (dbConnection.State == ConnectionState.Closed)
                {
                    dbConnection.Open();
                }

                dbTransaction = dbConnection.BeginTransaction(rIsolationLevel);
                dbCommand = dbConnection.CreateCommand();
                dbCommand.Transaction = dbTransaction;
            }
            else
            {

                this.con.Open();
                this.objSqlTransaction = con.BeginTransaction();
            }
        }
        public void CommitTransaction()
        {
            if (isIsolation)
            {
                dbTransaction.Commit();
            }
            else
            {
                objSqlTransaction.Commit();
                con.Close();
            }

        }
        public void RollBack()
        {
            if (isIsolation)
            {
                dbTransaction.Rollback();
            }
            else
            {

                this.objSqlTransaction.Rollback();
                this.con.Close();
            }
        }
        public void Close()
        {
            if (isIsolation)
            {

                dbCommand.Dispose();
                dbTransaction.Dispose();
                dbConnection.Close();
            }
            else
            {

                con.Close();

            }

        }
        public void Dispose()
        {
            con.Dispose();

        }
    }
}
