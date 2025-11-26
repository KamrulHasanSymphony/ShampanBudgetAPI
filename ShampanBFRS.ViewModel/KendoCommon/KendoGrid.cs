using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;


namespace ShampanBFRS.ViewModel.KendoCommon
{
    public class KendoGrid<T>
    {
        private static SqlDataAdapter da;
        private static SqlConnection dbConn;
        private static SqlCommand cmd;
        private static DataSet ds;
        private static DataTable dt;
        private static int totalCount = 0;

        public static GridEntity<T> GetGridData(GridOptions gridOption, string ProcName, string CallType, string orderby, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "", string param7 = "", string param8 = "", string param9 = "", string param10 = "")
        {
            try
            {
                dt = new DataTable();
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";

                if (gridOption.filter != null)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }
                if (gridOption.sort != null)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }
                else if (gridOption.sort == null)
                {
                    orderby = orderby + " " + " DESC ";
                }
                if (!string.IsNullOrEmpty(filterby) && filterby.Contains("YYYY-MM-DD"))
                {
                    filterby = filterby.Replace("YYYY-MM-DD", "yyyy-MM-dd");
                }


                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(ProcName, dbConn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("@CallType", CallType));
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take ", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));
                cmd.Parameters.Add(new SqlParameter("@param1", param1));
                cmd.Parameters.Add(new SqlParameter("@param2", param2));
                cmd.Parameters.Add(new SqlParameter("@param3", param3));
                cmd.Parameters.Add(new SqlParameter("@param4", param4));
                cmd.Parameters.Add(new SqlParameter("@param5", param5));
                cmd.Parameters.Add(new SqlParameter("@param6", param6));
                cmd.Parameters.Add(new SqlParameter("@param7", param7));
                cmd.Parameters.Add(new SqlParameter("@param8", param8));
                cmd.Parameters.Add(new SqlParameter("@param9", param9));
                cmd.Parameters.Add(new SqlParameter("@param10", param10));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                dt = ds.Tables[1];
                totalCount = Convert.ToInt32(ds.Tables[0].Rows[0]["TotalCount"]);
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dt).ToList();
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public static GridEntity<T> GetGridDataQuestions_CMD(GridOptions gridOption, string sqlQuery, string orderby, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "", string param7 = "", string param8 = "", string param9 = "", string param10 = "")
        {
            try
            {
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";
                if (gridOption.filter.Filters.Count > 0)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }

                if (gridOption.sort.Count > 0)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }
                else
                {
                    orderby = orderby + " DESC";
                }

                // Open SQL connection
                dbConn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                dbConn.Open();
                cmd = new SqlCommand(sqlQuery, dbConn);
                cmd.CommandType = CommandType.Text;

                // Add parameters dynamically
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                DataTable countTable = ds.Tables[0]; // Total count should be in the first result set
                int totalCount = 0;

                if (countTable.Rows.Count > 0)
                {
                    totalCount = Convert.ToInt32(countTable.Rows[0]["totalcount"]);
                }

                // Access the second table for actual data (should be in Tables[1])
                DataTable dataTable = ds.Tables[1]; // The actual data should be in the second result set
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dataTable).ToList();

                // Create result object
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public static GridEntity<T> GetGridData_CMD(GridOptions gridOption, string sqlQuery, string orderby, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "", string param7 = "", string param8 = "", string param9 = "", string param10 = "")
        {
            try
            {
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";
                if (gridOption.filter.Filters.Count > 0)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }

                if (gridOption.sort.Count > 0)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }
                else
                {
                    orderby = orderby + " DESC";
                }

                // Open SQL connection
                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(sqlQuery, dbConn);
                cmd.CommandType = CommandType.Text;

                // Add parameters dynamically
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                DataTable countTable = ds.Tables[0]; // Total count should be in the first result set
                int totalCount = 0;

                if (countTable.Rows.Count > 0)
                {
                    totalCount = Convert.ToInt32(countTable.Rows[0]["totalcount"]);
                }

                // Access the second table for actual data (should be in Tables[1])
                DataTable dataTable = ds.Tables[1]; // The actual data should be in the second result set
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dataTable).ToList();

                // Create result object
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public static GridEntity<T> GetTransactionalGridData_CMD(GridOptions gridOption, string sqlQuery, string orderby, string[] conditionalFields, string[] conditionalValues, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "", string param7 = "", string param8 = "", string param9 = "", string param10 = "")
        {
            try
            {
                int count = 1;
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";
                if (gridOption.filter.Filters.Count > 0)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }

                if (gridOption.sort.Count > 0)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }
                else
                {
                    orderby = orderby + " DESC";
                }

                for (int i = 0; i < conditionalFields.Length; i++)
                {
                    string cField = "";
                    string field = "";
                    if (string.IsNullOrWhiteSpace(conditionalFields[i]) || string.IsNullOrWhiteSpace(conditionalValues[i]))
                    {
                        continue;
                    }
                    field = conditionalFields[i].ToString().Split(".")[1];
                    cField = conditionalFields[i].ToString();
                    cField = cField.Replace(".", "").Replace(" between", "");
                    field = field.Replace(".", "").Replace(" between", "");
                    sqlQuery = sqlQuery.Replace("@" + cField, "'" + conditionalValues[i] + "'");

                    if (conditionalFields[i].ToLower().Contains("date"))
                    {
                        if (count == 1)
                        {
                            sqlQuery = sqlQuery.Replace("@" + field, "'" + conditionalValues[i + 1] + "'");
                        }
                        count++;
                    }
                }

                // Open SQL connection
                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(sqlQuery, dbConn);
                cmd.CommandType = CommandType.Text;

                // Add parameters dynamically
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                DataTable countTable = ds.Tables[0]; // Total count should be in the first result set
                int totalCount = 0;

                if (countTable.Rows.Count > 0)
                {
                    totalCount = Convert.ToInt32(countTable.Rows[0]["totalcount"]);
                }

                // Access the second table for actual data (should be in Tables[1])
                DataTable dataTable = ds.Tables[1]; // The actual data should be in the second result set
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dataTable).ToList();

                // Create result object
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public static GridEntity<T> GetAuthGridData_CMD(GridOptions gridOption, string sqlQuery, string orderby, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "", string param7 = "", string param8 = "", string param9 = "", string param10 = "")
        {
            try
            {
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";
                if (gridOption.filter.Filters.Count > 0)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }

                if (gridOption.sort.Count > 0)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }
                else
                {
                    orderby = orderby + " DESC";
                }

                // Open SQL connection
                dbConn = new SqlConnection(DatabaseHelper.GetAuthConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(sqlQuery, dbConn);
                cmd.CommandType = CommandType.Text;

                // Add parameters dynamically
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                DataTable countTable = ds.Tables[0]; // Total count should be in the first result set
                int totalCount = 0;

                if (countTable.Rows.Count > 0)
                {
                    totalCount = Convert.ToInt32(countTable.Rows[0]["totalcount"]);
                }

                // Access the second table for actual data (should be in Tables[1])
                DataTable dataTable = ds.Tables[1]; // The actual data should be in the second result set
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dataTable).ToList();

                // Create result object
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public List<T> Select_Data_List<T>(string procedureName, string callName, string parm1 = "", string parm2 = "", string parm3 = "", string parm4 = "", string parm5 = "", string parm6 = "", string parm7 = "", string parm8 = "", string parm9 = "", string parm10 = "")
        {
            try
            {
                dt = new DataTable();
                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();

                cmd = new SqlCommand(procedureName, dbConn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("@CallType", callName));
                cmd.Parameters.Add(new SqlParameter("@Desc1", parm1));
                cmd.Parameters.Add(new SqlParameter("@Desc2", parm2));
                cmd.Parameters.Add(new SqlParameter("@Desc3", parm3));
                cmd.Parameters.Add(new SqlParameter("@Desc4", parm4));
                cmd.Parameters.Add(new SqlParameter("@Desc5", parm5));
                cmd.Parameters.Add(new SqlParameter("@Desc6", parm6));
                cmd.Parameters.Add(new SqlParameter("@Desc7", parm7));
                cmd.Parameters.Add(new SqlParameter("@Desc8", parm8));
                cmd.Parameters.Add(new SqlParameter("@Desc9", parm9));
                cmd.Parameters.Add(new SqlParameter("@Desc10", parm10));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();

                dt = ds.Tables[0];
                var dataList = new List<T>();

                if (dt.Rows.Count > 0)
                {
                    dataList = (List<T>)ListConversion.ConvertTo<T>(dt);
                }

                return dataList;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string[] RoleCreateEdit(string ProcName, UserRoleVM model)
        {
            string[] results = new string[3];
            results[0] = "Fail";
            results[1] = "Fail";
            results[2] = "";

            try
            {
                using (var dbConn = new SqlConnection(DatabaseHelper.GetConnectionString()))
                {
                    dbConn.Open();
                    using (var cmd = new SqlCommand(ProcName, dbConn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@Id", SqlDbType.NVarChar, 50) { Value = model.Id });
                        cmd.Parameters.Add(new SqlParameter("@Name", SqlDbType.NVarChar, 50) { Value = model.Name.Trim() });
                        cmd.Parameters.Add(new SqlParameter("@CreatedBy", SqlDbType.NVarChar, 50) { Value = model.CreatedBy });
                        cmd.Parameters.Add(new SqlParameter("@CreatedFrom", SqlDbType.NVarChar, 50) { Value = model.CreatedFrom });
                        cmd.Parameters.Add(new SqlParameter("@Operation", SqlDbType.NVarChar, 50) { Value = model.Operation });

                        var result = cmd.ExecuteScalar();

                        dbConn.Close();
                        dbConn.Dispose();
                        cmd.Dispose();

                        if (result.ToString() == "-1")
                        {
                            throw new Exception(model.Operation.ToLower() == "add" ? MessageModel.InsertSuccess : MessageModel.UpdateSuccess);
                        }
                        else
                        {
                            results[0] = "Success";
                            results[1] = model.Operation.ToLower() == "add" ? MessageModel.InsertSuccess : MessageModel.UpdateSuccess;
                            results[2] = result.ToString();

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                results[0] = "Fail";
                results[1] = ex.Message;
                throw;
            }

            return results;
        }

        public static DataTable GetAll(string procName, int id, string userId)
        {
            var dt = new DataTable();
            try
            {
                using (var dbConn = new SqlConnection(DatabaseHelper.GetConnectionString()))
                {
                    dbConn.Open();
                    using (var cmd = new SqlCommand(procName, dbConn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(new SqlParameter("@Id", id));

                        if (!string.IsNullOrEmpty(userId))
                        {
                            cmd.Parameters.Add(new SqlParameter("@UserId", userId));
                        }

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (Exception ex)
            {
                throw new Exception("Error in GetAll: " + ex.Message);
            }
        }

        public static string[] RoleMenuCreateEdit(string ProcName, RoleMenuVM model)
        {
            string[] results = new string[2];
            results[0] = "Fail";
            results[1] = "Fail";

            try
            {
                using (var dbConn = new SqlConnection(DatabaseHelper.GetConnectionString()))
                {
                    dbConn.Open();
                    using (var cmd = new SqlCommand(ProcName, dbConn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@RoleId", SqlDbType.NVarChar, 50) { Value = model.RoleId });
                        cmd.Parameters.Add(new SqlParameter("@MenuId", SqlDbType.NVarChar, 50) { Value = model.MenuId });
                        cmd.Parameters.Add(new SqlParameter("@CreatedBy", SqlDbType.NVarChar, 50) { Value = model.CreatedBy });
                        cmd.Parameters.Add(new SqlParameter("@CreatedFrom", SqlDbType.NVarChar, 50) { Value = model.CreatedFrom });

                        var result = cmd.ExecuteNonQuery();

                        dbConn.Close();
                        dbConn.Dispose();
                        cmd.Dispose();

                        if (result == -1)
                        {
                            throw new Exception(MessageModel.SubmissionFail);
                        }
                        else
                        {
                            results[0] = "Success";
                            results[1] = MessageModel.SubmissionSuccess;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                results[0] = "Fail";
                results[1] = ex.Message;
                throw;
            }

            return results;
        }

        public static string[] UserMenuCreateEdit(string ProcName, UserMenuVM model)
        {
            string[] results = new string[2];
            results[0] = "Fail";
            results[1] = "Fail";

            try
            {
                using (var dbConn = new SqlConnection(DatabaseHelper.GetConnectionString()))
                {
                    dbConn.Open();
                    using (var cmd = new SqlCommand(ProcName, dbConn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@UserId", SqlDbType.NVarChar, 50) { Value = model.UserId });
                        cmd.Parameters.Add(new SqlParameter("@RoleId", SqlDbType.NVarChar, 50) { Value = model.RoleId });
                        cmd.Parameters.Add(new SqlParameter("@MenuId", SqlDbType.NVarChar, 50) { Value = model.MenuId });
                        cmd.Parameters.Add(new SqlParameter("@CreatedBy", SqlDbType.NVarChar, 50) { Value = model.CreatedBy });
                        cmd.Parameters.Add(new SqlParameter("@CreatedFrom", SqlDbType.NVarChar, 50) { Value = model.CreatedFrom });

                        var result = cmd.ExecuteNonQuery();

                        dbConn.Close();
                        dbConn.Dispose();
                        cmd.Dispose();

                        if (result == -1)
                        {
                            throw new Exception(MessageModel.SubmissionFail);
                        }
                        else
                        {
                            results[0] = "Success";
                            results[1] = MessageModel.SubmissionSuccess;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                results[0] = "Fail";
                results[1] = ex.Message;
                throw;
            }

            return results;
        }

        public static string[] Delete(string ProcName, string RoleId, string UserId)
        {
            string[] results = new string[2];
            results[0] = "Fail";
            results[1] = "Fail";

            try
            {
                using (var dbConn = new SqlConnection(DatabaseHelper.GetConnectionString()))
                {
                    dbConn.Open();
                    using (var cmd = new SqlCommand(ProcName, dbConn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@RoleId", SqlDbType.NVarChar, 50) { Value = RoleId });
                        cmd.Parameters.Add(new SqlParameter("@UserId", SqlDbType.NVarChar, 50) { Value = UserId });

                        var result = cmd.ExecuteNonQuery();

                        dbConn.Close();
                        dbConn.Dispose();
                        cmd.Dispose();

                        if (result == -1)
                        {
                            throw new Exception(MessageModel.DeleteFail);
                        }
                        else
                        {
                            results[0] = "Success";
                            results[1] = MessageModel.DeleteSuccess;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                results[0] = "Fail";
                results[1] = ex.Message;
                throw;
            }

            return results;
        }

        public static DataSet GetDashBoardData(string procName, string branchId, string sslConn = "")
        {
            try
            {
                using (var dbConn = new SqlConnection(DatabaseHelper.GetConnectionString()))
                {
                    dbConn.Open();
                    using (var cmd = new SqlCommand(procName, dbConn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 2000; // Set timeout to 2 minutes
                        cmd.Parameters.Add(new SqlParameter("@BranchId", SqlDbType.NVarChar) { Value = branchId });

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            var ds = new DataSet();
                            da.Fill(ds);
                            dbConn.Close();
                            return ds;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error executing stored procedure: {ex.Message}", ex);
            }
        }

        public static GridEntity<T> GetAllSabreDataForDetails(GridOptions gridOption, string ProcName, string CallType, string orderby, string param1 = ""
            , string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "", string param7 = ""
            , string param8 = "", string param9 = "", string param10 = "")
        {
            try
            {
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";

                if (gridOption.filter != null)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }
                if (gridOption.sort != null)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }
                else if (gridOption.sort == null)
                {
                    orderby = orderby + " " + " DESC ";
                }
                if (!string.IsNullOrEmpty(filterby) && filterby.Contains("YYYY-MM-DD"))
                {
                    filterby = filterby.Replace("YYYY-MM-DD", "yyyy-MM-dd");
                }


                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(ProcName, dbConn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("@CallType", CallType));
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take ", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));
                cmd.Parameters.Add(new SqlParameter("@param1", param1));
                cmd.Parameters.Add(new SqlParameter("@param2", param2));
                cmd.Parameters.Add(new SqlParameter("@param3", param3));
                cmd.Parameters.Add(new SqlParameter("@param4", param4));
                cmd.Parameters.Add(new SqlParameter("@param5", param5));
                cmd.Parameters.Add(new SqlParameter("@SageDBName", ""));

                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                dt = ds.Tables[1];
                totalCount = Convert.ToInt32(ds.Tables[0].Rows[0]["TotalCount"]);
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dt).ToList();
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }


        public static GridEntity<T> GetGridData_5(GridOptions gridOption, string ProcName, string CallType, string orderby, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "")
        {
            try
            {
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";
                if (gridOption.filter != null)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }
                if (gridOption.sort != null && gridOption.sort.Count > 0)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }

                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(ProcName, dbConn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("@CallType", CallType));
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take ", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));
                cmd.Parameters.Add(new SqlParameter("@param1", param1));
                cmd.Parameters.Add(new SqlParameter("@param2", param2));
                cmd.Parameters.Add(new SqlParameter("@param3", param3));
                cmd.Parameters.Add(new SqlParameter("@param4", param4));
                cmd.Parameters.Add(new SqlParameter("@param5", param5));
                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                dt = ds.Tables[1];
                totalCount = Convert.ToInt32(ds.Tables[0].Rows[0]["TotalCount"]);
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dt).ToList();
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }


    }
}