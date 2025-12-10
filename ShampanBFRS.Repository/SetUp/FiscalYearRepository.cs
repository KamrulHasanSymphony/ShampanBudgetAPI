using Newtonsoft.Json;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class FiscalYearRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(FiscalYearVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
                INSERT INTO FiscalYears
                (
                 Year, YearStart, YearEnd, YearLock, Remarks, CreatedBy, CreatedOn
                )
                VALUES 
                (
                 @Year, @YearStart, @YearEnd, @YearLock, @Remarks, @CreatedBy, GETDATE()
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Year", vm.Year);
                    cmd.Parameters.AddWithValue("@YearStart", vm.YearStart);
                    cmd.Parameters.AddWithValue("@YearEnd", vm.YearEnd);
                    cmd.Parameters.AddWithValue("@YearLock", vm.YearLock);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());


                    query = @" INSERT INTO FiscalYearDetails (
                            FiscalYearId,
                            Year,
                            Remarks,
                            --MonthId,
                            MonthStart,
                            MonthEnd,
                            MonthName,
                            MonthLock
                            
                        )
                        VALUES (
                            @FiscalYearId,  
                            @Year,          
                            @Remarks,       
                            --@MonthId,       
                            @MonthStart,    
                            @MonthEnd,      
                            @MonthName,     
                            @MonthLock  
                            
                        )";
                    foreach (FiscalYearDetailVM item in vm.fiscalYearDetails)
                    {
                        SqlCommand cmdDetails = new SqlCommand(query, conn, transaction);
                        cmdDetails.Parameters.AddWithValue("@FiscalYearId", vm.Id);
                        cmdDetails.Parameters.AddWithValue("@Year", vm.Year);
                        cmdDetails.Parameters.AddWithValue("@Remarks", item.Remarks ?? "-");
                        //cmdDetails.Parameters.AddWithValue("@MonthId", item.MonthId ?? 0);
                        cmdDetails.Parameters.AddWithValue("@MonthStart", item.MonthStart);
                        cmdDetails.Parameters.AddWithValue("@MonthEnd", item.MonthEnd);
                        cmdDetails.Parameters.AddWithValue("@MonthName", item.MonthName);
                        cmdDetails.Parameters.AddWithValue("@MonthLock", item.MonthLock);

                        cmdDetails.ExecuteNonQuery();
                    }

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = MessageModel.InsertFail;
                return result;
            }
        }

        // Update Method
        public async Task<ResultVM> Update(FiscalYearVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
                UPDATE FiscalYears
                SET 
                 Year = @Year, YearStart = @YearStart, YearEnd = @YearEnd, YearLock = @YearLock, 
                 Remarks = @Remarks, LastModifiedBy = @LastModifiedBy, LastModifiedOn = GETDATE()
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@Year", vm.Year);
                    cmd.Parameters.AddWithValue("@YearStart", vm.YearStart);
                    cmd.Parameters.AddWithValue("@YearEnd", vm.YearEnd);
                    cmd.Parameters.AddWithValue("@YearLock", vm.YearLock);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.LastModifiedBy ?? (object)DBNull.Value);

                    int rowsAffected = cmd.ExecuteNonQuery();

                    foreach (FiscalYearDetailVM item in vm.fiscalYearDetails)
                    {
                        query = @" Update FiscalYearDetails set
                              MonthLock =@MonthLock
                              ,Remarks=@Remarks
                              where Id=@Id
                              ";
                        SqlCommand cmdDetails = new SqlCommand(query, conn, transaction);
                        cmdDetails.Parameters.AddWithValue("@Id", item.Id);
                        cmdDetails.Parameters.AddWithValue("@MonthLock", item.MonthLock);
                        cmdDetails.Parameters.AddWithValue("@Remarks", item.Remarks ?? Convert.DBNull);
                        cmdDetails.ExecuteNonQuery();
                    }

                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.UpdateSuccess;
                    }
                    else
                    {
                        result.Message = MessageModel.UpdateFail;
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = MessageModel.UpdateFail;
                return result;
            }
        }

        // Delete Method
        public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                // If no connection is passed, create a new connection
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                // If no transaction is passed, create a new transaction
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                // Generate the IN clause for the fiscal year IDs
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                // Delete FiscalYears query
                string deleteFiscalYearsQuery = $"DELETE FROM FiscalYears WHERE Id IN ({inClause})";

                // Delete FiscalYearDetails query
                string deleteFiscalYearDetailsQuery = $"DELETE FROM FiscalYearDetails WHERE FiscalYearId IN ({inClause})";

                // Delete FiscalYears
                using (SqlCommand cmd = new SqlCommand(deleteFiscalYearsQuery, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected == 0)
                    {
                        throw new Exception("No rows were deleted.");
                    }
                }

                // Delete related FiscalYearDetails
                using (SqlCommand cmd = new SqlCommand(deleteFiscalYearDetailsQuery, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected == 0)
                    {
                        throw new Exception("No rows were deleted.");
                    }
                }

                // Set result status to success
                result.Status = MessageModel.Success;
                result.Message = MessageModel.DeleteSuccess;

                return result;
            }
            catch (Exception ex)
            {
                
                // Capture exception details
                result.ExMessage = ex.Message;
                result.Message = MessageModel.DeleteFail;
                return result;
            }
        }

        // List Method
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
            SELECT
            ISNULL(M.Id, 0) AS Id,
            ISNULL(M.Year, 0) AS Year,
            ISNULL(FORMAT(M.YearStart, 'yyyy-MM-dd HH:mm'), '1900-01-01') YearStart,
            ISNULL(FORMAT(M.YearEnd, 'yyyy-MM-dd HH:mm'), '1900-01-01') YearEnd,
            ISNULL(M.YearLock, 0) AS YearLock,
            ISNULL(M.Remarks, '') AS Remarks,
            ISNULL(M.CreatedBy, '') AS CreatedBy,
            ISNULL(FORMAT(M.CreatedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') CreatedOn,           
            ISNULL(M.LastModifiedBy, '') AS LastModifiedBy,
            ISNULL(FORMAT(M.LastModifiedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') LastModifiedOn
            FROM FiscalYears M
            WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND M.Id = @Id ";
                }

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);
                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);
                var model = new List<FiscalYearVM>();
                foreach (DataRow row in dataTable.Rows)
                {
                    model.Add(new FiscalYearVM
                    {
                        Id = row.Field<int>("Id"),
                        Year = row.Field<int>("Year"),
                        YearStart = row.Field<string>("YearStart"),
                        YearEnd = row.Field<string>("YearEnd"),
                        YearLock = row.Field<bool>("YearLock"),
                        Remarks = row.Field<string?>("Remarks"),
                        CreatedBy = row.Field<string?>("CreatedBy"),
                        CreatedOn = row.Field<string>("CreatedOn"),
                        LastModifiedBy = row.Field<string?>("LastModifiedBy"),
                        LastModifiedOn = row.Field<string?>("LastModifiedOn")
                    });
                }

                var detailsDataList = DetailsList(new[] { "D.FiscalYearId" }, conditionalValues, vm, conn, transaction);

                if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable dt)
                {
                    string json = JsonConvert.SerializeObject(dt);
                    var details = JsonConvert.DeserializeObject<List<FiscalYearDetailVM>>(json);

                    model.FirstOrDefault().fiscalYearDetails = details;
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = model;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in List.";
                return result;
            }
        }

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string query = @"
SELECT
            ISNULL(M.Id, 0) AS Id,
            ISNULL(M.Year, 0) AS Year,
            ISNULL(M.YearStart, '1900-01-01') AS YearStart,
            ISNULL(M.YearEnd, '1900-01-01') AS YearEnd,
            ISNULL(M.YearLock, 0) AS YearLock,
            ISNULL(M.Remarks, '') AS Remarks,
            ISNULL(M.CreatedBy, '') AS CreatedBy,
            ISNULL(M.CreatedOn, '1900-01-01') AS CreatedOn,
            ISNULL(M.LastModifiedBy, '') AS LastModifiedBy,
            ISNULL(M.LastModifiedOn, '1900-01-01') AS LastModifiedOn
        FROM FiscalYears M
        WHERE 1=1";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                DataTable dataTable = new DataTable();
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                    {
                        adapter.SelectCommand.Transaction = transaction;
                    }
                    adapter.Fill(dataTable);
                }

                result.Status =MessageModel.Success;
                result.Message =MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in ListAsDataTable.";
                return result;
            }
        }

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
SELECT Id, Name
FROM FiscalYears
WHERE IsActive = 1
ORDER BY Name";

                DataTable dropdownData = new DataTable();
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                    {
                        adapter.SelectCommand.Transaction = transaction;
                    }
                    adapter.Fill(dropdownData);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dropdownData;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = MessageModel.Fail;
                return result;
            }
        }

        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<FiscalYearVM>();

                // Define your SQL query string
                string sqlQuery = @"
    -- Count query
             SELECT COUNT(DISTINCT H.Id) AS totalcount
             FROM  FiscalYears H
            Where 1=1
    -- Add the filter condition
    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<FiscalYearVM>.FilterCondition(options.filter) + ")" : "") + @"

    -- Data query with pagination and sorting
    SELECT * 
    FROM (
        SELECT 
        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "H.Id DESC") + @") AS rowindex
        
            ,ISNULL(H.Id, 0) AS Id,
            ISNULL(H.Year, 0) AS Year,
            ISNULL(FORMAT(H.YearStart, 'yyyy-MM-dd'),'1900-01-01') AS YearStart,
            ISNULL(FORMAT(H.YearEnd, 'yyyy-MM-dd'),'1900-01-01') AS YearEnd,
            ISNULL(H.YearLock, 0) AS YearLock,
            ISNULL(H.Remarks, '') AS Remarks,					
            ISNULL(H.CreatedBy, '') AS CreatedBy,
            ISNULL(H.CreatedOn, '1900-01-01') AS CreatedOn,
            ISNULL(H.LastModifiedBy, '''') AS LastModifiedBy,
            ISNULL(H.LastModifiedOn, '1900-01-01') AS LastModifiedOn,
            ISNULL(H.CreatedFrom, '') AS CreatedFrom,
            ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom
            FROM FiscalYears H 
             Where 1=1

    -- Add the filter condition
    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<FiscalYearVM>.FilterCondition(options.filter) + ")" : "") + @"

    ) AS a
    WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
";

                data = KendoGrid<FiscalYearVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = data;

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public ResultVM DetailsList(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
                Select
                Id
                ,[FiscalYearId]
                      ,[Year]
                      ,MonthName
                      ,MonthStart
                      ,MonthEnd
                      ,MonthLock      
                      ,[Remarks]
                      ,[MonthId]
                FROM FiscalYearDetails D
                where 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.Id = @Id ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValue, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValue);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public bool DuplicateFiscal(int? year, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool exists = false;
            try
            {
                // Check if connection is passed in; if not, create a new one
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                // SQL query to retrieve fiscal year record by id
                string query = @"
            SELECT COUNT(1)
            FROM FiscalYears M
            WHERE Year = @year";

                // Create and configure SQL command
                SqlCommand cmd = new SqlCommand(query, conn);
                cmd.Parameters.AddWithValue("@year", year);

                // If a transaction is provided, assign it to the command
                if (transaction != null)
                {
                    cmd.Transaction = transaction;
                }

                // Execute the query and check if any record exists
                exists = Convert.ToInt32(cmd.ExecuteScalar()) > 0;

            }
            catch (Exception ex)
            {
                // Handle errors (log exception if needed)
                Console.WriteLine($"Error: {ex.Message}");
            }

            // Return true if fiscal year exists, otherwise false
            return exists;
        }

        public async Task<ResultVM> NewFiscalYear(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

            //    string query = @"
            //SELECT
            //ISNULL(M.Id, 0) AS Id,
            //ISNULL(M.Year, 0) AS Year,
            //ISNULL(FORMAT(M.YearStart, 'yyyy-MM-dd HH:mm'), '1900-01-01') YearStart,
            //ISNULL(FORMAT(M.YearEnd, 'yyyy-MM-dd HH:mm'), '1900-01-01') YearEnd,
            //ISNULL(M.YearLock, 0) AS YearLock,
            //ISNULL(M.Remarks, '') AS Remarks,
            //ISNULL(M.CreatedBy, '') AS CreatedBy,
            //ISNULL(FORMAT(M.CreatedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') CreatedOn,           
            //ISNULL(M.LastModifiedBy, '') AS LastModifiedBy,
            //ISNULL(FORMAT(M.LastModifiedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') LastModifiedOn
            //FROM FiscalYears M
            //WHERE 1=1";

            //    if (vm != null && !string.IsNullOrEmpty(vm.Id))
            //    {
            //        query += " AND M.Id = @Id ";
            //    }

            //    query = ApplyConditions(query, conditionalFields, conditionalValues, false);
            //    SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

            //    objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

            //    if (vm != null && !string.IsNullOrEmpty(vm.Id))
            //    {
            //        objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
            //    }

            //    objComm.Fill(dataTable);

                FiscalYearVM YearVM = new FiscalYearVM();

                YearVM.Year = 2026;
                YearVM.YearName = "2026-2027";
                YearVM.YearStart = "2026-07-01";
                YearVM.YearEnd = "2027-06-30";            
                YearVM.YearLock = false;
                YearVM.Remarks = "ok";
 
                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = YearVM;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in List.";
                return result;
            }
        }


    }

}
