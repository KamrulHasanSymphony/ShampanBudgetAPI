using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class FiscalYearDetailRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(FiscalYearDetailVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

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
                    INSERT INTO FiscalYearDetail
                    (
                     FiscalYearId, Year, MonthId, MonthStart, MonthEnd, MonthName, MonthLock, Remarks
                    )
                    VALUES 
                    (
                     @FiscalYearId, @Year, @MonthId, @MonthStart, @MonthEnd, @MonthName, @MonthLock, @Remarks
                    );
                    SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId);
                    cmd.Parameters.AddWithValue("@Year", vm.Year);
                    cmd.Parameters.AddWithValue("@MonthId", vm.MonthId);
                    cmd.Parameters.AddWithValue("@MonthStart", vm.MonthStart);
                    cmd.Parameters.AddWithValue("@MonthEnd", vm.MonthEnd);
                    cmd.Parameters.AddWithValue("@MonthName", vm.MonthName);
                    cmd.Parameters.AddWithValue("@MonthLock", vm.MonthLock);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = "Success";
                    result.Message = "Data inserted successfully.";
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }



                return result;
            }
            catch (Exception ex)
            {

                result.ExMessage = ex.Message;
                result.Message = "Error in Insert.";
                return result;
            }
        }

        // Update Method
        public async Task<ResultVM> Update(FiscalYearDetailVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

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
UPDATE FiscalYearDetail
SET 
 FiscalYearId = @FiscalYearId,
 Year = @Year,
 MonthId = @MonthId,
 MonthStart = @MonthStart,
 MonthEnd = @MonthEnd,
 MonthName = @MonthName,
 MonthLock = @MonthLock,
 Remarks = @Remarks

WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId);
                    cmd.Parameters.AddWithValue("@Year", vm.Year);
                    cmd.Parameters.AddWithValue("@MonthId", vm.MonthId);
                    cmd.Parameters.AddWithValue("@MonthStart", vm.MonthStart);
                    cmd.Parameters.AddWithValue("@MonthEnd", vm.MonthEnd);
                    cmd.Parameters.AddWithValue("@MonthName", vm.MonthName);
                    cmd.Parameters.AddWithValue("@MonthLock", vm.MonthLock);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);


                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Data updated successfully.";
                    }
                    else
                    {
                        result.Message = "No rows were updated.";
                    }
                }



                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in Update.";
                return result;
            }
        }

        // Delete Method
        public async Task<ResultVM> Delete(string[] IDs, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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

                string query = "UPDATE Areas SET IsArchive = 0, IsActive = 1 WHERE Id IN (@Ids)";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = $"Data deleted successfully.";
                    }
                    else
                    {
                        result.Message = "No rows were deleted.";
                    }
                }


                return result;
            }
            catch (Exception ex)
            {

                result.ExMessage = ex.Message;
                result.Message = "Error in Delete.";
                return result;
            }
        }

        // List Method
        public async Task<ResultVM> List(string FiscalYearId, string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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
ISNULL(Id, 0) AS Id,
ISNULL(FiscalYearId, 0) AS FiscalYearId,
ISNULL(Year, 0) AS Year,
ISNULL(Year, 0) AS YearPeriod,
ISNULL(MonthStart, '19000101') AS MonthStart,
ISNULL(MonthEnd, '19000101') AS MonthEnd,
CHAR(64 + ROW_NUMBER() OVER (ORDER BY MonthStart)) AS PeriodSl,
ISNULL(Remarks, 0) AS Remarks,
ISNULL(MonthName, 0) AS MonthName,
0 MonthId,
ISNULL(MonthLock, 0) AS MonthLock

FROM FiscalYearDetails
WHERE 1 = 1";
                
                if (!string.IsNullOrEmpty(FiscalYearId))
                    query += " AND FiscalYearId=@FiscalYearId ";


                //query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                //using (SqlDataAdapter adapter = CreateAdapter(query, conn, transaction))
                //{
                //    adapter.Fill(dataTable);
                //}

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (!string.IsNullOrEmpty(FiscalYearId))
                    adapter.SelectCommand.Parameters.AddWithValue("@FiscalYearId", FiscalYearId);
                
                adapter.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new FiscalYearDetailVM
                {
                    Id = row.Field<int>("Id"),
                    FiscalYearId = row.Field<int>("FiscalYearId"),
                    Year = row.Field<int>("Year"),
                    Remarks = row.Field<string>("Remarks"),
                    MonthId = row.Field<int>("MonthId"),
                    MonthStart = row.Field<string>("MonthStart"),
                    MonthEnd = row.Field<string>("MonthEnd"),
                    MonthName = row.Field<string>("MonthName"),
                    MonthLock = row.Field<bool>("MonthLock"),
                    PeriodSl = row.Field<string>("PeriodSl"),

                }).ToList();


                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = modelList;
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
    Id,
    FiscalYearId,
    Year,
    Remarks,
    MonthId,
    MonthStart,
    MonthEnd,
    MonthName,
    MonthLock

FROM FiscalYearDetails
WHERE 1 = 1";


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

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
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
FROM FiscalYearDetails
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
                result.Message = "Error in Dropdown.";
                return result;
            }
        }


    }

}
