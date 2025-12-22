using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.AccountVMs;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class UserProfileRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(UserProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        public async Task<ResultVM> UserInformationsInsert(UserInformationVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
                
                INSERT INTO UserInformations
                (
                    UserId,UserName,FullName, DepartmentId, CreatedBy,CreatedAt,CreatedFrom
                )
                VALUES
                (
                    @UserId,@UserName, @FullName, 1,@CreatedBy, @CreatedAt, @CreatedFrom
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {

                    cmd.Parameters.AddWithValue("@UserId", vm.UserId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@UserName", vm.UserName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FullName", vm.FullName ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@DepartmentId", vm.DepartmentId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedAt", vm.CreatedAt ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    //vm.Id = Convert.ToInt32(cmd.ExecuteScalar());
                    int newId = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
                    result.Id = newId.ToString();
                    result.DataVM = vm;
                }
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }

            return result;
        }

        // Update Method
        public async Task<ResultVM> Update(UserProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        // Delete Method
        public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            throw new NotImplementedException();
        }

        // List Method
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }


                string query = $@"
SELECT 
 U.Id 
,U.UserName
,U.FullName
,U.Email
,U.PhoneNumber
,U.PasswordHash
,U.NormalizedPassword
,ISNULL(U.IsHeadOffice,0) IsHeadOffice

FROM 
[{DatabaseHelper.AuthDbName()}].[dbo].AspNetUsers AS U
WHERE 1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND U.Id = @Id ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new UserProfileVM
                {
                    Id = row["Id"].ToString(),
                    UserName = row["UserName"].ToString(),
                    FullName = row["FullName"].ToString(),
                    IsHeadOffice = Convert.ToBoolean(row["IsHeadOffice"]),
                    Email = row["Email"].ToString(),
                    PhoneNumber = row["PhoneNumber"].ToString(),
                    Password = row["NormalizedPassword"].ToString(),
                    ConfirmPassword = row["NormalizedPassword"].ToString(),
                    CurrentPassword = row["NormalizedPassword"].ToString(),
                    NormalizedPassword = row["NormalizedPassword"].ToString(),
                }).ToList();

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;

                return result;

            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
SELECT 
 U.Id 
,U.UserName
,U.FullName
,U.Email
,U.PhoneNumber
,U.PasswordHash

FROM 

[dbo].[AspNetUsers] AS U

WHERE 1 = 1 ";

                DataTable dataTable = new DataTable();

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
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

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
                SELECT Id, UserName Name
                FROM [dbo].[AspNetUsers]
                WHERE 1 = 1
                ORDER BY UserName ";

                DataTable dropdownData = new DataTable();
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                    {
                        adapter.SelectCommand.Transaction = transaction;
                    }
                    adapter.Fill(dropdownData);
                }

                result.Status = "Success";
                result.Message = "Dropdown data retrieved successfully.";
                result.DataVM = dropdownData;

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status =MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<UserProfileVM>();

                // Define your SQL query string
                string sqlQuery = $@"
            -- Count query
                    SELECT COUNT(DISTINCT U.Id) AS totalcount
                    FROM 
                    [{DatabaseHelper.AuthDbName()}].[dbo].AspNetUsers AS U
                    LEFT OUTER JOIN [{DatabaseHelper.DBName()}].[dbo].UserInformations SP ON ISNULL(U.Id,0) = ISNULL(SP.Id,0)
                    WHERE 1 = 1
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<UserProfileVM>.FilterCondition(options.filter) + ")" : "") + @"

                    -- Data query with pagination and sorting
                    SELECT * 
                    FROM (
                        SELECT 
                         ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "U.UserName DESC ") + $@") AS rowindex,
                         U.Id 
                        ,U.UserName
                        ,U.FullName
                        ,U.Email
                        ,U.PhoneNumber
                        ,U.PasswordHash
                        ,ISNULL(U.IsHeadOffice,0) IsHeadOffice                        

                        FROM 
                        [{DatabaseHelper.AuthDbName()}].[dbo].AspNetUsers AS U

                        WHERE 1 = 1
                  
            -- Add the filter condition
            " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<UserProfileVM>.FilterCondition(options.filter) + ")" : "") + @"

            ) AS a
            WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
        ";

                data = KendoGrid<UserProfileVM>.GetAuthGridData_CMD(options, sqlQuery, "U.UserName");

                result.Status =MessageModel.Success;
                result.Message =MessageModel.RetrievedSuccess;
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

    }


}
