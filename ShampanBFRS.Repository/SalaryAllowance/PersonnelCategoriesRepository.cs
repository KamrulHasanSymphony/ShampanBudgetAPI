using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Repository.SalaryAllowance
{
    public class PersonnelCategoriesRepository : CommonRepository
    {

        // Insert Method
        public async Task<ResultVM> Insert(PersonnelCategoriesVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                INSERT INTO PersonnelCategories
                (
                    SL, CategoryOfPersonnel, IsActive, CreatedBy, CreatedFrom,CreatedOn
                )
                VALUES
                (
                    @SL,@CategoryOfPersonnel, @IsActive, @CreatedBy, @CreatedFrom,GETDate()
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@SL", vm.SL);
                    cmd.Parameters.AddWithValue("@CategoryOfPersonnel", vm.CategoryOfPersonnel ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", true);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                result.Id = vm.Id.ToString();
                result.DataVM = vm;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // Update Method
        public async Task<ResultVM> Update(PersonnelCategoriesVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                UPDATE PersonnelCategories
                SET 
                    SL = @SL,
                    CategoryOfPersonnel = @CategoryOfPersonnel,
                    IsActive = @IsActive,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateFrom = @LastUpdateFrom,
                    LastUpdateOn = GETDATE()
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@SL", vm.SL);
                    cmd.Parameters.AddWithValue("@CategoryOfPersonnel", vm.CategoryOfPersonnel ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.UpdateSuccess;
                    }
                    else
                    {
                        throw new Exception("No rows were updated.");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // Delete (Archive) Method
        public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", Id = string.Join(",", vm.IDs) };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $@"
                UPDATE Examinees
                SET IsArchive = 1, IsActive = 0,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateFrom = @LastUpdateFrom
     
                WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.ModifyBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.ModifyFrom ?? (object)DBNull.Value);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Department deleted successfully.";
                    }
                    else
                    {
                        throw new Exception("No rows were deleted.");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // List Method
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                 SELECT 
                ISNULL(M.Id,0) AS Id,
                ISNULL(M.SL, '') AS SL,
                ISNULL(M.CategoryOfPersonnel, '') AS CategoryOfPersonnel,                   
                ISNULL(M.IsActive, 0) AS IsActive,
                ISNULL(M.CreatedBy, '') AS CreatedBy,
				ISNULL(M.CreatedFrom, '') AS CreatedFrom,
                ISNULL(FORMAT(M.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn,
                ISNULL(FORMAT(M.LastUpdateOn,'yyyy-MM-dd HH:mm'),'') AS LastUpdateOn,
                ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
				ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom
                FROM PersonnelCategories M
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND M.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new PersonnelCategoriesVM
                {
                    Id = row.Field<int>("Id"),
                    SL = row.Field<int>("SL"),
                    CategoryOfPersonnel = row.Field<string>("CategoryOfPersonnel"),
                    IsActive = row.Field<bool>("IsActive"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    CreatedFrom = row.Field<string>("CreatedFrom"),
                    CreatedOn = row.Field<string>("CreatedOn"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateFrom = row.Field<string>("LastUpdateFrom"),
                    LastUpdateAt = row.Field<string>("LastUpdateOn")
                }).ToList();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = list;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT Id,Name, Remarks, IsActive, IsArchive, CreatedBy, CreatedAt, LastUpdateBy, LastUpdateAt
                FROM Departments
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND Id=@Id";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT Id, Name
                FROM ProductGroups
                WHERE IsActive = 1 AND IsArchive = 0
                ORDER BY Name";

                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                        adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dt);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // GetGridData Method
        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                var data = new GridEntity<PersonnelCategoriesVM>();

                string sqlQuery = @"
                -- Count
                SELECT COUNT(DISTINCT H.Id) AS totalcount
                FROM PersonnelCategories H
                WHERE H.IsActive = 1
                " + (options.filter.Filters.Count > 0
                        ? " AND (" + GridQueryBuilder<PersonnelCategoriesVM>.FilterCondition(options.filter) + ")"
                        : "") + @"

                -- Data
                SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? "H." + options.sort[0].field + " " + options.sort[0].dir
                            : "H.Id DESC") + @") AS rowindex,
                           	   ISNULL(H.Id,0) AS Id,
                               ISNULL(H.SL,'') AS SL,
                               ISNULL(H.CategoryOfPersonnel,'') AS CategoryOfPersonnel,
                               ISNULL(H.IsActive,0) AS IsActive,
                               CASE WHEN ISNULL(H.IsActive,'')=1 THEN 'Active' ELSE 'Inactive' END AS Status,
                               ISNULL(H.CreatedBy,'') AS CreatedBy,
							   ISNULL(H.CreatedFrom, '') AS CreatedFrom,
                               ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn,
                               ISNULL(H.LastUpdateBy,'') AS LastUpdateBy,
                               ISNULL(FORMAT(H.LastUpdateOn,'yyyy-MM-dd HH:mm'),'') AS LastUpdateOn,
							   ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom
                               FROM PersonnelCategories H
                              WHERE 1 = 1
                    " + (options.filter.Filters.Count > 0
                            ? " AND (" + GridQueryBuilder<PersonnelCategoriesVM>.FilterCondition(options.filter) + ")"
                            : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take=0 OR rowindex <= @take)";

                data = KendoGrid<PersonnelCategoriesVM>.GetGridDataQuestions_CMD(options, sqlQuery, "H.Id");

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = data;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }
    }
}
