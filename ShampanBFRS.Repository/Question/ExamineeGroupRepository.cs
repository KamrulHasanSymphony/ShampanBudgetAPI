using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Repository.Question
{
    public class ExamineeGroupRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(ExamineeGroupVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                INSERT INTO ExamineeGroups
                (
                    Name, Remarks, 
                    IsActive, IsArchive, CreatedBy, CreatedFrom, CreatedAt
                )
                VALUES
                (
                    @Name, @Remarks, 
                    @IsActive, @IsArchive, @CreatedBy, @CreatedFrom, GETDATE()
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());
                }

                result.Status = "Success";
                result.Message = "ExamineeGroup inserted successfully.";
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
        public async Task<ResultVM> Update(ExamineeGroupVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                UPDATE ExamineeGroups
                SET 
                    Name = @Name,
                    Remarks = @Remarks,
                    IsActive = @IsActive,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateFrom = @LastUpdateFrom,
                    LastUpdateAt = GETDATE()
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = "Success";
                        result.Message = "ExamineeGroup updated successfully.";
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
                UPDATE ExamineeGroups
                SET IsArchive = 1, IsActive = 0,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateFrom = @LastUpdateFrom,
                    LastUpdateAt = GETDATE()
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
                        result.Message = "ExamineeGroup deleted successfully.";
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
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT 
                    ISNULL(M.Id,0) AS Id,
                    ISNULL(M.Name,'') AS Name,
                    ISNULL(M.Remarks,'') AS Remarks,
                    ISNULL(M.IsActive,0) AS IsActive,
                    ISNULL(M.IsArchive,0) AS IsArchive,
                    ISNULL(M.CreatedBy,'') AS CreatedBy,
                    ISNULL(FORMAT(M.CreatedAt,'yyyy-MM-dd HH:mm'),'') AS CreatedAt,
                    ISNULL(M.LastUpdateBy,'') AS LastUpdateBy,
                    ISNULL(FORMAT(M.LastUpdateAt,'yyyy-MM-dd HH:mm'),'') AS LastUpdateAt
                FROM ExamineeGroups M
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND M.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new ExamineeGroupVM
                {
                    Id = row.Field<int>("Id"),
                    Name = row.Field<string>("Name"),
                    Remarks = row.Field<string>("Remarks"),
                    IsActive = row.Field<bool>("IsActive"),
                    IsArchive = row.Field<bool>("IsArchive"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    CreatedAt = row.Field<string>("CreatedAt"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateAt = row.Field<string>("LastUpdateAt")
                }).ToList();

                result.Status = "Success";
                result.Message = "ExamineeGroups retrieved successfully.";
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
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT Id, Name, Remarks,
                       IsActive, IsArchive, CreatedBy, CreatedAt,
                       LastUpdateBy, LastUpdateAt
                FROM ExamineeGroups
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND Id=@Id";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                result.Status = "Success";
                result.Message = "ExamineeGroups DataTable retrieved successfully.";
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
        public async Task<ResultVM> Dropdown(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
                    SELECT Id,
                           Name,
                           Remarks
                    FROM ExamineeGroups
                    WHERE IsActive = 1 AND IsArchive = 0
                    ORDER BY Name
                    ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new ExamineeGroupVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Name = row["Name"]?.ToString(),
                    Remarks = row["Remarks"]?.ToString()
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

        // GetGridData Method
        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                var data = new GridEntity<ExamineeGroupVM>();

                string sqlQuery = @"
                -- Count
                SELECT COUNT(DISTINCT H.Id) AS totalcount
                FROM ExamineeGroups H
                WHERE H.IsArchive != 1
                " + (options.filter.Filters.Count > 0
                        ? " AND (" + GridQueryBuilder<ExamineeGroupVM>.FilterCondition(options.filter) + ")"
                        : "") + @"

                -- Data
                SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? "H." + options.sort[0].field + " " + options.sort[0].dir
                            : "H.Id DESC") + @") AS rowindex,
                           ISNULL(H.Id,0) AS Id,
                           ISNULL(H.Name,'') AS Name,
                           ISNULL(H.Remarks,'') AS Remarks,
                           ISNULL(H.IsActive,0) AS IsActive,
                           ISNULL(H.IsArchive,0) AS IsArchive,
                           CASE WHEN ISNULL(H.IsActive,0)=1 THEN 'Active' ELSE 'Inactive' END AS Status,
                           ISNULL(H.CreatedBy,'') AS CreatedBy,
                           ISNULL(FORMAT(H.CreatedAt,'yyyy-MM-dd HH:mm'),'') AS CreatedAt,
                           ISNULL(H.LastUpdateBy,'') AS LastUpdateBy,
                           ISNULL(FORMAT(H.LastUpdateAt,'yyyy-MM-dd HH:mm'),'') AS LastUpdateAt
                    FROM ExamineeGroups H
                    WHERE H.IsArchive != 1
                    " + (options.filter.Filters.Count > 0
                            ? " AND (" + GridQueryBuilder<ExamineeGroupVM>.FilterCondition(options.filter) + ")"
                            : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take=0 OR rowindex <= @take)";

                data = KendoGrid<ExamineeGroupVM>.GetGridDataQuestions_CMD(options, sqlQuery, "H.Id");

                result.Status = "Success";
                result.Message = "ExamineeGroups grid data retrieved successfully.";
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

        // ReportPreview Method
        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT 
                    ISNULL(H.Id,0) AS Id,
                    ISNULL(H.Name,'') AS Name,
                    ISNULL(H.Remarks,'') AS Remarks,
                    ISNULL(H.IsActive,0) AS IsActive,
                    ISNULL(H.IsArchive,0) AS IsArchive,
                    CASE WHEN ISNULL(H.IsActive,0)=1 THEN 'Active' ELSE 'Inactive' END AS Status,
                    ISNULL(H.CreatedBy,'') AS CreatedBy,
                    ISNULL(FORMAT(H.CreatedAt,'yyyy-MM-dd HH:mm'),'') AS CreatedAt,
                    ISNULL(H.LastUpdateBy,'') AS LastUpdateBy,
                    ISNULL(FORMAT(H.LastUpdateAt,'yyyy-MM-dd HH:mm'),'') AS LastUpdateAt
                FROM ExamineeGroups H
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND H.Id=@Id";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                result.Status = "Success";
                result.Message = "ExamineeGroups report data retrieved successfully.";
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
    }
}
