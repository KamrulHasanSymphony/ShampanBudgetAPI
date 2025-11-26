using Newtonsoft.Json;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using System;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.Question
{
    public class QuestionSetHeaderRepository : CommonRepository
    {
        #region Insert
        public async Task<ResultVM> Insert(QuestionSetHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                INSERT INTO QuestionSetHeaders
                (
                    Name, TotalMark, Remarks, IsActive, IsArchive, CreatedBy, CreatedFrom, CreatedAt
                )
                VALUES
                (
                    @Name, @TotalMark, @Remarks, @IsActive, @IsArchive, @CreatedBy, @CreatedFrom, GETDATE()
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TotalMark", vm.TotalMark);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());
                }

                result.Status = "Success";
                result.Message = "QuestionSetHeader inserted successfully.";
                result.Id = vm.Id.ToString();
                result.DataVM = vm;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }
            return result;
        }
        #endregion

        #region Update
        public async Task<ResultVM> Update(QuestionSetHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", Id = vm.Id.ToString(), DataVM = vm };
            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                UPDATE QuestionSetHeaders 
                SET 
                    Name = @Name,
                    TotalMark = @TotalMark,
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
                    cmd.Parameters.AddWithValue("@TotalMark", vm.TotalMark);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = "Success";
                        result.Message = "QuestionSetHeader updated successfully.";
                    }
                    else
                    {
                        throw new Exception("No rows updated.");
                    }
                }
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }
            return result;
        }
        #endregion

        #region List
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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
                    ISNULL(M.TotalMark,0) AS TotalMark,
                    ISNULL(M.Remarks,'') AS Remarks,
                    ISNULL(M.IsActive,0) AS IsActive,
                    ISNULL(M.IsArchive,0) AS IsArchive,
                    ISNULL(M.CreatedBy,'') AS CreatedBy,
                    ISNULL(FORMAT(M.CreatedAt,'yyyy-MM-dd HH:mm'),'') AS CreatedAt,
                    ISNULL(M.LastUpdateBy,'') AS LastUpdateBy,
                    ISNULL(FORMAT(M.LastUpdateAt,'yyyy-MM-dd HH:mm'),'') AS LastUpdateAt
                FROM QuestionSetHeaders M
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND M.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new QuestionSetHeaderVM
                {
                    Id = row.Field<int>("Id"),
                    Name = row.Field<string>("Name"),
                    TotalMark = row.Field<int>("TotalMark"),
                    Remarks = row.Field<string>("Remarks"),
                    IsActive = row.Field<bool>("IsActive"),
                    IsArchive = row.Field<bool>("IsArchive"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    CreatedAt = row.Field<string>("CreatedAt"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateAt = row.Field<string>("LastUpdateAt")
                }).ToList();

                QuestionSetDetailRepository questionsetdetailsrepository = new QuestionSetDetailRepository();
                // ✅ Load Design Category Details
                var QuestionDetailsDataList = questionsetdetailsrepository.List(new[] { "M.QuestionSetHeaderId" }, conditionalValues, vm, conn, transaction);
                if (QuestionDetailsDataList.Status == "Success" && QuestionDetailsDataList.DataVM is DataTable dt2)
                {
                    string json = JsonConvert.SerializeObject(dt2);
                    var QuestionDetails = JsonConvert.DeserializeObject<List<QuestionSetDetailVM>>(json);

                    if (list.Any())
                        list.FirstOrDefault().questionSetDetailList = QuestionDetails;
                }

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
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
        #endregion

        #region GetGridData
        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                var data = new GridEntity<QuestionSetHeaderVM>();

                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT H.Id) AS totalcount
                FROM QuestionSetHeaders H
                WHERE H.IsArchive != 1
                " + (options.filter.Filters.Count > 0
                        ? " AND (" + GridQueryBuilder<QuestionSetHeaderVM>.FilterCondition(options.filter) + ")"
                        : "") + @"

                -- Data query
                SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? "H." + options.sort[0].field + " " + options.sort[0].dir
                            : "H.Id DESC") + @") AS rowindex,
                           ISNULL(H.Id,0) AS Id,
                           ISNULL(H.Name,'') AS Name,
                           ISNULL(H.TotalMark,0) AS TotalMark,
                           ISNULL(H.Remarks,'') AS Remarks,
                           ISNULL(H.IsActive,0) AS IsActive,
                           ISNULL(H.IsArchive,0) AS IsArchive,
                           CASE WHEN ISNULL(H.IsActive,0)=1 THEN 'Active' ELSE 'Inactive' END AS Status,
                           ISNULL(H.CreatedBy,'') AS CreatedBy,
                           FORMAT(H.CreatedAt,'yyyy-MM-dd HH:mm') AS CreatedAt,
                           ISNULL(H.LastUpdateBy,'') AS LastUpdateBy,
                           FORMAT(H.LastUpdateAt,'yyyy-MM-dd HH:mm') AS LastUpdateAt
                    FROM QuestionSetHeaders H
                    WHERE H.IsArchive != 1
                                " + (options.filter.Filters.Count > 0
                                                ? " AND (" + GridQueryBuilder<QuestionSetHeaderVM>.FilterCondition(options.filter) + ")"
                                                : "") + @"
                            ) AS a
                            WHERE rowindex > @skip AND (@take=0 OR rowindex <= @take)";

                data = KendoGrid<QuestionSetHeaderVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

                result.Status = "Success";
                result.Message = "Grid data retrieved successfully.";
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
        #endregion
    }
}
