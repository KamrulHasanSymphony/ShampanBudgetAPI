using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;

namespace ShampanBFRS.Repository.SetUp
{
    public class COARepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(COAVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
        INSERT INTO COAs
        (
 
 PID
,COASL
,StructureId
,COAGroupId
,Code
,Name
,Nature
,COAType
,ReportType
,Remarks
,IsActive
,IsArchive
,IsRetainedEarning
,IsNetProfit
,IsDepreciation
,CreatedBy
,CreatedAt
,CreatedFrom
        )
        VALUES
        (
 @PID
,@COASL
,@StructureId
,@COAGroupId
,@Code
,@Name
,@Nature
,@COAType
,@ReportType
,@Remarks
,@IsActive
,@IsArchive
,@IsRetainedEarning
,@IsNetProfit
,@IsDepreciation
,@CreatedBy
,@CreatedAt
,@CreatedFrom
);
        SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {                  
                    cmd.Parameters.AddWithValue("@PID", vm.PID ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@COASL", vm.COASL ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@StructureId", vm.StructureId);       
                    cmd.Parameters.AddWithValue("@COAGroupId", vm.COAGroupId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Nature", vm.Nature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@COAType", vm.COAType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ReportType", vm.ReportType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);

                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@IsRetainedEarning", vm.IsRetainedEarning );
                    cmd.Parameters.AddWithValue("@IsNetProfit",vm.IsNetProfit );
                    cmd.Parameters.AddWithValue("@IsDepreciation", vm.IsDepreciation );

                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedAt", vm.CreatedAt ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());
                }

                result.Status = MessageModel.Success;
                result.Message =MessageModel.InsertSuccess;
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

        // Update Method
        public async Task<ResultVM> Update(COAVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                UPDATE COAs
                SET 
                 PID= @PID
                ,COASL= @COASL
                ,StructureId=@StructureId
                ,COAGroupId= @COAGroupId
                ,Code=@Code
                ,Name= @Name
                ,Nature= @Nature
                ,COAType= @COAType
                ,ReportType= @ReportType
                ,Remarks= @Remarks
                ,IsActive= @IsActive
                ,IsRetainedEarning= @IsRetainedEarning
                ,IsNetProfit= @IsNetProfit
                ,IsDepreciation= @IsDepreciation
                ,LastUpdateBy= @LastUpdateBy
                ,LastUpdateAt= @LastUpdateAt
                ,LastUpdateFrom= @LastUpdateFrom
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@PID", vm.PID ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@COASL", vm.COASL ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@StructureId", vm.StructureId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@COAGroupId", vm.COAGroupId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Nature", vm.Nature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@COAType", vm.COAType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ReportType", vm.ReportType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);

                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@IsRetainedEarning", vm.IsRetainedEarning);
                    cmd.Parameters.AddWithValue("@IsNetProfit", vm.IsNetProfit );
                    cmd.Parameters.AddWithValue("@IsDepreciation", vm.IsDepreciation);

                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateAt", vm.LastUpdateAt ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = "Success";
                        result.Message = "COA updated successfully.";
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
                UPDATE COAs
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
                        result.Message = "COAGroup deleted successfully.";
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
                     ISNULL(C.Id,0) Id
                    ,ISNULL(C.PID,0) PID
                    ,ISNULL(C.COASL,0) COASL
                    ,ISNULL(C.StructureId,0) StructureId
                    ,ISNULL(C.COAGroupId,0) COAGroupId
                    ,ISNULL(C.Code,'') Code
                    ,ISNULL(C.Name,'') Name
                    ,ISNULL(C.Nature,'') Nature
                    ,ISNULL(C.COAType,'') COAType
                    ,ISNULL(C.ReportType,'') ReportType
                    ,ISNULL(C.IsActive,0) IsActive
                    ,ISNULL(C.IsArchive,0) IsArchive
                    ,ISNULL(C.Remarks,'') Remarks
                    ,ISNULL(C.IsRetainedEarning,0) IsRetainedEarning
                    ,ISNULL(C.IsNetProfit,0) IsNetProfit
                    ,ISNULL(C.IsDepreciation,0) IsDepreciation
                    ,ISNULL(C.CreatedBy,'') CreatedBy
                    ,Isnull(FORMAT(C.CreatedAt,'yyyy-MM-dd HH:mm:ss'),'1900-01-01')CreatedAt
                    ,Isnull(FORMAT(C.LastUpdateAt,'yyyy-MM-dd HH:mm:ss'),'1900-01-01')LastUpdateAt
                    ,ISNULL(C.LastUpdateBy,'') LastUpdateBy

                     FROM COAs C
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND C.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new COAVM
                {
                    Id = row.Field<int>("Id"),
                    PID = row.Field<int>("PID"),
                    COASL = row.Field<int>("COASL"),
                    StructureId = row.Field<int>("StructureId"),
                    COAGroupId = row.Field<int>("COAGroupId"),
                    Code = row.Field<string>("Code"),
                    Name = row.Field<string>("Name"),
                    Nature = row.Field<string>("Nature"),
                    COAType = row.Field<string>("COAType"),
                    ReportType = row.Field<string>("ReportType"),
                    Remarks = row.Field<string>("Remarks"),
                    IsActive = row.Field<bool>("IsActive"),
                    IsArchive = row.Field<bool>("IsArchive"),
                    IsRetainedEarning = row.Field<bool>("IsRetainedEarning"),
                    IsNetProfit = row.Field<bool>("IsNetProfit"),
                    IsDepreciation = row.Field<bool>("IsDepreciation"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateAt = row.Field<string>("LastUpdateAt")

                }).ToList();

                result.Status = "Success";
                result.Message = "Department retrieved successfully.";
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
                SELECT Id,PID,COASL,StructureId,COAGroupId,Code,Name,Nature,COAType,ReportType,Remarks,IsActive,IsArchive,IsRetainedEarning,IsNetProfit,IsDepreciation,CreatedBy,CreatedAt,CreatedFrom
                FROM COAs
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
                result.Message = "Department DataTable retrieved successfully.";
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
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT Id, Name
                FROM COAs
                WHERE IsActive = 1 AND IsArchive = 0
                ORDER BY Name";

                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                        adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dt);
                }

                result.Status = "Success";
                result.Message = "COA dropdown data retrieved successfully.";
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
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                var data = new GridEntity<COAVM>();

                string sqlQuery = @"
                -- Count
                SELECT COUNT(DISTINCT C.Id) AS totalcount
                FROM COAs C
                WHERE C.IsArchive != 1
                " + (options.filter.Filters.Count > 0
                        ? " AND (" + GridQueryBuilder<COAVM>.FilterCondition(options.filter) + ")"
                        : "") + @"

                -- Data
                SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? "C." + options.sort[0].field + " " + options.sort[0].dir
                            : "C.Id DESC") + @") AS rowindex,
                             ISNULL(C.Id,0) Id
                            ,ISNULL(C.PID,0) PID
                            ,ISNULL(C.COASL,0) COASL
                            ,ISNULL(C.StructureId,0) StructureId
                            ,ISNULL(C.COAGroupId,0) COAGroupId
		                    ,ISNULL(CG.Name,'') GroupName
                            ,ISNULL(C.Code,'') Code
                            ,ISNULL(C.Name,'') Name
                            ,ISNULL(C.Nature,'') Nature
                            ,ISNULL(C.COAType,'') COAType
                            ,ISNULL(C.ReportType,'') ReportType
                            ,ISNULL(C.Remarks,0) AS Remarks
                            ,ISNULL(C.IsActive,0) AS IsActive
                            ,CASE WHEN ISNULL(C.IsActive,0)=1 THEN 'Active' ELSE 'Inactive' END AS Status
                            ,ISNULL(C.CreatedBy,'') AS CreatedBy
                            ,ISNULL(FORMAT(C.CreatedAt,'yyyy-MM-dd HH:mm'),'') AS CreatedAt
                            ,ISNULL(C.LastUpdateBy,'') AS LastUpdateBy
                            ,ISNULL(FORMAT(C.LastUpdateAt,'yyyy-MM-dd HH:mm'),'') AS LastUpdateAt
                            FROM COAs C
                            LEFT OUTER JOIN COAGroups CG ON C.COAGroupId =CG.Id
                    WHERE C.IsArchive != 1
                    " + (options.filter.Filters.Count > 0
                            ? " AND (" + GridQueryBuilder<COAVM>.FilterCondition(options.filter) + ")"
                            : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take=0 OR rowindex <= @take)";

                data = KendoGrid<COAVM>.GetGridDataQuestions_CMD(options, sqlQuery, "C.Id");

                result.Status = "Success";
                result.Message = "COA grid data retrieved successfully.";
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
