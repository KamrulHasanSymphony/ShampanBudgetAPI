using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using System.Data.SqlClient;
using System.Data;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.KendoCommon;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Options;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.SalaryAllowance;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRS.ViewModel.Sale;

namespace ShampanBFRS.Repository.Ceiling
{
    public class BudgetRepository : CommonRepository
    {

        public async Task<ResultVM> Insert(BudgetHeaderVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                string query = @"
        INSERT INTO BudgetHeaders
        (
            CompanyId,
            BranchId,
            FiscalYearId,
            BudgetSetNo,
            BudgetType,
            Code,
            TransactionType,
            TransactionDate,
            IsPost,
            Remarks,
            CreatedBy,
            CreatedOn,
            CreatedFrom,
            ApproveLevelRequired,
            CompletedApproveLevel,
            ApprovalStatus,
            IsApproveFinal
        )
        VALUES
        (
            @CompanyId,
            @BranchId,
            @FiscalYearId,
            @BudgetSetNo,
            @BudgetType,
            @Code,
            @TransactionType,
            @TransactionDate,
            @IsPost,
            @Remarks,
            @CreatedBy,
            @CreatedOn,
            @CreatedFrom,
            @ApproveLevelRequired,
            @CompletedApproveLevel,
            @ApprovalStatus,
            @IsApproveFinal
        );

        SELECT CAST(SCOPE_IDENTITY() AS INT);";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    // Required
                    cmd.Parameters.AddWithValue("@CompanyId", vm.CompanyId);
                    cmd.Parameters.AddWithValue("@BranchId", vm.BranchId);
                    cmd.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId);
                    cmd.Parameters.AddWithValue("@BudgetSetNo", vm.BudgetSetNo);
                    cmd.Parameters.AddWithValue("@BudgetType", vm.BudgetType);
                    cmd.Parameters.AddWithValue("@Code", vm.Code);
                    cmd.Parameters.AddWithValue("@TransactionType", vm.TransactionType);
                    cmd.Parameters.AddWithValue("@TransactionDate", DateTime.Now);

                    // Flags
                    cmd.Parameters.AddWithValue("@IsPost", 'N');
                    cmd.Parameters.AddWithValue("@IsApproveFinal", vm.IsApproveFinal ?? false);

                    // Approval
                    cmd.Parameters.AddWithValue("@ApproveLevelRequired", vm.ApproveLevelRequired ?? 0);
                    cmd.Parameters.AddWithValue("@CompletedApproveLevel", vm.CompletedApproveLevel ?? 0);
                    cmd.Parameters.AddWithValue("@ApprovalStatus", vm.ApprovalStatus ?? "");

                    // Audit
                    cmd.Parameters.AddWithValue("@Remarks", (object?)vm.Remarks ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy);
                    cmd.Parameters.AddWithValue("@CreatedOn", DateTime.Now);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? "Unknown");

                    vm.Id = (int)await cmd.ExecuteScalarAsync();
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                result.Id = vm.Id.ToString();
                result.DataVM = vm;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
            }

            return result;
        }

        public async Task<ResultVM> Update(BudgetHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                if (transaction == null)
                    transaction = conn.BeginTransaction();

                string query = @"
                    UPDATE BudgetHeaders
                    SET

                        BudgetType = @BudgetType,
                        FiscalYearId = @FiscalYearId,
                        TransactionDate = @TransactionDate,

                        LastUpdateBy = @LastUpdateBy,
                        LastUpdateOn = GETDATE(),
                        LastUpdateFrom = @LastUpdateFrom,
                        BranchId = @BranchId

                    WHERE ID = @ID";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {

                    cmd.Parameters.AddWithValue("@ID", vm.Id);
                    cmd.Parameters.AddWithValue("@BudgetType", vm.BudgetType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TransactionDate", vm.TransactionDate ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BranchId", vm.BranchId);

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();

                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.UpdateSuccess;
                    }
                    else
                    {
                        result.Message = "No rows were updated.";
                        throw new Exception("No rows were updated.");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.ExMessage = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                string query = @"
SELECT 
    ISNULL(M.Id, 0) AS Id,  
    ISNULL(M.CompanyId, 0) AS CompanyId,
    ISNULL(M.BranchId, 0) AS BranchId,
    ISNULL(M.Code, '') AS Code,
    ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
    ISNULL(M.BudgetType, '') AS BudgetType,
    ISNULL(M.TransactionDate, '1900-01-01') AS TransactionDate,
    ISNULL(M.IsPost, '') AS IsPost,
    CASE WHEN ISNULL(M.IsPost, '') = 'Y'  THEN 'Posted' ELSE 'Not Posted' END AS Status,
    ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
    ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
    ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
    ISNULL(M.PostedBy, '') AS PostedBy,
    ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
    ISNULL(M.PostedFrom, '') AS PostedFrom,  
    ISNULL(M.CreatedBy, '') AS CreatedBy,
    ISNULL(M.CreatedOn, '1900-01-01') AS CreatedOn,
    ISNULL(M.CreatedFrom, '') AS CreatedFrom
FROM BudgetHeaders M

WHERE 1 = 1
                ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND M.Id = @Id ";
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

                var modelList = dataTable.AsEnumerable().Select(row => new BudgetHeaderVM
                {
                    Id = row.Field<int>("Id"),
                    CompanyId = row.Field<int>("CompanyId"),
                    BranchId = row.Field<int>("BranchId"),
                    Code = row.Field<string>("Code"),
                    FiscalYearId = row.Field<int>("FiscalYearId"),
                    BudgetType = row.Field<string>("BudgetType"),
                    TransactionDate = row.Field<DateTime?>("TransactionDate")?.ToString("yyyy-MM-dd") ?? "",  // Format if necessary
                    IsPost = row.Field<string>("IsPost"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateOn = row.Field<DateTime?>("LastUpdateOn")?.ToString("yyyy-MM-dd") ?? "",  // Format if necessary
                    LastUpdateFrom = row.Field<string>("LastUpdateFrom"),
                    PostedBy = row.Field<string>("PostedBy"),
                    PostedOn = row.Field<DateTime?>("PostedOn")?.ToString("yyyy-MM-dd") ?? "",  // Format if necessary
                    PostedFrom = row.Field<string>("PostedFrom")
                }).ToList();





                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetBudgetDataForDetailsNew(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };


            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlQuery = $@"
        ---------------------------------------------
        -- TOTAL COUNT
        ---------------------------------------------
        SELECT COUNT(DISTINCT Sabres.Id) AS totalcount
        FROM Sabres
        LEFT OUTER JOIN COAs ON COAs.Id = Sabres.COAId
        INNER JOIN DepartmentSabres DS ON DS.SabreId = Sabres.Id
        INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId
        WHERE 1 = 1
          AND UI.UserName = 'erp'
          {(options.filter.Filters.Count > 0
                      ? " AND (" + GridQueryBuilder<BudgetDetailVM>.FilterCondition(options.filter) + ")"
                      : "")}
          {ApplyConditions("", conditionalFields, conditionalValues, false)}

        ---------------------------------------------
        -- GRID DATA
        ---------------------------------------------
        SELECT *
        FROM
        (
            SELECT
                ROW_NUMBER() OVER
                (
                    ORDER BY
                    {(options.sort.Count > 0
                                ? options.sort[0].field + " " + options.sort[0].dir
                                : "Sabres.Id DESC")}
                ) AS rowindex,

                ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS Serial,

                Sabres.Id          AS SabreId,
                COAs.Code          AS iBASCode,
                COAs.Name          AS iBASName,
                Sabres.Code        AS SabreCode,
                Sabres.[Name]      AS SabreName,
                0.0                AS InputTotal

            FROM Sabres
            LEFT OUTER JOIN COAs ON COAs.Id = Sabres.COAId
            INNER JOIN DepartmentSabres DS ON DS.SabreId = Sabres.Id
            INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId
            WHERE 1 = 1
              {(options.filter.Filters.Count > 0
                          ? " AND (" + GridQueryBuilder<BudgetDetailVM>.FilterCondition(options.filter) + ")"
                          : "")}
              {ApplyConditions("", conditionalFields, conditionalValues, false)}
        ) t
        WHERE t.rowindex > @skip
          AND (@take = 0 OR t.rowindex <= @take);
        ";

                var data = KendoGrid<BudgetDetailVM>
                    .GetTransactionalGridData_CMD(
                        options,
                        sqlQuery,
                        "Sabres.Id",
                        conditionalFields,
                        conditionalValues);

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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> InsertDetails(BudgetDetailVM detail, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";

                sqlText = @" INSERT INTO BudgetDetails (

 
                 BudgetHeaderId
                ,SabreId
                ,InputTotal
                ,M1
                ,M2
                ,M3
                ,M4
                ,M5
                ,M6
                ,M7
                ,M8
                ,M9
                ,M10
                ,M11
                ,M12
                ,Q1
                ,Q2
                ,Q3
                ,Q4
                ,H1
                ,H2
                ,Yearly
                ,IsPost
          
                ) VALUES (

                 @BudgetHeaderId
                ,@SabreId
                ,@InputTotal
                ,@M1
                ,@M2
                ,@M3
                ,@M4
                ,@M5
                ,@M6
                ,@M7
                ,@M8
                ,@M9
                ,@M10
                ,@M11
                ,@M12
                ,@Q1
                ,@Q2
                ,@Q3
                ,@Q4
                ,@H1
                ,@H2
                ,@Yearly
                ,@IsPost

                )SELECT SCOPE_IDENTITY() ";

                SqlCommand commands = new SqlCommand(sqlText, conn, transaction);

                commands.Parameters.Add("@BudgetHeaderId", SqlDbType.Int).Value = detail.BudgetHeaderId;
                commands.Parameters.Add("@SabreId", SqlDbType.Int).Value = detail.SabreId;
                commands.Parameters.Add("@InputTotal", SqlDbType.Decimal).Value = detail.InputTotal;
                commands.Parameters.Add("@M1", SqlDbType.Decimal).Value = detail.M1;
                commands.Parameters.Add("@M2", SqlDbType.Decimal).Value = detail.M2;
                commands.Parameters.Add("@M3", SqlDbType.Decimal).Value = detail.M3;
                commands.Parameters.Add("@M4", SqlDbType.Decimal).Value = detail.M4;
                commands.Parameters.Add("@M5", SqlDbType.Decimal).Value = detail.M5;
                commands.Parameters.Add("@M6", SqlDbType.Decimal).Value = detail.M6;
                commands.Parameters.Add("@M7", SqlDbType.Decimal).Value = detail.M7;
                commands.Parameters.Add("@M8", SqlDbType.Decimal).Value = detail.M8;
                commands.Parameters.Add("@M9", SqlDbType.Decimal).Value = detail.M9;
                commands.Parameters.Add("@M10", SqlDbType.Decimal).Value = detail.M10;
                commands.Parameters.Add("@M11", SqlDbType.Decimal).Value = detail.M11;
                commands.Parameters.Add("@M12", SqlDbType.Decimal).Value = detail.M12;
                commands.Parameters.Add("@Q1", SqlDbType.Decimal).Value = detail.Q1;
                commands.Parameters.Add("@Q2", SqlDbType.Decimal).Value = detail.Q2;
                commands.Parameters.Add("@Q3", SqlDbType.Decimal).Value = detail.Q3;
                commands.Parameters.Add("@Q4", SqlDbType.Decimal).Value = detail.Q4;
                commands.Parameters.Add("@H1", SqlDbType.Decimal).Value = detail.H1;
                commands.Parameters.Add("@H2", SqlDbType.Decimal).Value = detail.H2;
                commands.Parameters.Add("@Yearly", SqlDbType.Decimal).Value = detail.Yearly;
                commands.Parameters.Add("@IsPost", SqlDbType.NVarChar).Value = "";

                detail.Id = Convert.ToInt32(commands.ExecuteScalar());

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                result.Id = detail.Id.ToString();
                result.DataVM = detail;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        //DetailsList
        public async Task<ResultVM> DetailsList(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }

                string query = @"
              SELECT 
                 ISNULL(D.Id, 0) AS Id
                 ,D.BudgetHeaderId
                 ,D.SabreId
                 ,D.InputTotal
                 ,D.M1
                 ,D.M2
                 ,D.M3
                 ,D.M4
                 ,D.M5
                 ,D.M6
                 ,D.M7
                 ,D.M8
                 ,D.M9
                 ,D.M10
                 ,D.M11
                 ,D.M12
                 ,D.Q1
                 ,D.Q2
                 ,D.Q3
                 ,D.Q4
                 ,D.H1
                 ,D.H2
                 ,D.Yearly
                 ,COAs.Code          AS iBASCode
                 ,COAs.Name          AS iBASName
                 ,S.Code        AS SabreCode
                 ,S.Name      AS SabreName
                  FROM BudgetDetails D
  
                  LEFT OUTER JOIN Sabres S ON S.Id = D.SabreId
                  LEFT OUTER JOIN COAs ON COAs.Id = S.COAId
                  INNER JOIN DepartmentSabres DS ON DS.SabreId = S.Id
                  INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId

                  WHERE 1 = 1
  
                  ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.BudgetHeaderId = @Id ";
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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> GetGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };


            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<BudgetHeaderVM>();

                // Define your SQL query string
                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT M.Id) AS totalcount
FROM BudgetHeaders M
WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                -- Data query with pagination and sorting
                SELECT * 
                FROM (
                    SELECT 
                    ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "M.Id DESC") + @") AS rowindex,
   
                        ISNULL(M.Id, 0) AS Id,  
                        ISNULL(M.CompanyId, 0) AS CompanyId,
                        ISNULL(M.BranchId, 0) AS BranchId,
                        ISNULL(M.Code, '') AS Code,
                        ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
                        ISNULL(fy.YearName,'') AS YearName,
                        ISNULL(M.BudgetType, '') AS BudgetType,
                        ISNULL(M.TransactionDate, '1900-01-01') AS TransactionDate,
                        ISNULL(M.IsPost, '') AS IsPost,
                        CASE WHEN ISNULL(M.IsPost, '') = 'Y'  THEN 'Posted' ELSE 'Not Posted' END AS Status,
                        ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
                        ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
                        ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
                        ISNULL(M.PostedBy, '') AS PostedBy,
                        ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
                        ISNULL(M.PostedFrom, '') AS PostedFrom,  
                        ISNULL(M.CreatedBy, '') AS CreatedBy,
                        ISNULL(M.CreatedOn, '1900-01-01') AS CreatedOn,
                        ISNULL(M.CreatedFrom, '') AS CreatedFrom
                    FROM BudgetHeaders M
                     LEFT JOIN FiscalYears fy ON fy.Id = M.FiscalYearId
                    WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            ";

                // Execute the query and get data
                data = KendoGrid<BudgetHeaderVM>.GetTransactionalGridData_CMD(options, sqlQuery, "M.Id", conditionalFields, conditionalValues);

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

        public async Task<ResultVM> GetDetailDataById(GridOptions options, int masterId, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                var data = new GridEntity<BudgetDetailVM>();

                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT D.SabreId) AS totalcount
                FROM BudgetDetails D
                left outer join Sabres s on s.Id= D.SabreId
                left outer join COAs C ON s.COAId = C.Id
                WHERE D.BudgetHeaderId = @masterId
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetDetailVM>.FilterCondition(options.filter) + ")" : "") + @"

                -- Data query with pagination and sorting
                SELECT s.Code SabreCode,s.Name SabreName,SabreId,InputTotal,C.Code iBASCode, C.Name iBASName
                FROM (
                    SELECT
                                ROW_NUMBER() OVER (ORDER BY MAX(D.Id) DESC) AS rowindex,
                                ISNULL(D.SabreId, 0) AS SabreId,
                                SUM(ISNULL(D.InputTotal, 0)) AS InputTotal
                                FROM BudgetDetails D
                                Where D.BudgetHeaderId = @masterId
                                GROUP BY D.SabreId
                    -- Add the filter condition
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetDetailVM>.FilterCondition(options.filter) + ")" : "") + @"
                ) AS a
                left outer join Sabres s on s.Id=a.SabreId
                left outer join COAs C ON s.COAId = C.Id
                
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
                ";
                sqlQuery = sqlQuery.Replace("@masterId", "" + masterId + "");
                data = KendoGrid<BudgetDetailVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

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

        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new() { Status = "Fail", Message = "Error" };

            try
            {
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));
                string query = $@"
                    UPDATE BudgetHeaders
                    SET IsPost = 'Y', PostedBy = @PostedBy, PostedFrom = @PostedFrom, PostedOn = GETDATE()
                    WHERE Id IN ({inClause})";

                using SqlCommand cmd = new(query, conn, transaction);
                for (int i = 0; i < vm.IDs.Length; i++)
                    cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);

                cmd.Parameters.AddWithValue("@PostedBy", vm.ModifyBy ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@PostedFrom", vm.ModifyFrom ?? (object)DBNull.Value);

                int rows = await cmd.ExecuteNonQueryAsync();

                if (rows > 0)
                {
                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.PostSuccess;
                }
                else
                {
                    throw new Exception("No rows posted.");
                }
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }

            return result;
        }

        public async Task<ResultVM> BudgetFinalReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"


DECLARE @BranchId INT = @BId;
DECLARE @Year INT;               -- Base year (Estimated year)
DECLARE @EstimatedYear INT;
DECLARE @ApprovedYear INT;
DECLARE @ActualYear INT;

DECLARE @EstimatedYearName NVARCHAR(50);
DECLARE @ApprovedYearName NVARCHAR(50);
DECLARE @ActualYearName NVARCHAR(50);

DECLARE @SQL NVARCHAR(MAX);
DECLARE @ReportType NVARCHAR(50) = @RType;

------------------------------------------------------------
-- Get base year (Estimated)
------------------------------------------------------------
SELECT @Year = [Year]
FROM FiscalYears
WHERE Id = @FYId;

SET @EstimatedYear = @Year;
SET @ApprovedYear  = @Year - 1;
SET @ActualYear    = @Year - 2;

------------------------------------------------------------
-- Get year names
------------------------------------------------------------
SELECT @EstimatedYearName = YearName FROM FiscalYears WHERE [Year] = @EstimatedYear;
SELECT @ApprovedYearName  = YearName FROM FiscalYears WHERE [Year] = @ApprovedYear;
SELECT @ActualYearName    = YearName FROM FiscalYears WHERE [Year] = @ActualYear;

------------------------------------------------------------
-- Start building SELECT
------------------------------------------------------------
SET @SQL = N'
SELECT
    COA.Code AS [iBAS Code],
    COA.Name AS [iBAS Name],
    s.Code   AS [Sabre Code],
    s.Name   AS [Sabre Name],

    -- Estimated (Base Year)
    SUM(CASE 
            WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + ' 
             AND c.BudgetType = ''Estimated'' 
            THEN cd.Yearly ELSE 0 
        END) AS [Estimated(' + @EstimatedYearName + ')],

    -- Revised (Previous Year)
    SUM(CASE 
            WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
             AND c.BudgetType = ''Revised'' 
            THEN cd.Yearly ELSE 0 
        END) AS [Revised(' + @ApprovedYearName + ')],

    -- Approved (Previous Year)
    SUM(CASE 
            WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
             AND c.BudgetType = ''Approved'' 
            THEN cd.Yearly ELSE 0 
        END) AS [Approved(' + @ApprovedYearName + ')],

    -- Actual Audited (Two Years Back)
    SUM(CASE 
            WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + ' 
             AND c.BudgetType = ''Actual_Audited'' 
            THEN cd.Yearly ELSE 0 
        END) AS [Actual Audited(' + @ActualYearName + ')]';

------------------------------------------------------------
-- Conditionally add 1st / 2nd 6 months columns
------------------------------------------------------------
IF @ReportType <> '2nd_6months_actual'
BEGIN
    SET @SQL += ',
    SUM(CASE 
            WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
             AND c.BudgetType = ''1st_6months_actual'' 
            THEN cd.Yearly ELSE 0 
        END) AS [1st 6 Months Actual(' + @ApprovedYearName + ')]';
END;

IF @ReportType <> '1st_6months_actual'
BEGIN
    SET @SQL += ',
    SUM(CASE 
            WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
             AND c.BudgetType = ''2nd_6months_actual'' 
            THEN cd.Yearly ELSE 0 
        END) AS [2nd 6 Months Actual(' + @ApprovedYearName + ')]';
END;


SET @SQL += ',

-- Estimated %
CASE 
    WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                  AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) = 0
    THEN 0
    ELSE ROUND(
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Estimated'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        /
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        * 100, 2)
END AS [Estimated %],

-- Revised %
CASE 
    WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                  AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) = 0
    THEN 0
    ELSE ROUND(
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Revised'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        /
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        * 100, 2)
END AS [Revised %],

-- Actual Audited %
CASE 
    WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                  AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) = 0
    THEN 0
    ELSE ROUND(
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Actual_Audited'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        /
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        * 100, 2)
END AS [Actual Audited %]';


IF @ReportType <> '2nd_6months_actual'
BEGIN
    SET @SQL += ',
CASE 
    WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                  AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) = 0
    THEN 0
    ELSE ROUND(
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''1st_6months_actual'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        /
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        * 100, 2)
END AS [1st 6 Months Actual %]';
END;

IF @ReportType <> '1st_6months_actual'
BEGIN
    SET @SQL += ',
CASE 
    WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                  AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) = 0
    THEN 0
    ELSE ROUND(
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        /
        CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                      AND c.BudgetType = ''Approved'' THEN cd.Yearly ELSE 0 END) AS DECIMAL(18,2))
        * 100, 2)
END AS [2nd 6 Months Actual %]';
END;


SET @SQL += '
FROM BudgetHeaders c
INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
INNER JOIN FiscalYears FY    ON c.FiscalYearId = FY.Id
INNER JOIN Sabres s          ON cd.SabreId = s.Id
INNER JOIN COAs COA          ON COA.Id = s.COAId
WHERE c.BudgetType IN
(
    ''Estimated'',
    ''Revised'',
    ''Approved'',
    ''1st_6months_actual'',
    ''2nd_6months_actual'',
    ''Actual_Audited''
)
AND FY.[Year] IN (' 
    + CAST(@ActualYear AS NVARCHAR) + ',' 
    + CAST(@ApprovedYear AS NVARCHAR) + ',' 
    + CAST(@EstimatedYear AS NVARCHAR) + ')';

IF @BranchId IS NOT NULL
BEGIN
    SET @SQL += ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR);
END;

SET @SQL += '
GROUP BY
    s.Code, s.Name,
    COA.Code, COA.Name
ORDER BY s.Code;';


EXEC sp_executesql @SQL;

";

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand.Parameters.AddWithValue("@FYId", vm.YearId);

                if (!string.IsNullOrEmpty(vm.BranchId))
                    adapter.SelectCommand.Parameters.AddWithValue("@BId", vm.BranchId);

                if (!string.IsNullOrEmpty(vm.ReportType))
                    adapter.SelectCommand.Parameters.AddWithValue("@RType", vm.ReportType);

                adapter.Fill(dt);

                var list = new List<Dictionary<string, object>>();

                foreach (DataRow row in dt.Rows)
                {
                    var dict = new Dictionary<string, object>();
                    foreach (DataColumn col in dt.Columns)
                    {
                        dict[col.ColumnName] = row[col];
                    }
                    list.Add(dict);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = list;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> BudgetTransferHeader(BudgetHeaderVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";
                int count = 0;

                string checkQuery = @"
SELECT COUNT(Id) FROM BudgetHeaders WHERE BranchId = @BranchId 
AND FiscalYearId = @FiscalYearId 
AND BudgetSetNo = @BudgetSetNo 
AND BudgetType = @BudgetType 
--AND CreatedBy = @CreatedBy 

";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@FiscalYearId", SqlDbType.NVarChar).Value = objMaster.FiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                //checkCommand.Parameters.Add("@CreatedBy", SqlDbType.NVarChar).Value = objMaster.CreatedBy;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count <= 0)
                {
                    throw new Exception("From Budget Not Found!");
                }


                checkQuery = @"
SELECT COUNT(Id) FROM BudgetHeaders WHERE BranchId = @BranchId 
AND FiscalYearId = @GLFiscalYearId 
AND BudgetSetNo = @BudgetSetNo 
AND BudgetType = @BudgetType 
AND CreatedBy = @CreatedBy 

";
                checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.ToFiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.ToBudgetType;
                checkCommand.Parameters.Add("@CreatedBy", SqlDbType.NVarChar).Value = objMaster.CreatedBy;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    throw new Exception("Already Exists!");
                }

                sqlText = @"
INSERT INTO BudgetHeaders (

 CompanyId
,BranchId
,FiscalYearId
,Code
,TransactionDate
,IsPost
,Remarks
,BudgetSetNo
,BudgetType
,CreatedBy
,CreatedOn
,CreatedFrom
,TransactionType            
) VALUES 
(
 @CompanyId
,@BranchId
,@GLFiscalYearId
,@Code
,@TransactionDate
,@IsPost
,@Remarks
,@BudgetSetNo
,@BudgetType
,@CreatedBy
,@CreatedOn
,@CreatedFrom
,@TransactionType

 ) SELECT SCOPE_IDENTITY() ";

                SqlCommand command = new SqlCommand(sqlText, conn, transaction);

                command.Parameters.Add("@Code", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.Code) ? (object)DBNull.Value : objMaster.Code.Trim();
                command.Parameters.Add("@TransactionDate", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.TransactionDate) ? (object)DBNull.Value : objMaster.TransactionDate.Trim();
                command.Parameters.Add("@GLFiscalYearId", SqlDbType.NChar).Value = objMaster.ToFiscalYearId;
                command.Parameters.Add("@Remarks", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.Remarks) ? (object)DBNull.Value : objMaster.Remarks.Trim();
                command.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                command.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.ToBudgetType) ? (object)DBNull.Value : objMaster.ToBudgetType.Trim();
                command.Parameters.Add("@CreatedBy", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.CreatedBy) ? (object)DBNull.Value : objMaster.CreatedBy.Trim();
                command.Parameters.Add("@CreatedFrom", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.CreatedFrom) ? (object)DBNull.Value : objMaster.CreatedFrom.Trim();
                command.Parameters.Add("@CreatedOn", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.CreatedOn.ToString()) ? (object)DBNull.Value : objMaster.CreatedOn.ToString();
                command.Parameters.Add("@BranchId", SqlDbType.Int).Value = objMaster.BranchId;
                command.Parameters.Add("@CompanyId", SqlDbType.Int).Value = objMaster.CompanyId;
                command.Parameters.Add("@IsPost", SqlDbType.NChar).Value = "N";
                command.Parameters.Add("@TransactionType", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.TransactionType) ? (object)DBNull.Value : objMaster.TransactionType.Trim();

                objMaster.Id = Convert.ToInt32(command.ExecuteScalar());

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                result.Id = objMaster.Id.ToString();
                result.DataVM = objMaster;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> BudgetTransferDetails(BudgetHeaderVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";

                sqlText = @" 

INSERT INTO BudgetDetails (
 BudgetHeaderId
,SabreId
,InputTotal
,M1
,M2
,M3
,M4
,M5
,M6
,M7
,M8
,M9
,M10
,M11
,M12
,Q1
,Q2
,Q3
,Q4
,H1
,H2
,Yearly
,IsPost
          
)
select 
 @BudgetHeaderId
,cd.SabreId
,sum(cd.InputTotal)
,sum(cd.M1)
,sum(cd.M2)
,sum(cd.M3)
,sum(cd.M4)
,sum(cd.M5)
,sum(cd.M6)
,sum(cd.M7)
,sum(cd.M8)
,sum(cd.M9)
,sum(cd.M10)
,sum(cd.M11)
,sum(cd.M12)
,sum(cd.Q1)
,sum(cd.Q2)
,sum(cd.Q3)
,sum(cd.Q4)
,sum(cd.H1)
,sum(cd.H2)
,sum(cd.Yearly)
,'N'
from BudgetDetails cd
left outer join BudgetHeaders s on s.Id =cd.BudgetHeaderId 
where 1=1
and s.FiscalYearId=@FiscalYearId
and s.BudgetSetNo=@BudgetSetNo
and s.BudgetType=@BudgetType

group by 
 cd.SabreId

";

                SqlCommand commands = new SqlCommand(sqlText, conn, transaction);

                commands.Parameters.Add("@BudgetHeaderId", SqlDbType.Int).Value = objMaster.Id;
                commands.Parameters.Add("@FiscalYearId", SqlDbType.Int).Value = objMaster.FiscalYearId;
                commands.Parameters.Add("@BudgetSetNo", SqlDbType.Int).Value = objMaster.BudgetSetNo;
                commands.Parameters.Add("@BudgetType", SqlDbType.NChar).Value = objMaster.BudgetType;
                commands.ExecuteNonQuery();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> BudgetLoadFinalReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"


DECLARE @BranchId INT = @BId;
DECLARE @Year INT;
DECLARE @EstimatedYear INT;
DECLARE @ApprovedYear INT;
DECLARE @ActualYear INT;

DECLARE @SQL NVARCHAR(MAX);
DECLARE @ReportType NVARCHAR(50) = @RType;

SELECT @Year = [Year]
FROM FiscalYears
WHERE Id = @FYId;

SET @EstimatedYear = @Year;
SET @ApprovedYear  = @Year - 1;
SET @ActualYear    = @Year - 2;

SET @SQL = N'
SELECT
    COA.Code AS [iBAS Code],
    COA.Name AS [iBAS Name],
    s.Code   AS [Sabre Code],
    s.Name   AS [Sabre Name],
	cg.Name AS  [iBAS Group],

    SUM(CASE 
        WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + ' 
         AND c.BudgetType = ''Estimated'' 
        THEN cd.Yearly ELSE 0 END) AS Estimated,

    SUM(CASE 
        WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
         AND c.BudgetType = ''Revised'' 
        THEN cd.Yearly ELSE 0 END) AS Revised,

    SUM(CASE 
        WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
         AND c.BudgetType = ''Approved'' 
        THEN cd.Yearly ELSE 0 END) AS Approved,

    SUM(CASE 
        WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + ' 
         AND c.BudgetType = ''Actual_Audited'' 
        THEN cd.Yearly ELSE 0 END) AS [Actual Audited]';

IF @ReportType <> '2nd_6months_actual'
BEGIN
    SET @SQL += ',
    SUM(CASE 
        WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
         AND c.BudgetType = ''1st_6months_actual'' 
        THEN cd.Yearly ELSE 0 END) AS [1st 6 Months Actual]';
END;

IF @ReportType <> '1st_6months_actual'
BEGIN
    SET @SQL += ',
    SUM(CASE 
        WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
         AND c.BudgetType = ''2nd_6months_actual'' 
        THEN cd.Yearly ELSE 0 END) AS [2nd 6 Months Actual]';
END;

SET @SQL += '
FROM BudgetHeaders c
INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
INNER JOIN Sabres s ON cd.SabreId = s.Id
INNER JOIN COAs COA ON COA.Id = s.COAId
INNER JOIN COAGroups cg ON cg.Id = COA.COAGroupId
WHERE c.BudgetType IN
(
    ''Estimated'',
    ''Revised'',
    ''Approved'',
    ''1st_6months_actual'',
    ''2nd_6months_actual'',
    ''Actual_Audited''
)
AND FY.[Year] IN (' 
    + CAST(@ActualYear AS NVARCHAR) + ',' 
    + CAST(@ApprovedYear AS NVARCHAR) + ',' 
    + CAST(@EstimatedYear AS NVARCHAR) + ')';

IF @BranchId IS NOT NULL
BEGIN
    SET @SQL += ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR);
END;

SET @SQL += '
GROUP BY
    s.Code, s.Name,
    COA.Code, COA.Name ,cg.Name
ORDER BY s.Code;';

EXEC sp_executesql @SQL;


";

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand.Parameters.AddWithValue("@FYId", vm.YearId);
                adapter.SelectCommand.Parameters.AddWithValue("@BId", vm.BranchId);
                adapter.SelectCommand.Parameters.AddWithValue("@RType", vm.ReportType);

                adapter.Fill(dt);

                var list = new List<Dictionary<string, object>>();
                foreach (DataRow row in dt.Rows)
                {
                    var dict = new Dictionary<string, object>();
                    foreach (DataColumn col in dt.Columns)
                    {
                        dict[col.ColumnName] = row[col];
                    }
                    list.Add(dict);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = list;
                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        #region Budget All

        public async Task<ResultVM> GetGridDataBudgetAll(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };


            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<BudgetHeaderVM>();

                // Define your SQL query string
                string sqlQuery = @"
                -- Count query
           SELECT COUNT(DISTINCT M.FiscalYearId) AS TotalCount
            FROM BudgetHeaders M
            WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                -- Data query with pagination and sorting
                SELECT * 
                FROM (
                    SELECT 
                    ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "fy.YearName DESC") + @") AS rowindex,
   
                        ISNULL(M.CompanyId, 0) AS CompanyId,
                        ISNULL(M.BranchId, 0) AS BranchId,
                        
                        M.FiscalYearId,
                        ISNULL(fy.YearName, '') AS YearName,
                        ISNULL(M.BudgetType, '') AS BudgetType

                         FROM BudgetHeaders M
                         LEFT JOIN FiscalYears fy ON fy.Id = M.FiscalYearId
                         WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                GROUP BY
        
        M.CompanyId,
        M.BranchId,
        M.FiscalYearId,
        fy.YearName,
        M.BudgetType
) AS a
WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
order by a.FiscalYearId desc
            ";

                // Execute the query and get data
                data = KendoGrid<BudgetHeaderVM>.GetTransactionalGridData_CMD(options, sqlQuery, "M.Id", conditionalFields, conditionalValues);

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

        public async Task<ResultVM> BudgetAllDetailsList( CommonVM vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }

                string query = @"

 SELECT 
D.SabreId,
COAs.Code   AS iBASCode,
COAs.Name   AS iBASName,
S.Code      AS SabreCode,
S.Name      AS SabreName,
SUM(D.InputTotal) AS InputTotal,
SUM(D.M1) AS M1,
SUM(D.M2) AS M2,
SUM(D.M3) AS M3,
SUM(D.M4) AS M4,
SUM(D.M5) AS M5,
SUM(D.M6) AS M6,
SUM(D.M7) AS M7,
SUM(D.M8) AS M8,
SUM(D.M9) AS M9,
SUM(D.M10) AS M10,
SUM(D.M11) AS M11,
SUM(D.M12) AS M12,
SUM(D.Q1) AS Q1,
SUM(D.Q2) AS Q2,
SUM(D.Q3) AS Q3,
SUM(D.Q4) AS Q4,
SUM(D.H1) AS H1,
SUM(D.H2) AS H2,
SUM(D.Yearly) AS Yearly
FROM BudgetDetails D
LEFT JOIN BudgetHeaders bh ON bh.Id = D.BudgetHeaderId
LEFT JOIN Sabres S ON S.Id = D.SabreId
LEFT JOIN COAs ON COAs.Id = S.COAId
INNER JOIN DepartmentSabres DS ON DS.SabreId = S.Id
INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId

WHERE 1 = 1
and bh.FiscalYearId = @FiscalYearId
and bh.BudgetType = @BudgetType

GROUP BY 
D.SabreId,
COAs.Code,
COAs.Name,
S.Code,
S.Name
ORDER BY 
COAs.Code, S.Code

           ";

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                if (!string.IsNullOrEmpty(vm.FiscalYearId))
                    objComm.SelectCommand.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId);

                if (!string.IsNullOrEmpty(vm.BudgetType))
                    objComm.SelectCommand.Parameters.AddWithValue("@BudgetType", vm.BudgetType);

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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> BudgetListAll(string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction, CommonVM vm = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                string query = @"
SELECT 
    ISNULL(M.CompanyId, 0) AS CompanyId,
    ISNULL(M.BranchId, 0) AS BranchId,
    ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
    ISNULL(M.BudgetType, '') AS BudgetType

FROM BudgetHeaders M

WHERE 1 = 1
                ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                query += @"
GROUP BY 
    M.CompanyId,
    M.BranchId,
    M.FiscalYearId,
    M.BudgetType
";

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new BudgetHeaderVM
                {
                    Id = 0,
                    CompanyId = row.Field<int>("CompanyId"),
                    BranchId = row.Field<int>("BranchId"),
                    Code = "",
                    FiscalYearId = row.Field<int>("FiscalYearId"),
                    BudgetType = row.Field<string>("BudgetType"),
                    TransactionDate = "",  // Format if necessary
                    IsPost = "",
                    LastUpdateBy = "",
                    LastUpdateOn = "",  // Format if necessary
                    LastUpdateFrom = "",
                    PostedBy = "",
                    PostedOn = "",  // Format if necessary
                    PostedFrom = ""
                }).ToList();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }


        #endregion


    }

}
