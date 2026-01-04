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
                    cmd.Parameters.AddWithValue("@IsPost",false);
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

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues
         , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
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

//        public async Task<ResultVM> ListEdit(string[] conditionalFields, string[] conditionalValues
//   , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
//        {
//            DataTable dataTable = new DataTable();
//            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

//            try
//            {
//                string query = @"
//SELECT
//   Sabres.Id          AS SabreId,
//    COAs.Code          AS iBASCode,
//    COAs.Name          AS iBASName,
//    Sabres.Code        AS SabreCode,
//    Sabres.[Name]      AS SabreName,
//	bd.InputTotal      As InputTotal,

//    ISNULL(bh.Id, 0 AS Id,  
//    ISNULL(bh.CompanyId, 0) AS CompanyId,
//    ISNULL(bh.BranchId, 0) AS BranchId,
//    ISNULL(bh.Code, '') AS Code,
//    ISNULL(bh.FiscalYearId, 0) AS FiscalYearId,
//    ISNULL(bh.BudgetType, '') AS BudgetType,
//    ISNULL(bh.TransactionDate, '1900-01-01') AS TransactionDate,
//    ISNULL(bh.IsPost, '') AS IsPost,
//    CASE WHEN ISNULL(bh.IsPost, '') = '1'  THEN 'Posted' ELSE 'Not Posted' END AS Status,
//    ISNULL(bh.LastUpdateBy, '') AS LastUpdateBy,
//    ISNULL(bh.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
//    ISNULL(bh.LastUpdateFrom, '') AS LastUpdateFrom,
//    ISNULL(bh.PostedBy, '') AS PostedBy,
//    ISNULL(bh.PostedOn, '1900-01-01') AS PostedOn,
//    ISNULL(bh.PostedFrom, '') AS PostedFrom,  
//    ISNULL(bh.CreatedBy, '') AS CreatedBy,
//    ISNULL(bh.CreatedOn, '1900-01-01') AS CreatedOn,
//    ISNULL(bh.CreatedFrom, '') AS CreatedFrom


//FROM Sabres
//LEFT OUTER JOIN COAs ON COAs.Id = Sabres.COAId
//INNER JOIN DepartmentSabres DS ON DS.SabreId = Sabres.Id
//INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId
//INNER JOIN BudgetDetails bd ON bd.SabreId = Sabres.Id
//INNER JOIN BudgetHeaders bh ON bh.Id = bd.BudgetHeaderId
//WHERE 1 = 1
//                ";

//                if (vm != null && !string.IsNullOrEmpty(vm.Id))
//                {
//                    query += " AND M.Id = @Id ";
//                }

//                // Apply additional conditions
//                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

//                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

//                // SET additional conditions param
//                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

//                if (vm != null && !string.IsNullOrEmpty(vm.Id))
//                {
//                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
//                }

//                objComm.Fill(dataTable);

//                var modelList = dataTable.AsEnumerable().Select(row => new BudgetHeaderVM
//                {
//                    CompanyId = row.Field<int>("CompanyId"),
//                    BranchId = row.Field<int>("BranchId"),
//                    Code = row.Field<string>("Code"),
//                    FiscalYearId = row.Field<int>("FiscalYearId"),
//                    BudgetType = row.Field<string>("BudgetType"),
//                    TransactionDate = row.Field<DateTime?>("TransactionDate")?.ToString("yyyy-MM-dd") ?? "",  // Format if necessary
//                    IsPost = row.Field<string>("IsPost"),
//                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
//                    LastUpdateOn = row.Field<DateTime?>("LastUpdateOn")?.ToString("yyyy-MM-dd") ?? "",  // Format if necessary
//                    LastUpdateFrom = row.Field<string>("LastUpdateFrom"),
//                    PostedBy = row.Field<string>("PostedBy"),
//                    PostedOn = row.Field<DateTime?>("PostedOn")?.ToString("yyyy-MM-dd") ?? "",  // Format if necessary
//                    PostedFrom = row.Field<string>("PostedFrom")
//                }).ToList();


//                result.Status = MessageModel.Success;
//                result.Message = MessageModel.RetrievedSuccess;
//                result.DataVM = modelList;
//                return result;
//            }
//            catch (Exception ex)
//            {
//                result.Message = ex.Message;
//                result.ExMessage = ex.Message;
//                return result;
//            }
//        }




        public async Task<ResultVM> GetBudgetDataForDetailsNew( GridOptions options,string[] conditionalFields,string[] conditionalValues,SqlConnection conn,SqlTransaction transaction)
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
                      ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")"
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
                0                  AS InputTotal

            FROM Sabres
            LEFT OUTER JOIN COAs ON COAs.Id = Sabres.COAId
            INNER JOIN DepartmentSabres DS ON DS.SabreId = Sabres.Id
            INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId
            WHERE 1 = 1
              {(options.filter.Filters.Count > 0
                          ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")"
                          : "")}
              {ApplyConditions("", conditionalFields, conditionalValues, false)}
        ) t
        WHERE t.rowindex > @skip
          AND (@take = 0 OR t.rowindex <= @take);
        ";

                var data = KendoGrid<BudgetHeaderVM>
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
                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

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

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            ";

                // Execute the query and get data
                data = KendoGrid<BudgetHeaderVM>.GetGridData_CMD(options, sqlQuery, "M.Id");

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
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                var data = new GridEntity<BudgetDetailVM>();

                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT D.Id) AS totalcount
        FROM BudgetDetails D
     
        WHERE D.BudgetHeaderId = @masterId
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetDetailVM>.FilterCondition(options.filter) + ")" : "") + @"

                -- Data query with pagination and sorting
                SELECT *
                FROM (
                    SELECT
                        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "D.Id DESC ") + @") AS rowindex,
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
           FROM BudgetDetails D
           Where D.BudgetHeaderId = @masterId


                    -- Add the filter condition
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BudgetDetailVM>.FilterCondition(options.filter) + ")" : "") + @"
                ) AS a
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

    }

}
