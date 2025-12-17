using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;


namespace ShampanBFRS.Repository.Ceiling
{
    public class CeilingRepository : CommonRepository
    {

        public async Task<ResultVM> Insert(CeilingVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";
                int count = 0;

                string checkQuery = @"
SELECT COUNT(Id) FROM Ceilings WHERE BranchId = @BranchId 
AND GLFiscalYearId = @GLFiscalYearId 
AND BudgetSetNo = @BudgetSetNo 
AND BudgetType = @BudgetType 
AND CreatedBy = @CreatedBy 

";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.GLFiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                checkCommand.Parameters.Add("@CreatedBy", SqlDbType.NVarChar).Value = objMaster.CreatedBy;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    throw new Exception("Already Exists!");
                }

                sqlText = @" INSERT INTO Ceilings (

 CompanyId
,BranchId
,GLFiscalYearId
,Code
,TransactionDate
,IsPost
,Remarks
,BudgetSetNo
,BudgetType
,IsActive
,IsArchive
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
,@IsActive
,@IsArchive
,@CreatedBy
,@CreatedOn
,@CreatedFrom
,@TransactionType

 ) SELECT SCOPE_IDENTITY() ";

                SqlCommand command = new SqlCommand(sqlText, conn, transaction);

                command.Parameters.Add("@Code", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.Code) ? (object)DBNull.Value : objMaster.Code.Trim();
                command.Parameters.Add("@TransactionDate", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.TransactionDate) ? (object)DBNull.Value : objMaster.TransactionDate.Trim();
                command.Parameters.Add("@GLFiscalYearId", SqlDbType.NChar).Value = objMaster.GLFiscalYearId;
                command.Parameters.Add("@IsActive", SqlDbType.Bit).Value = objMaster.IsActive;
                command.Parameters.Add("@IsArchive", SqlDbType.Bit).Value = objMaster.IsArchive;
                command.Parameters.Add("@Remarks", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.Remarks) ? (object)DBNull.Value : objMaster.Remarks.Trim();
                command.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                command.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.BudgetType) ? (object)DBNull.Value : objMaster.BudgetType.Trim();
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

        public async Task<ResultVM> Update(CeilingVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", Id = objMaster.Id.ToString(), DataVM = objMaster };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";
                int count = 0;

                string checkQuery = "SELECT COUNT(Id) FROM Ceilings WHERE BranchId = @BranchId AND GLFiscalYearId = @GLFiscalYearId " +
                    "AND BudgetSetNo = @BudgetSetNo AND BudgetType = @BudgetType AND Id != @Id ";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.GLFiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                checkCommand.Parameters.Add("@Id", SqlDbType.NVarChar).Value = objMaster.Id;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    throw new Exception("Already Exists!");
                }

                string query = @"  update Ceilings set 

 GLFiscalYearId=@GLFiscalYearId
,TransactionDate=@TransactionDate
,Remarks=@Remarks
,BudgetSetNo=@BudgetSetNo
,BudgetType=@BudgetType
,IsActive=@IsActive
,LastUpdateBy=@LastUpdateBy
,LastUpdateOn=@LastUpdateOn
,LastUpdateFrom=@LastUpdateFrom

where  Id=@Id  ";

                SqlCommand command = new SqlCommand(query, conn, transaction);

                command.Parameters.Add("@TransactionDate", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.TransactionDate) ? (object)DBNull.Value : objMaster.TransactionDate.Trim();
                command.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.GLFiscalYearId;
                command.Parameters.Add("@Remarks", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.Remarks) ? (object)DBNull.Value : objMaster.Remarks.Trim();
                command.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                command.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.BudgetType) ? (object)DBNull.Value : objMaster.BudgetType.Trim();

                command.Parameters.Add("@IsActive", SqlDbType.Bit).Value = objMaster.IsActive;
                command.Parameters.Add("@LastUpdateBy", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.LastUpdateBy) ? (object)DBNull.Value : objMaster.LastUpdateBy.Trim();
                command.Parameters.Add("@LastUpdateOn", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.LastModifiedOn.ToString()) ? (object)DBNull.Value : objMaster.LastModifiedOn.ToString();
                command.Parameters.Add("@LastUpdateFrom", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.LastUpdateFrom) ? (object)DBNull.Value : objMaster.LastUpdateFrom.Trim();

                command.Parameters.Add("@Id", SqlDbType.Int).Value = objMaster.Id;

                int rowcount = command.ExecuteNonQuery();

                if (rowcount > 0)
                {
                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.UpdateSuccess;
                }
                else
                {
                    throw new Exception("No rows were updated.");
                }

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

        public async Task<ResultVM> InsertDetails(CeilingDetailVM ObjDetail, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";

                sqlText = @" INSERT INTO CeilingDetails (

 
 GLCeilingId
,AccountId
,GLFiscalYearDetailId
,PeriodSl
,PeriodStart
,PeriodEnd
,Amount
,IsPost
          
) VALUES (

 @GLCeilingId
,@AccountId
,@GLFiscalYearDetailId
,@PeriodSl
,@PeriodStart
,@PeriodEnd
,@Amount
,@IsPost

)SELECT SCOPE_IDENTITY() ";

                SqlCommand commands = new SqlCommand(sqlText, conn, transaction);

                commands.Parameters.Add("@GLCeilingId", SqlDbType.Int).Value = ObjDetail.GLCeilingId;
                commands.Parameters.Add("@AccountId", SqlDbType.Int).Value = ObjDetail.AccountId;
                commands.Parameters.Add("@GLFiscalYearDetailId", SqlDbType.Int).Value = ObjDetail.GLFiscalYearDetailId;
                commands.Parameters.Add("@PeriodSl", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(ObjDetail.PeriodSl) ? (object)DBNull.Value : ObjDetail.PeriodSl.Trim();
                commands.Parameters.Add("@PeriodStart", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(ObjDetail.PeriodStart) ? (object)DBNull.Value : ObjDetail.PeriodStart.Trim();
                commands.Parameters.Add("@PeriodEnd", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(ObjDetail.PeriodEnd) ? (object)DBNull.Value : ObjDetail.PeriodEnd.Trim();
                commands.Parameters.Add("@Amount", SqlDbType.Decimal).Value = ObjDetail.Amount;
                commands.Parameters.Add("@IsPost", SqlDbType.NVarChar).Value = "N";

                ObjDetail.Id = Convert.ToInt32(commands.ExecuteScalar());

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                result.Id = ObjDetail.Id.ToString();
                result.DataVM = ObjDetail;

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

        public async Task<ResultVM> xxxxGetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                var data = new GridEntity<CeilingVM>();

                string sqlQuery = @"
                -- Count
                SELECT COUNT(DISTINCT c.Id) AS totalcount
                FROM Ceilings c
                WHERE c.IsArchive != 1
                " + (options.filter.Filters.Count > 0
                        ? " AND (" + GridQueryBuilder<CeilingVM>.FilterCondition(options.filter) + ")"
                        : "") + @"

                -- Data
                SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? "c." + options.sort[0].field + " " + options.sort[0].dir
                            : "c.Id DESC") + @") AS rowindex,
                           ISNULL(c.Id,0) AS Id
                           ,ISNULL(c.CompanyId,0) AS CompanyId
                           ,ISNULL(c.BranchId,0) AS BranchId
                           ,ISNULL(c.GLFiscalYearId,0) AS FiscalYearId
                           ,ISNULL(c.BudgetSetNo,0) AS BudgetSetNo
                           ,ISNULL(c.BudgetType,'') AS BudgetType
                           ,ISNULL(c.Code,0) AS Code
                           ,ISNULL(FORMAT(c.TransactionDate,'yyyy-MM-dd HH:mm'),'') AS TransactionDate
                           ,ISNULL(c.IsPost ,'') AS IsPost
                           ,ISNULL(c.Remarks,'') AS Remarks
                           ,ISNULL(c.IsActive ,0) AS IsActive
                           ,ISNULL(c.IsArchive,0) AS IsArchive
                           ,ISNULL(c.CreatedBy,'') AS CreatedBy
                           ,ISNULL(FORMAT(c.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn
                           ,ISNULL(c.TransactionType,'') AS TransactionType
                             FROM Ceilings c
                    WHERE c.IsArchive != 1
                    " + (options.filter.Filters.Count > 0
                            ? " AND (" + GridQueryBuilder<CeilingVM>.FilterCondition(options.filter) + ")"
                            : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take=0 OR rowindex <= @take)";

                data = KendoGrid<CeilingVM>.GetGridDataQuestions_CMD(options, sqlQuery, "c.Id");

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = data;

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

        public async Task<ResultVM> GetGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                var data = new GridEntity<CeilingVM>();

                string sqlQuery = $@"
                    -- Count query
                    SELECT COUNT(DISTINCT c.Id) AS totalcount
                FROM Ceilings c
                WHERE c.IsArchive != 1
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<CeilingVM>.FilterCondition(options.filter) + ")" : "");

                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
            -- Data query with pagination and sorting
            SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? "c." + options.sort[0].field + " " + options.sort[0].dir
                            : "c.Id DESC") + @") AS rowindex,
                           ISNULL(c.Id,0) AS Id
                           ,ISNULL(c.CompanyId,0) AS CompanyId
                           ,ISNULL(c.BranchId,0) AS BranchId
                           ,ISNULL(c.GLFiscalYearId,0) AS FiscalYearId
                           ,ISNULL(fy.YearName,0) AS YearName
                           ,ISNULL(c.BudgetSetNo,0) AS BudgetSetNo
                           ,ISNULL(c.BudgetType,'') AS BudgetType
                           ,ISNULL(c.Code,0) AS Code
                           ,ISNULL(FORMAT(c.TransactionDate,'yyyy-MM-dd HH:mm'),'') AS TransactionDate
                           ,ISNULL(c.IsPost ,'') AS IsPost
                           ,ISNULL(c.Remarks,'') AS Remarks
                           ,ISNULL(c.IsActive ,0) AS IsActive
                           ,ISNULL(c.IsArchive,0) AS IsArchive
                           ,ISNULL(c.CreatedBy,'') AS CreatedBy
                           ,ISNULL(FORMAT(c.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn
                           ,ISNULL(c.TransactionType,'') AS TransactionType
                             FROM Ceilings c
                            left outer join FiscalYears fy on fy.Id = c.GLFiscalYearId
                    WHERE c.IsArchive != 1

            -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<CeilingVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
            ) AS a
            WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
        ";

                data = KendoGrid<CeilingVM>.GetTransactionalGridData_CMD(options, sqlQuery, "CO.Id", conditionalFields, conditionalValues);

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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> CeilingList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"
                SELECT 

 ISNULL(H.Id,0) Id
,ISNULL(H.Code,'') Code
,ISNULL(H.GLFiscalYearId,0) GLFiscalYearId
,ISNULL(FORMAT(H.TransactionDate,'yyyy-MM-dd'),'1900-01-01') TransactionDate
,ISNULL(H.BudgetSetNo,0) BudgetSetNo
,ISNULL(H.BudgetType,'') BudgetType
,ISNULL(H.Remarks,'') Remarks
,ISNULL(H.CreatedBy,'') CreatedBy
,ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm:ss'),'1900-01-01') CreatedOn
,ISNULL(H.CreatedFrom,'') CreatedFrom
,ISNULL(H.LastUpdateBy,'') LastUpdateBy
,ISNULL(FORMAT(H.LastUpdateOn,'yyyy-MM-dd HH:mm:ss'),'1900-01-01') LastUpdateOn
,ISNULL(H.LastUpdateFrom,'') LastUpdateFrom
,ISNULL(H.PostedBy,'') PostedBy
,ISNULL(FORMAT(H.PostedOn,'yyyy-MM-dd HH:mm:ss'),'1900-01-01') PostedOn
,ISNULL(H.PostedFrom,'') PostedFrom
,ISNULL(H.IsActive,0) IsActive
,ISNULL(H.IsArchive,0) IsArchive
,ISNULL(H.BranchId,0) BranchId
,ISNULL(H.CompanyId,0) CompanyId
,ISNULL(H.IsPost,'N') IsPost
,ISNULL(H.IsApproveFinal,0) IsApproveFinal

FROM Ceilings H

WHERE 1=1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND H.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new CeilingVM
                {
                    Id = row.Field<int>("Id"),
                    Code = row.Field<string>("Code"),
                    GLFiscalYearId = row.Field<int>("GLFiscalYearId"),
                    TransactionDate = row.Field<string>("TransactionDate"),
                    BudgetSetNo = row.Field<int>("BudgetSetNo"),
                    BudgetType = row.Field<string>("BudgetType"),
                    Remarks = row.Field<string>("Remarks"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    CreatedOn = row.Field<string>("CreatedOn"),
                    CreatedFrom = row.Field<string>("CreatedFrom"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastModifiedOn = row.Field<string>("LastUpdateOn"),
                    LastUpdateFrom = row.Field<string>("LastUpdateFrom"),
                    PostedBy = row.Field<string>("PostedBy"),
                    PostedOn = row.Field<string>("PostedOn"),
                    PostedFrom = row.Field<string>("PostedFrom"),
                    IsActive = row.Field<bool>("IsActive"),
                    IsArchive = row.Field<bool>("IsArchive"),
                    BranchId = row.Field<int>("BranchId"),
                    CompanyId = row.Field<int>("CompanyId"),
                    IsPost = row.Field<string>("IsPost"),
                    IsApproveFinal = row.Field<bool>("IsApproveFinal")
                }).ToList();

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

        public async Task<ResultVM> DeleteDetails(CeilingVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", Id = vm.Id.ToString() };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = $@"
                delete CeilingDetails where GLCeilingId=@GLCeilingId";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@GLCeilingId", vm.Id);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.DeleteSuccess;
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
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> xxxxxxBudgetFinalReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues,
             SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"
--DECLARE @FYId INT = FYId;       
--DECLARE @BranchId INT = 1; 
DECLARE @Year INT;

-- Get selected Fiscal Year
SELECT @Year = [Year]
FROM FiscalYears
WHERE Id = @FYId;

SELECT
    COA.Code AS COACode,
    COA.Name AS COAName,
    s.Code AS SabreCode,
    s.Name AS SabreName,

    -- Next Year: Estimated
    SUM(CASE WHEN FY.[Year] = @Year + 1 AND c.BudgetType = 'Estimated' THEN cd.Amount ELSE 0 END) AS Estimated,

    -- Current Year: Revised
    SUM(CASE WHEN FY.[Year] = @Year AND c.BudgetType = 'Revised' THEN cd.Amount ELSE 0 END) AS Revised,

    -- Current Year: Approved
    SUM(CASE WHEN FY.[Year] = @Year AND c.BudgetType = 'Approved' THEN cd.Amount ELSE 0 END) AS Approved,

    -- Previous Year: Actual (Audited)
    SUM(CASE WHEN FY.[Year] = @Year - 1 AND c.BudgetType = 'Actual_Audited' THEN cd.Amount ELSE 0 END) AS ActualAudited,

    -- Current Year: 1st_6months_actual
    SUM(CASE WHEN FY.[Year] = @Year AND c.BudgetType = '1st_6months_actual' THEN cd.Amount ELSE 0 END) AS First6MonthsActual

FROM Ceilings c
INNER JOIN CeilingDetails cd ON c.Id = cd.GLCeilingId
INNER JOIN FiscalYears FY ON c.GLFiscalYearId = FY.Id
INNER JOIN Sabres s ON cd.AccountId = s.Id
INNER JOIN COAs COA ON COA.Id = s.COAId
WHERE c.BudgetType IN ('Revised','1st_6months_actual','Approved','Actual_Audited','Estimated')
  AND FY.[Year] IN (@Year - 1, @Year, @Year + 1)
    


";

                if (vm != null && !string.IsNullOrEmpty(vm.BranchId))
                    query += " AND c.BranchId = @BranchId ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                query += @"  
GROUP BY s.Code, s.Name, COA.Code, COA.Name
ORDER BY s.Code;"
;

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                adapter.SelectCommand.Parameters.AddWithValue("@FYId", vm.YearId);

                if (vm != null && !string.IsNullOrEmpty(vm.BranchId))
                    adapter.SelectCommand.Parameters.AddWithValue("@BranchId", vm.BranchId);

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

        public async Task<ResultVM> xxxxBudgetFinalReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"

DECLARE @Year INT;
DECLARE @CurrentYearName NVARCHAR(50);
DECLARE @PrevYearName NVARCHAR(50);
DECLARE @NextYearName NVARCHAR(50);
DECLARE @SQL NVARCHAR(MAX);

SELECT @Year = [Year] FROM FiscalYears WHERE Id = @FYId;

SELECT @CurrentYearName = YearName FROM FiscalYears WHERE [Year] = @Year;
SELECT @PrevYearName = YearName FROM FiscalYears WHERE [Year] = @Year - 1;
SELECT @NextYearName = YearName FROM FiscalYears WHERE [Year] = @Year + 1;

SET @SQL = N'
SELECT
    COA.Code AS [iBAS Code],
    COA.Name AS [iBAS Name],
    s.Code AS [Sabre Code],
    s.Name AS [Sabre Name],

    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year + 1 AS NVARCHAR) + ' AND c.BudgetType = ''Estimated'' THEN cd.Amount ELSE 0 END) AS [Estimated(' + @NextYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Revised'' THEN cd.Amount ELSE 0 END) AS [Revised(' + @CurrentYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS [Approved(' + @CurrentYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year - 1 AS NVARCHAR) + ' AND c.BudgetType = ''Actual_Audited'' THEN cd.Amount ELSE 0 END) AS [Actual Audited(' + @PrevYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''1st_6months_actual'' THEN cd.Amount ELSE 0 END) AS [1st 6 Months Actual(' + @CurrentYearName + ')]
   ,SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Amount ELSE 0 END) AS [2nd 6 Months Actual(' + @CurrentYearName + ')]

	 -- Ratios based on Approved
    ,CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) = 0 
     THEN 0
     ELSE ROUND(
         CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year + 1 AS NVARCHAR) + ' AND c.BudgetType = ''Estimated'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
         / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100
     , 2)
END AS [Estimated %], --[Estimated(' + @NextYearName + ') %]

CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) = 0 
     THEN 0
     ELSE ROUND(
         CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Revised'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
         / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100
     , 2)
END AS [Revised %], --[Revised(' + @CurrentYearName + ')%]

CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) = 0 
     THEN 0
     ELSE ROUND(
         CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year - 1 AS NVARCHAR) + ' AND c.BudgetType = ''Actual_Audited'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
         / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100
     , 2)
END AS [Actual Audited %] --[Actual Audited(' + @PrevYearName + ')%]
,CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) = 0 
     THEN 0
     ELSE ROUND(
         CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''1st_6months_actual'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
         / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100
     , 2)
END AS [1st 6 Months Actual %]
,CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) = 0 
     THEN 0
     ELSE ROUND(
         CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
         / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100
     , 2)
END AS [2nd 6 Months Actual %]

FROM Ceilings c
INNER JOIN CeilingDetails cd ON c.Id = cd.GLCeilingId
INNER JOIN FiscalYears FY ON c.GLFiscalYearId = FY.Id
INNER JOIN Sabres s ON cd.AccountId = s.Id
INNER JOIN COAs COA ON COA.Id = s.COAId
WHERE c.BudgetType IN (''Revised'',''1st_6months_actual'',''Approved'',''Actual_Audited'',''Estimated'')
  AND FY.[Year] IN (' + CAST(@Year - 1 AS NVARCHAR) + ',' + CAST(@Year AS NVARCHAR) + ',' + CAST(@Year + 1 AS NVARCHAR) + ') ';

IF (@BranchId IS NOT NULL)
    SET @SQL = @SQL + ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) + ' ';

SET @SQL = @SQL + '
GROUP BY s.Code, s.Name, COA.Code, COA.Name
ORDER BY s.Code;
';

-- Execute
EXEC sp_executesql @SQL;

";

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand.Parameters.AddWithValue("@FYId", vm.YearId);

                if (!string.IsNullOrEmpty(vm.BranchId))
                    adapter.SelectCommand.Parameters.AddWithValue("@BranchId", vm.BranchId);

                adapter.Fill(dt);

                // Conditional column removal based on ReportType
                if (vm.ReportType == "1st_6months_actual")
                {
                    // Remove columns related to the 2nd 6 months
                    dt.Columns.Remove("2nd 6 Months Actual(" + vm.YearId + ")");
                    dt.Columns.Remove("2nd 6 Months Actual %");
                }
                else if (vm.ReportType == "2nd_6months_actual")
                {
                    // Remove columns related to the 1st 6 months
                    dt.Columns.Remove("1st 6 Months Actual(" + vm.YearId + ")");
                    dt.Columns.Remove("1st 6 Months Actual %");
                }

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

                //if (vm.ShowFirstSixMonths)
                //{
                //    // Delete columns related to the 2nd 6 months
                //    dt.Columns.Remove("2nd 6 Months Actual(" + @CurrentYearName + ")");
                //    dt.Columns.Remove("2nd 6 Months Actual %");
                //}
                //else if (vm.ShowSecondSixMonths)
                //{
                //    // Delete columns related to the 1st 6 months
                //    dt.Columns.Remove("1st 6 Months Actual(" + @CurrentYearName + ")");
                //    dt.Columns.Remove("1st 6 Months Actual %");
                //}

                //var list = new List<Dictionary<string, object>>();

                //foreach (DataRow row in dt.Rows)
                //{
                //    var dict = new Dictionary<string, object>();
                //    foreach (DataColumn col in dt.Columns)
                //    {
                //        dict[col.ColumnName] = row[col];
                //    }
                //    list.Add(dict);
                //}

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

        public async Task<ResultVM> BudgetFinalReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"


DECLARE @BranchId INT=@BId;
DECLARE @Year INT;
DECLARE @CurrentYearName NVARCHAR(50);
DECLARE @PrevYearName NVARCHAR(50);
DECLARE @NextYearName NVARCHAR(50);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @ReportType NVARCHAR(50) = @RType;

-- Get fiscal year info
SELECT @Year = [Year] FROM FiscalYears WHERE Id = 7;

SELECT @CurrentYearName = YearName FROM FiscalYears WHERE [Year] = @Year;
SELECT @PrevYearName = YearName FROM FiscalYears WHERE [Year] = @Year - 1;
SELECT @NextYearName = YearName FROM FiscalYears WHERE [Year] = @Year + 1;

-- Start building dynamic SQL
SET @SQL = N'SELECT
    COA.Code AS [iBAS Code],
    COA.Name AS [iBAS Name],
    s.Code AS [Sabre Code],
    s.Name AS [Sabre Name],

    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year + 1 AS NVARCHAR) + ' AND c.BudgetType = ''Estimated'' THEN cd.Amount ELSE 0 END) AS [Estimated(' + @NextYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Revised'' THEN cd.Amount ELSE 0 END) AS [Revised(' + @CurrentYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS [Approved(' + @CurrentYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year - 1 AS NVARCHAR) + ' AND c.BudgetType = ''Actual_Audited'' THEN cd.Amount ELSE 0 END) AS [Actual Audited(' + @PrevYearName + ')]';

-- Conditionally add 1st or 2nd 6 months columns
IF @ReportType <> '2nd_6months_actual'
    SET @SQL = @SQL + ',
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''1st_6months_actual'' THEN cd.Amount ELSE 0 END) AS [1st 6 Months Actual(' + @CurrentYearName + ')]';

IF @ReportType <> '1st_6months_actual'
    SET @SQL = @SQL + ',
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Amount ELSE 0 END) AS [2nd 6 Months Actual(' + @CurrentYearName + ')]';

-- Add ratios based on Approved
SET @SQL = @SQL + ',
CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year + 1 AS NVARCHAR) + ' AND c.BudgetType = ''Estimated'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [Estimated %],

CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Revised'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [Revised %],

CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year - 1 AS NVARCHAR) + ' AND c.BudgetType = ''Actual_Audited'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [Actual Audited %]';

-- Conditionally add 1st or 2nd 6 months %
IF @ReportType <> '2nd_6months_actual'
    SET @SQL = @SQL + ',
CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''1st_6months_actual'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2))
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [1st 6 Months Actual %]';

IF @ReportType <> '1st_6months_actual'
    SET @SQL = @SQL + ',
CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2))
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [2nd 6 Months Actual %]';

-- FROM, JOINs, WHERE
SET @SQL = @SQL + '
FROM Ceilings c
INNER JOIN CeilingDetails cd ON c.Id = cd.GLCeilingId
INNER JOIN FiscalYears FY ON c.GLFiscalYearId = FY.Id
INNER JOIN Sabres s ON cd.AccountId = s.Id
INNER JOIN COAs COA ON COA.Id = s.COAId
WHERE c.BudgetType IN (''Revised'',''1st_6months_actual'',''2nd_6months_actual'',''Approved'',''Actual_Audited'',''Estimated'')
  AND FY.[Year] IN (' + CAST(@Year - 1 AS NVARCHAR) + ',' + CAST(@Year AS NVARCHAR) + ',' + CAST(@Year + 1 AS NVARCHAR) + ')';

-- BranchId filter
IF (@BranchId IS NOT NULL)
    SET @SQL = @SQL + ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR);

-- GROUP BY, ORDER BY
SET @SQL = @SQL + '
GROUP BY s.Code, s.Name, COA.Code, COA.Name
ORDER BY s.Code;';

-- Execute
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

        public async Task<ResultVM> GridDataReportType(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"


DECLARE @BranchId INT=@BId;
DECLARE @Year INT;
DECLARE @CurrentYearName NVARCHAR(50);
DECLARE @PrevYearName NVARCHAR(50);
DECLARE @NextYearName NVARCHAR(50);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @ReportType NVARCHAR(50) = @RType;

-- Get fiscal year info
SELECT @Year = [Year] FROM FiscalYears WHERE Id = 7;

SELECT @CurrentYearName = YearName FROM FiscalYears WHERE [Year] = @Year;
SELECT @PrevYearName = YearName FROM FiscalYears WHERE [Year] = @Year - 1;
SELECT @NextYearName = YearName FROM FiscalYears WHERE [Year] = @Year + 1;

-- Start building dynamic SQL
SET @SQL = N'SELECT
    COA.Code AS [iBAS Code],
    COA.Name AS [iBAS Name],
    s.Code AS [Sabre Code],
    s.Name AS [Sabre Name],

    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year + 1 AS NVARCHAR) + ' AND c.BudgetType = ''Estimated'' THEN cd.Amount ELSE 0 END) AS [Estimated(' + @NextYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Revised'' THEN cd.Amount ELSE 0 END) AS [Revised(' + @CurrentYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS [Approved(' + @CurrentYearName + ')],
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year - 1 AS NVARCHAR) + ' AND c.BudgetType = ''Actual_Audited'' THEN cd.Amount ELSE 0 END) AS [Actual Audited(' + @PrevYearName + ')]';

-- Conditionally add 1st or 2nd 6 months columns
IF @ReportType <> '2nd_6months_actual'
    SET @SQL = @SQL + ',
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''1st_6months_actual'' THEN cd.Amount ELSE 0 END) AS [1st 6 Months Actual(' + @CurrentYearName + ')]';

IF @ReportType <> '1st_6months_actual'
    SET @SQL = @SQL + ',
    SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Amount ELSE 0 END) AS [2nd 6 Months Actual(' + @CurrentYearName + ')]';

-- Add ratios based on Approved
SET @SQL = @SQL + ',
CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year + 1 AS NVARCHAR) + ' AND c.BudgetType = ''Estimated'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [Estimated %],

CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Revised'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [Revised %],

CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year - 1 AS NVARCHAR) + ' AND c.BudgetType = ''Actual_Audited'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) 
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [Actual Audited %]';

-- Conditionally add 1st or 2nd 6 months %
IF @ReportType <> '2nd_6months_actual'
    SET @SQL = @SQL + ',
CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''1st_6months_actual'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2))
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [1st 6 Months Actual %]';

IF @ReportType <> '1st_6months_actual'
    SET @SQL = @SQL + ',
CASE WHEN SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END)=0 THEN 0
ELSE ROUND(
    CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''2nd_6months_actual'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2))
    / CAST(SUM(CASE WHEN FY.[Year] = ' + CAST(@Year AS NVARCHAR) + ' AND c.BudgetType = ''Approved'' THEN cd.Amount ELSE 0 END) AS DECIMAL(18,2)) * 100,2)
END AS [2nd 6 Months Actual %]';

-- FROM, JOINs, WHERE
SET @SQL = @SQL + '
FROM Ceilings c
INNER JOIN CeilingDetails cd ON c.Id = cd.GLCeilingId
INNER JOIN FiscalYears FY ON c.GLFiscalYearId = FY.Id
INNER JOIN Sabres s ON cd.AccountId = s.Id
INNER JOIN COAs COA ON COA.Id = s.COAId
WHERE c.BudgetType IN (''Revised'',''1st_6months_actual'',''2nd_6months_actual'',''Approved'',''Actual_Audited'',''Estimated'')
  AND FY.[Year] IN (' + CAST(@Year - 1 AS NVARCHAR) + ',' + CAST(@Year AS NVARCHAR) + ',' + CAST(@Year + 1 AS NVARCHAR) + ')';

-- BranchId filter
IF (@BranchId IS NOT NULL)
    SET @SQL = @SQL + ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR);

-- GROUP BY, ORDER BY
SET @SQL = @SQL + '
GROUP BY s.Code, s.Name, COA.Code, COA.Name
ORDER BY s.Code;';

-- Execute
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

        public async Task<ResultVM> GetCeilingDetailDataById(GridOptions options, int masterId, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                var data = new GridEntity<CeilingDetailVM>();

                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT D.AccountId) AS totalcount
        FROM CeilingDetails D
     
        WHERE D.GLCeilingId = @masterId
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<CeilingDetailVM>.FilterCondition(options.filter) + ")" : "") + @"

                -- Data query with pagination and sorting
                SELECT s.Code AccountCode,s.Name AccountName,AccountId,InputTotal
                FROM (
                    SELECT
                                ROW_NUMBER() OVER (ORDER BY MAX(D.Id) DESC) AS rowindex,
                                ISNULL(D.AccountId, 0) AS AccountId,
                                SUM(ISNULL(D.Amount, 0)) AS InputTotal
                                FROM CeilingDetails D
                                Where D.GLCeilingId = @masterId
                                GROUP BY D.AccountId
                    -- Add the filter condition
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<CeilingDetailVM>.FilterCondition(options.filter) + ")" : "") + @"
                ) AS a
                left outer join Sabres s on s.Id=a.AccountId
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
                ";
                sqlQuery = sqlQuery.Replace("@masterId", "" + masterId + "");
                data = KendoGrid<CeilingDetailVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

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

    }
}
