using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SalaryAllowance;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Repository.SalaryAllowance
{
    public class SalaryAllowanceRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(SalaryAllowanceHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                if (transaction == null)
                    transaction = conn.BeginTransaction();

                string query = @"
            INSERT INTO SalaryAllowanceHeaders
            (
                Code, 
                BudgetType,
                FiscalYearId, 
                TransactionDate,
                IsPost, 
                CreatedBy, 
                CreatedOn, 
                CreatedFrom,
                BranchId
            )
            VALUES
            (
                @Code, 
                @BudgetType,
                @FiscalYearId, 
                @TransactionDate, 
                @IsPost,
                @CreatedBy, 
                GETDATE(),
                @CreatedFrom,
                @BranchId
            );

            SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    // Check if IsPost is null and assign a default value (if necessary)
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BudgetType", vm.BudgetType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TransactionDate", vm.TransactionDate ?? (object)DBNull.Value);

                    // Ensure @IsPost is always provided, even if it's null
                    cmd.Parameters.AddWithValue("@IsPost", vm.IsPost); // Default to "N" if IsPost is null

                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? "ERP");
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BranchId", vm.BranchId);

                    object newId = await cmd.ExecuteScalarAsync();

                    vm.Id = Convert.ToInt32(newId);

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
                result.Message = ex.Message;
                return result;
            }
        }

        // Update Method
        public async Task<ResultVM> Update(SalaryAllowanceHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                if (transaction == null)
                    transaction = conn.BeginTransaction();

                string query = @"
                    UPDATE SalaryAllowanceHeaders
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
                        result.Status = "Success";
                        result.Message = "Data updated successfully.";
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

        // Delete Method
        public async Task<ResultVM> MultipleDelete(CommonVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE SalaryAllowanceHeaders SET IsArchive = 1, IsActive = 0, LastModifiedBy = @LastModifiedBy, LastUpdateFrom = @LastUpdateFrom, LastModifiedOn = GETDATE() WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.ModifyFrom);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
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
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // List Method
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues
            , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                string query = @"
SELECT 
    ISNULL(M.BranchId, 0) AS BranchId,
    ISNULL(M.Code, '') AS Code,
    ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
    ISNULL(M.BudgetType, '') AS BudgetType,
    ISNULL(M.TransactionDate, '1900-01-01') AS TransactionDate,
    ISNULL(M.IsPost, '') AS IsPost,
    ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
    ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
    ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
    ISNULL(M.PostedBy, '') AS PostedBy,
    ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
    ISNULL(M.PostedFrom, '') AS PostedFrom
FROM SalaryAllowanceHeaders M
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

                var modelList = dataTable.AsEnumerable().Select(row => new SalaryAllowanceHeaderVM
                {
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

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues
           , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            try
            {
                string query = @"
SELECT 
    ISNULL(M.BranchId, 0) AS BranchId,
    ISNULL(M.Code, '') AS Code,
    ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
    ISNULL(M.BudgetType, '') AS BudgetType,
    ISNULL(M.TransactionDate, '1900-01-01') AS TransactionDate,
    ISNULL(M.IsPost, '') AS IsPost,
    ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
    ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
    ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
    ISNULL(M.PostedBy, '') AS PostedBy,
    ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
    ISNULL(M.PostedFrom, '') AS PostedFrom
FROM SalaryAllowanceHeaders M
WHERE 1 = 1
                ";

                DataTable dataTable = new DataTable();

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND Id = @Id ";
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

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            try
            {
                string query = @"
                SELECT Id, ChargeGroup
                FROM SalaryAllowanceHeaders
                WHERE IsActive = 1
                ORDER BY Code";

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
                result.Message = ex.Message;
                return result;
            }
        }

        // GetGridData Method
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

                var data = new GridEntity<SalaryAllowanceHeaderVM>();

                // Define your SQL query string
                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT M.Id) AS totalcount
FROM SalaryAllowanceHeaders M
WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SalaryAllowanceHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                -- Data query with pagination and sorting
                SELECT * 
                FROM (
                    SELECT 
                    ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "M.Id DESC") + @") AS rowindex,

                   
    
    ISNULL(M.Id, 0) AS Id,    
    ISNULL(M.BranchId, 0) AS BranchId,
    ISNULL(M.Code, '') AS Code,
    ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
    ISNULL(M.BudgetType, '') AS BudgetType,
    ISNULL(M.TransactionDate, '1900-01-01') AS TransactionDate,
    ISNULL(M.IsPost, '') AS IsPost,
    CASE WHEN ISNULL(M.IsPost, '') = 'Y' THEN 'Active' ELSE 'In Active' END AS Status,
    ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
    ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
    ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
    ISNULL(M.PostedBy, '') AS PostedBy,
    ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
    ISNULL(M.PostedFrom, '') AS PostedFrom,  
    ISNULL(M.CreatedBy, '') AS CreatedBy,
    ISNULL(M.CreatedOn, '1900-01-01') AS CreatedOn,
    ISNULL(M.CreatedFrom, '') AS CreatedFrom
FROM SalaryAllowanceHeaders M
WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SalaryAllowanceHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            ";

                // Execute the query and get data
                data = KendoGrid<SalaryAllowanceHeaderVM>.GetGridData_CMD(options, sqlQuery, "M.Id");

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
        // ReportPreview Method
        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValue
            , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                string query = @"
                SELECT 
                    ISNULL(H.Id, 0) AS Id,
                    ISNULL(H.Code, '') AS Code,
                    ISNULL(H.TransactionDate, '') AS TransactionDate,
                    ISNULL(H.TransactionType, '') AS TransactionType,
                    ISNULL(H.SupplierId, 0) AS SupplierId,
                    ISNULL(H.Status, '') AS Status,
                    ISNULL(H.Notes, '') AS Notes,
                    ISNULL(H.CreatedBy, '') AS CreatedBy,
                    ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') AS CreatedOn
                FROM FM_FeedPurchaseHeaders H
                WHERE 1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND H.Id = @Id ";
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

        // MultiplePost Method
        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE SalaryAllowanceHeaders SET IsPost = 1, PostedBy = @PostedBy , PostedFrom = @PostedFrom ,PostedOn = GETDATE() WHERE Id IN ({inClause}) ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@PostedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@PostedFrom", vm.ModifyFrom);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.RetrievedSuccess;
                    }
                    else
                    {
                        throw new Exception("No rows were posted.");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }
        // InsertDetails Method
        public async Task<ResultVM> InsertDetails(SalaryAllowanceDetailVM details, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                string query = @"
                    INSERT INTO SalaryAllowanceDetails
                    (SalaryAllowanceHeaderId, PersonnelCategoriesId, TotalPostSanctioned,ActualPresentStrength,ExpectedNumber,BasicWagesSalaries,OtherCash,TotalSalary,PersonnelSentForTraining)
                    VALUES 
                       (@SalaryAllowanceHeaderId, @PersonnelCategoriesId, @TotalPostSanctioned,@ActualPresentStrength,@ExpectedNumber,@BasicWagesSalaries,@OtherCash,@TotalSalary,@PersonnelSentForTraining);
                    SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@SalaryAllowanceHeaderId", details.SalaryAllowanceHeaderId);
                    cmd.Parameters.AddWithValue("@PersonnelCategoriesId", details.PersonnelCategoriesId);
                    cmd.Parameters.AddWithValue("@TotalPostSanctioned", details.TotalPostSanctioned);
                    cmd.Parameters.AddWithValue("@ActualPresentStrength", details.ActualPresentStrength);
                    cmd.Parameters.AddWithValue("@ExpectedNumber", details.ExpectedNumber);
                    cmd.Parameters.AddWithValue("@BasicWagesSalaries", details.BasicWagesSalaries);
                    cmd.Parameters.AddWithValue("@OtherCash", details.OtherCash);
                    cmd.Parameters.AddWithValue("@TotalSalary", details.TotalSalary);
                    cmd.Parameters.AddWithValue("@PersonnelSentForTraining", details.PersonnelSentForTraining);

                    object newId = await cmd.ExecuteScalarAsync();

                    details.Id = Convert.ToInt32(newId);

                    result.Status = "Success";
                    result.Message = "Details data inserted successfully.";
                    result.DetailId = newId.ToString();
                    result.DataVM = details;
                }

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
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
            ,D.SalaryAllowanceHeaderId
            ,D.PersonnelCategoriesId
            ,P.CategoryOfPersonnel AS PersonnelCategoriesName
            ,D.TotalPostSanctioned
            ,D.ActualPresentStrength
            ,D.ExpectedNumber
            ,D.BasicWagesSalaries
            ,D.OtherCash
            ,D.TotalSalary
            ,D.PersonnelSentForTraining

        FROM SalaryAllowanceDetails D
        LEFT JOIN PersonnelCategories P ON D.PersonnelCategoriesId = P.Id
  
                   WHERE 1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.SalaryAllowanceHeaderId = @Id ";
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
        //GetPurchaseDetailDataById
        public async Task<ResultVM> GetDetailDataById(GridOptions options, int masterId, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                var data = new GridEntity<SalaryAllowanceDetailVM>();

                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT D.Id) AS totalcount
        FROM ChargeDetails D
     
        WHERE D.ChargeHeaderId = @masterId
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SalaryAllowanceDetailVM>.FilterCondition(options.filter) + ")" : "") + @"

                -- Data query with pagination and sorting
                SELECT *
                FROM (
                    SELECT
                        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "D.Id DESC ") + @") AS rowindex,
             ISNULL(D.Id, 0) AS Id
            ,D.SalaryAllowanceHeaderId
            ,D.PersonnelCategoriesId
            ,P.CategoryOfPersonnel AS PersonnelCategoriesName
            ,D.TotalPostSanctioned
            ,D.ActualPresentStrength
            ,D.ExpectedNumber
            ,D.BasicWagesSalaries
            ,D.OtherCash
            ,D.TotalSalary
            ,D.PersonnelSentForTraining

        FROM SalaryAllowanceDetails D
        LEFT JOIN PersonnelCategories P ON D.PersonnelCategoriesId = P.Id
  
        Where D.SalaryAllowanceHeaderId = @masterId


                    -- Add the filter condition
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SalaryAllowanceDetailVM>.FilterCondition(options.filter) + ")" : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
                ";
                sqlQuery = sqlQuery.Replace("@masterId", "" + masterId + "");
                data = KendoGrid<SalaryAllowanceDetailVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

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


        //GetDetailsGridData
        public async Task<ResultVM> GetDetailsGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {

                var data = new GridEntity<SalaryAllowanceDetailVM>();

                // Define your SQL query string
                string sqlQuery = $@"
    -- Count query
    SELECT COUNT(DISTINCT D.Id) AS totalcount
        FROM SalaryAllowanceDetails D
        LEFT JOIN PersonnelCategories P ON D.PersonnelCategoriesId = P.Id
  
                   WHERE 1 = 1
    -- Add the filter condition
        " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SalaryAllowanceDetailVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    -- Data query with pagination and sorting
    SELECT * 
    FROM (
        SELECT 
        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "D.Id DESC") + $@") AS rowindex,
             SELECT 
             ISNULL(D.Id, 0) AS Id
            ,D.SalaryAllowanceHeaderId
            ,D.PersonnelCategoriesId
            ,P.CategoryOfPersonnel AS CategoryOfPersonnel
            ,D.TotalPostSanctioned
            ,D.ActualPresentStrength
            ,D.ExpectedNumber
            ,D.BasicWagesSalaries
            ,D.OtherCash
            ,D.TotalSalary
            ,D.PersonnelSentForTraining

        FROM SalaryAllowanceDetails D
        LEFT JOIN PersonnelCategories P ON D.PersonnelCategoriesId = P.Id
  
                   WHERE 1 = 1

    -- Add the filter condition
        " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SalaryAllowanceDetailVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    ) AS a
    WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
";

                data = KendoGrid<SalaryAllowanceDetailVM>.GetTransactionalGridData_CMD(options, sqlQuery, "D.Id", conditionalFields, conditionalValues);

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


    }
}
