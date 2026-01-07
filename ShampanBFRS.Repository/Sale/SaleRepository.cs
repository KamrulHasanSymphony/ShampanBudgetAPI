using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SalaryAllowance;
using ShampanBFRS.ViewModel.Sale;
using ShampanBFRS.ViewModel.Utility;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.Sale
{
    public class SaleRepository : CommonRepository
    {

        // Insert Method
        public async Task<ResultVM> Insert(SaleHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                if (transaction == null)
                    transaction = conn.BeginTransaction();

                string query = @"
            INSERT INTO SaleHeaders
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

                   
                    cmd.Parameters.AddWithValue("@IsPost", false); 

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
        public async Task<ResultVM> Update(SaleHeaderVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                if (transaction == null)
                    transaction = conn.BeginTransaction();

                string query = @"
                    UPDATE SaleHeaders
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

                string query = $" UPDATE SaleHeaders SET IsArchive = 1, IsActive = 0, LastModifiedBy = @LastModifiedBy, LastUpdateFrom = @LastUpdateFrom, LastModifiedOn = GETDATE() WHERE Id IN ({inClause})";

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
FROM SaleHeaders M
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

                var modelList = dataTable.AsEnumerable().Select(row => new SaleHeaderVM
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
FROM SaleHeaders M
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
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                var data = new GridEntity<SaleHeaderVM>();

                string sqlQuery = @"
                SELECT COUNT(DISTINCT M.ID) AS totalcount
                FROM SaleHeaders M
                WHERE 1 = 1 
    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleHeaderVM>.FilterCondition(options.filter) + ")" : "");

                //Apply additional conditions
                sqlQuery = ApplyConditionsWithBetween(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    
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
            CASE WHEN ISNULL(M.IsPost, '') = 1 THEN 'Posted' ELSE 'Not Posted' END AS Status,
                ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
                ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
                ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
                ISNULL(M.PostedBy, '') AS PostedBy,
                ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
                ISNULL(M.PostedFrom, '') AS PostedFrom,
                ISNULL(M.CreatedBy, '') AS CreatedBy,
                ISNULL(M.CreatedOn, '1900-01-01') AS CreatedOn,
                ISNULL(M.CreatedFrom, '') AS CreatedFrom
            FROM SaleHeaders M
            WHERE 1 = 1             
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleHeaderVM>.FilterCondition(options.filter) + ")" : "");

                //Apply additional conditions
                sqlQuery = ApplyConditionsWithBetween(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"

    ) AS a
    WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
    ";

                data = KendoGrid<SaleHeaderVM>.GetDataWithBetween_CMD(options, sqlQuery, "M.ID", conditionalFields, conditionalValues);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = data;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
            //            DataTable dataTable = new DataTable();
            //            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };


            //            try
            //            {
            //                if (conn == null)
            //                {
            //                    throw new Exception("Database connection fail!");
            //                }

            //                var data = new GridEntity<SaleHeaderVM>();

            //                // Define your SQL query string
            //                string sqlQuery = @"
            //                -- Count query
            //                SELECT COUNT(DISTINCT M.Id) AS totalcount
            //FROM SaleHeaders M
            //WHERE 1 = 1

            //                -- Add the filter condition
            //                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleHeaderVM>.FilterCondition(options.filter) + ")" : "");

            //                // Apply additional conditions
            //                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

            //                sqlQuery += @"
            //                -- Data query with pagination and sorting
            //                SELECT * 
            //                FROM (
            //                    SELECT 
            //                    ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "M.Id DESC") + @") AS rowindex,



            //ISNULL(M.Id, 0) AS Id,
            //ISNULL(M.BranchId, 0) AS BranchId,
            //ISNULL(M.Code, '') AS Code,
            //ISNULL(M.FiscalYearId, 0) AS FiscalYearId,
            //ISNULL(M.BudgetType, '') AS BudgetType,
            //ISNULL(M.TransactionDate, '1900-01-01') AS TransactionDate,
            //ISNULL(M.IsPost, '') AS IsPost,
            //CASE WHEN ISNULL(M.IsPost, '') = 'Y' THEN 'Posted' ELSE 'Not Posted' END AS Status,
            //    ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
            //    ISNULL(M.LastUpdateOn, '1900-01-01') AS LastUpdateOn,
            //    ISNULL(M.LastUpdateFrom, '') AS LastUpdateFrom,
            //    ISNULL(M.PostedBy, '') AS PostedBy,
            //    ISNULL(M.PostedOn, '1900-01-01') AS PostedOn,
            //    ISNULL(M.PostedFrom, '') AS PostedFrom,
            //    ISNULL(M.CreatedBy, '') AS CreatedBy,
            //    ISNULL(M.CreatedOn, '1900-01-01') AS CreatedOn,
            //    ISNULL(M.CreatedFrom, '') AS CreatedFrom
            //FROM SaleHeaders M
            //WHERE 1 = 1

            //                -- Add the filter condition
            //                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleHeaderVM>.FilterCondition(options.filter) + ")" : "");

            //                // Apply additional conditions
            //                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

            //                sqlQuery += @"
            //                ) AS a
            //                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            //            ";

            //                // Execute the query and get data
            //                data = KendoGrid<SaleHeaderVM>.GetGridData_CMD(options, sqlQuery, "M.Id");

            //                result.Status = MessageModel.Success;
            //                result.Message = MessageModel.RetrievedSuccess;
            //                result.DataVM = data;

            //                return result;
            //            }
            //            catch (Exception ex)
            //            {
            //                result.ExMessage = ex.Message;
            //                result.Message = ex.Message;
            //                return result;
            //            }
        }
        
        // MultiplePost Method
        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE SaleHeaders SET IsPost = 1, PostedBy = @PostedBy , PostedFrom = @PostedFrom ,PostedOn = GETDATE() WHERE Id IN ({inClause}) ";

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
                        result.Message = MessageModel.PostSuccess;
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
        public async Task<ResultVM> InsertDetails(SaleDetailVM details, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                string query = @"
                    INSERT INTO SaleDetails
                    (SaleHeaderId, ProductId, ConversionFactor,ProductionMT,PriceMT,PriceLTR,SalesExERLValue,SalesExImport_LocalMT,SalesExImport_LocalValue,TotalMT,TotalValueTK_LAC)
                    VALUES 
                       (@SaleHeaderId, @ProductId, @ConversionFactor,@ProductionMT,@PriceMT,@PriceLTR,@SalesExERLValue,@SalesExImport_LocalMT,@SalesExImport_LocalValue,@TotalMT,@TotalValueTK_LAC);
                    SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@SaleHeaderId", details.SaleHeaderId);
                    cmd.Parameters.AddWithValue("@ProductId", details.ProductId);
                    cmd.Parameters.AddWithValue("@ConversionFactor", details.ConversionFactor);
                    cmd.Parameters.AddWithValue("@ProductionMT", details.ProductionMT);
                    cmd.Parameters.AddWithValue("@PriceMT", details.PriceMT);
                    cmd.Parameters.AddWithValue("@PriceLTR", details.PriceLTR);
                    cmd.Parameters.AddWithValue("@SalesExERLValue", details.SalesExERLValue);
                    cmd.Parameters.AddWithValue("@SalesExImport_LocalMT", details.SalesExImport_LocalMT);
                    cmd.Parameters.AddWithValue("@SalesExImport_LocalValue", details.SalesExImport_LocalValue);
                    cmd.Parameters.AddWithValue("@TotalMT", details.TotalMT);
                    cmd.Parameters.AddWithValue("@TotalValueTK_LAC", details.TotalValueTK_LAC);

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
            ,D.SaleHeaderId
            ,D.ProductId
            ,P.Name AS ProductName
            ,D.ConversionFactor
            ,D.ProductionMT
            ,D.PriceMT
            ,D.PriceLTR
            ,D.SalesExERLValue
            ,D.SalesExImport_LocalMT
            ,D.SalesExImport_LocalValue
			,D.TotalMT
			,D.TotalValueTK_LAC

        FROM SaleDetails D
        LEFT JOIN Products P ON D.ProductId = P.Id
  
                   WHERE 1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.SaleHeaderId = @Id ";
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
                var data = new GridEntity<SaleDetailVM>();

                string sqlQuery = @"
                -- Count query
       SELECT COUNT(DISTINCT D.Id) AS totalcount

        FROM SaleDetails D
        LEFT JOIN Products P ON D.ProductId = P.Id
  
        Where D.SaleHeaderId = @masterId
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleDetailVM>.FilterCondition(options.filter) + ")" : "") + @"

                -- Data query with pagination and sorting
                SELECT *
                FROM (
                    SELECT
                        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "D.Id DESC ") + @") AS rowindex,
           
             ISNULL(D.Id, 0) AS Id
            ,D.SaleHeaderId
            ,D.ProductId
            ,P.Name AS ProductName
            ,D.ConversionFactor
            ,D.ProductionMT
            ,D.PriceMT
            ,D.PriceLTR
            ,D.SalesExERLValue
            ,D.SalesExImport_LocalMT
            ,D.SalesExImport_LocalValue
			,D.TotalMT
			,D.TotalValueTK_LAC

        FROM SaleDetails D
        LEFT JOIN Products P ON D.ProductId = P.Id
  
        Where D.SaleHeaderId = @masterId


                    -- Add the filter condition
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleDetailVM>.FilterCondition(options.filter) + ")" : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
                ";
                sqlQuery = sqlQuery.Replace("@masterId", "" + masterId + "");
                data = KendoGrid<SaleDetailVM>.GetGridData_CMD(options, sqlQuery, "D.Id");

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

                var data = new GridEntity<SaleDetailVM>();

                // Define your SQL query string
                string sqlQuery = $@"
    -- Count query
    SELECT COUNT(DISTINCT D.Id) AS totalcount
        FROM SaleDetails D
        LEFT JOIN Products P ON D.ProductId = P.Id
  
                   WHERE 1 = 1
    -- Add the filter condition
        " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleDetailVM>.FilterCondition(options.filter) + ")" : "");

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
            ,D.SaleHeaderId
            ,D.ProductId
            ,P.Name AS ProductName
            ,D.ConversionFactor
            ,D.ProductionMT
            ,D.PriceMT
            ,D.PriceLTR
            ,D.SalesExERLValue
            ,D.SalesExImport_LocalMT
            ,D.SalesExImport_LocalValue
			,D.TotalMT
			,D.TotalValueTK_LAC

        FROM SaleDetails D
        LEFT JOIN Products P ON D.ProductId = P.Id
  
                   WHERE 1 = 1

    -- Add the filter condition
        " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<SaleDetailVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    ) AS a
    WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
";

                data = KendoGrid<SaleDetailVM>.GetTransactionalGridData_CMD(options, sqlQuery, "D.Id", conditionalFields, conditionalValues);

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

        public string ApplyConditionsWithBetween(string sqlText, string[] conditionalFields, string[] conditionalValue, bool orOperator = false)
        {
            try
            {
                string cField = "";
                string field = "";
                bool conditionFlag = true;
                var checkValueExist = conditionalValue == null ? false : conditionalValue.ToList().Any(x => !string.IsNullOrEmpty(x));
                var checkConditioanlValue = conditionalValue == null ? false : conditionalValue.ToList().Any(x => !string.IsNullOrEmpty(x));

                if (checkValueExist && orOperator && checkConditioanlValue)
                {
                    sqlText += " and (";
                }

                if (conditionalFields != null && conditionalValue != null && conditionalFields.Length == conditionalValue.Length)
                {
                    for (int i = 0; i < conditionalFields.Length; i++)
                    {
                        if (string.IsNullOrWhiteSpace(conditionalFields[i]) || string.IsNullOrWhiteSpace(conditionalValue[i]))
                        {
                            continue;
                        }
                        cField = conditionalFields[i].ToString();
                        field = StringReplacing(cField);
                        cField = cField.Replace(".", "");
                        string operand = " AND ";

                        if (orOperator)
                        {
                            operand = " OR ";

                            if (conditionFlag)
                            {
                                operand = "  ";
                                conditionFlag = false;
                            }
                        }

                        if (conditionalFields[i].ToLower().Contains("like"))
                        {
                            sqlText += operand + conditionalFields[i] + " '%'+ " + " @" + cField.Replace("like", "").Trim() + " +'%'";
                        }

                        else if (conditionalFields[i].Contains(">") || conditionalFields[i].Contains("<"))
                        {
                            sqlText += operand + conditionalFields[i] + " @" + cField;
                        }

                        else if (conditionalFields[i].ToLower().Contains("between"))
                        {
                            cField = conditionalFields[i].Replace(".", "").Replace(" between", "");
                            sqlText += " AND " + conditionalFields[i].Replace(" between", "") +
                                       " BETWEEN @" + cField + "_From AND @" + cField + "_To";
                        }
                        else if (conditionalFields[i].ToLower().Contains("not"))
                        {
                            cField = cField.Replace(" not", "");
                            string param = conditionalFields[i].Replace(" not", "");
                            sqlText += operand + param + " != @" + cField;
                        }
                        else if (conditionalFields[i].Contains("in", StringComparison.OrdinalIgnoreCase))
                        {
                            var test = conditionalFields[i].Split(" in");

                            if (test.Length > 1)
                            {
                                sqlText += operand + conditionalFields[i] + "(" + conditionalValue[i] + ")";
                            }
                            else
                            {
                                sqlText += operand + conditionalFields[i] + "= '" + Convert.ToString(conditionalValue[i]) + "'";
                            }
                        }
                        else
                        {
                            sqlText += operand + conditionalFields[i] + "= @" + cField;
                        }
                    }
                }

                if (checkValueExist && orOperator && checkConditioanlValue)
                {
                    sqlText += " )";
                }

                return sqlText;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    
        public async Task<ResultVM> ReportPreview(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"

select 
 p.Name Products
,sd.ProductionMT as 'Production (M.Ton)'
,sd.ConversionFactor as 'Conversion (LTR/Gallon per M.Ton)'
,sd.PriceMT as 'Price /M.Ton'
,sd.PriceLTR as 'Price /LTR'
,sd.SalesExERLValue as 'Sales (Ex-ERL) Value'
,sd.SalesExImport_LocalMT as 'Sales (Ex-Import+Local) M.Ton'
,sd.SalesExImport_LocalValue as 'Sales (Ex-Import+Local) Value'
,sd.TotalMT as 'Total M.Ton'
,sd.TotalValueTK_LAC as 'Total Value  TK/LAC'

from SaleHeaders sh
left outer join SaleDetails sd on sd.SaleHeaderId=sh.Id
left outer join Products p on sd.ProductId=p.Id
where 1=1
and sh.FiscalYearId=@FiscalYearId
and sh.BudgetType=@BudgetType

";


                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand.Parameters.AddWithValue("@BudgetType", vm.BudgetType);
                adapter.SelectCommand.Parameters.AddWithValue("@FiscalYearId", vm.YearId);

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


    }
}

