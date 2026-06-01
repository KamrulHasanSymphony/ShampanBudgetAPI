using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Repository.PieChart
{
    public class PieChartRepository
    {
        public async Task<ResultVM> GetBudgetPieChart(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string companyId = "0";

                // ✅ FIX: Get CompanyId properly
                if (conditionalValues != null && conditionalValues.Length > 0)
                    companyId = conditionalValues[0] ?? "0";

                string sql = @"
DECLARE @BranchId INT = @BId;
DECLARE @Year INT;

DECLARE @EstimatedYear INT;
DECLARE @ApprovedYear INT;
DECLARE @ActualYear INT;

DECLARE @EstimatedYearName NVARCHAR(50);
DECLARE @ApprovedYearName NVARCHAR(50);
DECLARE @ActualYearName NVARCHAR(50);

DECLARE @SQL NVARCHAR(MAX);
DECLARE @ReportType NVARCHAR(50) = @RType;

------------------------------------------------------------
-- Get base year
------------------------------------------------------------
SELECT @Year = [Year]
FROM FiscalYears
WHERE Id =@FYId;

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
-- Build query
------------------------------------------------------------
SET @SQL = '

SELECT Particular, TotalValue
FROM
(
    -- Estimated
    SELECT 
        ''Estimated(' + @EstimatedYearName + ')'' AS Particular,
        SUM(CASE 
                WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + ' 
                 AND c.BudgetType = ''Estimated'' 
                THEN cd.Yearly ELSE 0 
            END) AS TotalValue,
        1 AS SortOrder

    FROM BudgetHeaders c
    INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
    INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
    WHERE FY.[Year] IN (' 
        + CAST(@ActualYear AS NVARCHAR) + ',' 
        + CAST(@ApprovedYear AS NVARCHAR) + ',' 
        + CAST(@EstimatedYear AS NVARCHAR) + ')
        ' + CASE WHEN @BranchId IS NOT NULL 
                THEN ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
                ELSE '' END + '

    UNION ALL

    -- Revised
    SELECT 
        ''Revised(' + @ApprovedYearName + ')'', 
        SUM(CASE 
                WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                 AND c.BudgetType = ''Revised'' 
                THEN cd.Yearly ELSE 0 
            END),
        2
    FROM BudgetHeaders c
    INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
    INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
    WHERE FY.[Year] IN (' 
        + CAST(@ActualYear AS NVARCHAR) + ',' 
        + CAST(@ApprovedYear AS NVARCHAR) + ',' 
        + CAST(@EstimatedYear AS NVARCHAR) + ')
        ' + CASE WHEN @BranchId IS NOT NULL 
                THEN ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
                ELSE '' END + '

    UNION ALL

    -- Approved
    SELECT 
        ''Approved(' + @ApprovedYearName + ')'', 
        SUM(CASE 
                WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                 AND c.BudgetType = ''Approved'' 
                THEN cd.Yearly ELSE 0 
            END),
        3
    FROM BudgetHeaders c
    INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
    INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
    WHERE FY.[Year] IN (' 
        + CAST(@ActualYear AS NVARCHAR) + ',' 
        + CAST(@ApprovedYear AS NVARCHAR) + ',' 
        + CAST(@EstimatedYear AS NVARCHAR) + ')
        ' + CASE WHEN @BranchId IS NOT NULL 
                THEN ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
                ELSE '' END + '

    UNION ALL

    -- Actual Audited
    SELECT 
        ''Actual Audited(' + @ActualYearName + ')'', 
        SUM(CASE 
                WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + ' 
                 AND c.BudgetType = ''Actual_Audited'' 
                THEN cd.Yearly ELSE 0 
            END),
        4
    FROM BudgetHeaders c
    INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
    INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
    WHERE FY.[Year] IN (' 
        + CAST(@ActualYear AS NVARCHAR) + ',' 
        + CAST(@ApprovedYear AS NVARCHAR) + ',' 
        + CAST(@EstimatedYear AS NVARCHAR) + ')
        ' + CASE WHEN @BranchId IS NOT NULL 
                THEN ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
                ELSE '' END + '

    ' + CASE WHEN @ReportType <> '2nd_6months_actual' THEN '
    UNION ALL
    -- 1st 6 Months
    SELECT 
        ''1st 6 Months Actual(' + @ApprovedYearName + ')'', 
        SUM(CASE 
                WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                 AND c.BudgetType = ''1st_6months_actual'' 
                THEN cd.Yearly ELSE 0 
            END),
        5
    FROM BudgetHeaders c
    INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
    INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
    WHERE FY.[Year] IN (' 
        + CAST(@ActualYear AS NVARCHAR) + ',' 
        + CAST(@ApprovedYear AS NVARCHAR) + ',' 
        + CAST(@EstimatedYear AS NVARCHAR) + ')
        ' + CASE WHEN @BranchId IS NOT NULL 
                THEN ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
                ELSE '' END 
    ELSE '' END + '

    ' + CASE WHEN @ReportType <> '1st_6months_actual' THEN '
    UNION ALL
    -- 2nd 6 Months
    SELECT 
        ''2nd 6 Months Actual(' + @ApprovedYearName + ')'', 
        SUM(CASE 
                WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + ' 
                 AND c.BudgetType = ''2nd_6months_actual'' 
                THEN cd.Yearly ELSE 0 
            END),
        6
    FROM BudgetHeaders c
    INNER JOIN BudgetDetails cd ON c.Id = cd.BudgetHeaderId
    INNER JOIN FiscalYears FY ON c.FiscalYearId = FY.Id
    WHERE FY.[Year] IN (' 
        + CAST(@ActualYear AS NVARCHAR) + ',' 
        + CAST(@ApprovedYear AS NVARCHAR) + ',' 
        + CAST(@EstimatedYear AS NVARCHAR) + ')
        ' + CASE WHEN @BranchId IS NOT NULL 
                THEN ' AND c.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
                ELSE '' END 
    ELSE '' END + '

) X
ORDER BY SortOrder
';

EXEC sp_executesql @SQL;
 ";

                SqlDataAdapter objComm = new SqlDataAdapter(sql, conn);
                objComm.SelectCommand.Transaction = transaction;

                //var fiscalYearId = 6;
                //var branchId = 1;
                //var reportType = "1st_6months_actual";

                int fiscalYearId = Convert.ToInt32(vm.FiscalYearId ?? "0");
                int branchId = Convert.ToInt32(vm.BranchId ?? "0");
                string reportType = vm.ReportType ?? "";

                objComm.SelectCommand.Parameters.Add("@BId", SqlDbType.Int).Value = branchId;
                objComm.SelectCommand.Parameters.Add("@FYId", SqlDbType.Int).Value = fiscalYearId;
                objComm.SelectCommand.Parameters.Add("@RType", SqlDbType.VarChar).Value = reportType;
                objComm.SelectCommand.Parameters.Add("@CompanyId", SqlDbType.VarChar).Value = companyId;

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new PieChartVM
                {
                    Particular = row["Particular"].ToString(),
                    TotalValue = row["TotalValue"] != DBNull.Value
                        ? Convert.ToDecimal(row["TotalValue"])
                        : 0
                }).ToList();

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;

                return await Task.FromResult(result);
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetSalaryAllowancePieChart(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();

            ResultVM result = new ResultVM
            {
                Status = "Fail",
                Message = "Error"
            };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                //---------------------------------------------------
                // Parameters
                //---------------------------------------------------
                //int branchId = 1;
                //int fiscalYearId = 6;

                //if (conditionalValues != null)
                //{
                //    if (conditionalValues.Length > 0 && !string.IsNullOrWhiteSpace(conditionalValues[0]))
                //        int.TryParse(conditionalValues[0], out branchId);

                //    if (conditionalValues.Length > 1 && !string.IsNullOrWhiteSpace(conditionalValues[1]))
                //        int.TryParse(conditionalValues[1], out fiscalYearId);
                //}

                //---------------------------------------------------
                // SQL
                //---------------------------------------------------
                string sql = @"
DECLARE @BranchId INT = @BId;
DECLARE @Year INT;

DECLARE @EstimatedYear INT;
DECLARE @ApprovedYear INT;
DECLARE @ActualYear INT;

DECLARE @EstimatedYearName NVARCHAR(50);
DECLARE @ApprovedYearName NVARCHAR(50);
DECLARE @ActualYearName NVARCHAR(50);

DECLARE @SQL NVARCHAR(MAX);

------------------------------------------------------------
-- Get base year
------------------------------------------------------------
SELECT @Year = [Year]
FROM FiscalYears
WHERE Id = @FiscalYearId;

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
-- Build Query
------------------------------------------------------------
SET @SQL = '

SELECT 
    Particular,
    TotalSalary
FROM
(
    ------------------------------------------------------------
    -- Estimated
    ------------------------------------------------------------
    SELECT
        ''Estimated(' + @EstimatedYearName + ')'' AS Particular,

        SUM(CASE
                WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + '
                 AND H.BudgetType = ''Estimated''
                THEN D.TotalSalary
                ELSE 0
            END) AS TotalSalary,

        1 AS SortOrder

    FROM SalaryAllowanceDetails D

    LEFT JOIN PersonnelCategories P
        ON D.PersonnelCategoriesId = P.Id

    INNER JOIN SalaryAllowanceHeaders H
        ON D.SalaryAllowanceHeaderId = H.Id

    INNER JOIN FiscalYears FY
        ON H.FiscalYearId = FY.Id

    WHERE FY.[Year] IN (
        ' + CAST(@ActualYear AS NVARCHAR) + ',
        ' + CAST(@ApprovedYear AS NVARCHAR) + ',
        ' + CAST(@EstimatedYear AS NVARCHAR) + '
    )

    ' + CASE WHEN @BranchId IS NOT NULL 
        THEN ' AND H.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
        ELSE '' END + '

    UNION ALL

    ------------------------------------------------------------
    -- Revised
    ------------------------------------------------------------
    SELECT
        ''Revised(' + @ApprovedYearName + ')'' AS Particular,

        SUM(CASE
                WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 AND H.BudgetType = ''Revised''
                THEN D.TotalSalary
                ELSE 0
            END) AS TotalSalary,

        2 AS SortOrder

    FROM SalaryAllowanceDetails D

    LEFT JOIN PersonnelCategories P
        ON D.PersonnelCategoriesId = P.Id

    INNER JOIN SalaryAllowanceHeaders H
        ON D.SalaryAllowanceHeaderId = H.Id

    INNER JOIN FiscalYears FY
        ON H.FiscalYearId = FY.Id

    WHERE FY.[Year] IN (
        ' + CAST(@ActualYear AS NVARCHAR) + ',
        ' + CAST(@ApprovedYear AS NVARCHAR) + ',
        ' + CAST(@EstimatedYear AS NVARCHAR) + '
    )

    ' + CASE WHEN @BranchId IS NOT NULL 
        THEN ' AND H.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
        ELSE '' END + '

    UNION ALL

    ------------------------------------------------------------
    -- Approved
    ------------------------------------------------------------
    SELECT
        ''Approved(' + @ApprovedYearName + ')'' AS Particular,

        SUM(CASE
                WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 AND H.BudgetType = ''Approved''
                THEN D.TotalSalary
                ELSE 0
            END) AS TotalSalary,

        3 AS SortOrder

    FROM SalaryAllowanceDetails D

    LEFT JOIN PersonnelCategories P
        ON D.PersonnelCategoriesId = P.Id

    INNER JOIN SalaryAllowanceHeaders H
        ON D.SalaryAllowanceHeaderId = H.Id

    INNER JOIN FiscalYears FY
        ON H.FiscalYearId = FY.Id

    WHERE FY.[Year] IN (
        ' + CAST(@ActualYear AS NVARCHAR) + ',
        ' + CAST(@ApprovedYear AS NVARCHAR) + ',
        ' + CAST(@EstimatedYear AS NVARCHAR) + '
    )

    ' + CASE WHEN @BranchId IS NOT NULL 
        THEN ' AND H.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
        ELSE '' END + '

    UNION ALL

    ------------------------------------------------------------
    -- Actual
    ------------------------------------------------------------
    SELECT
        ''Actual(' + @ActualYearName + ')'' AS Particular,

        SUM(CASE
                WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + '
                 AND H.BudgetType = ''Actual_Audited''
                THEN D.TotalSalary
                ELSE 0
            END) AS TotalSalary,

        4 AS SortOrder

    FROM SalaryAllowanceDetails D

    LEFT JOIN PersonnelCategories P
        ON D.PersonnelCategoriesId = P.Id

    INNER JOIN SalaryAllowanceHeaders H
        ON D.SalaryAllowanceHeaderId = H.Id

    INNER JOIN FiscalYears FY
        ON H.FiscalYearId = FY.Id

    WHERE FY.[Year] IN (
        ' + CAST(@ActualYear AS NVARCHAR) + ',
        ' + CAST(@ApprovedYear AS NVARCHAR) + ',
        ' + CAST(@EstimatedYear AS NVARCHAR) + '
    )

    ' + CASE WHEN @BranchId IS NOT NULL 
        THEN ' AND H.BranchId = ' + CAST(@BranchId AS NVARCHAR) 
        ELSE '' END + '

) X
ORDER BY SortOrder
';

EXEC sp_executesql @SQL;
";

                //---------------------------------------------------
                // Execute
                //---------------------------------------------------
                SqlDataAdapter objComm = new SqlDataAdapter(sql, conn);
                objComm.SelectCommand.Transaction = transaction;

                int fiscalYearId = Convert.ToInt32(vm.FiscalYearId ?? "0");
                int branchId = Convert.ToInt32(vm.BranchId ?? "0");

                objComm.SelectCommand.Parameters.Add("@BId", SqlDbType.Int).Value = branchId;
                objComm.SelectCommand.Parameters.Add("@FiscalYearId", SqlDbType.Int).Value = fiscalYearId;

                objComm.Fill(dataTable);

                //---------------------------------------------------
                // Mapping
                //---------------------------------------------------
                var modelList = dataTable.AsEnumerable()
                    .Select(row => new PieChartVM
                    {
                        Particular = row["Particular"]?.ToString(),
                        TotalValue = row["TotalSalary"] != DBNull.Value
                            ? Convert.ToDecimal(row["TotalSalary"])
                            : 0
                    })
                    .ToList();

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;

                return await Task.FromResult(result);
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetProductBudgetPieChart(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();

            ResultVM result = new ResultVM
            {
                Status = "Fail",
                Message = "Error"
            };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                //---------------------------------------------------
                // Parameters
                //---------------------------------------------------
                int branchId = 1;
                int fiscalYearId = 6;

                if (conditionalValues != null)
                {
                    if (conditionalValues.Length > 0 && !string.IsNullOrWhiteSpace(conditionalValues[0]))
                        int.TryParse(conditionalValues[0], out branchId);

                    if (conditionalValues.Length > 1 && !string.IsNullOrWhiteSpace(conditionalValues[1]))
                        int.TryParse(conditionalValues[1], out fiscalYearId);
                }

                //---------------------------------------------------
                // SQL
                //---------------------------------------------------
                string sql = @"
DECLARE @BranchId INT = @BId;
DECLARE @Year INT;
DECLARE @EstimatedYear INT;
DECLARE @ApprovedYear INT;
DECLARE @ActualYear INT;

DECLARE @EstimatedYearName NVARCHAR(50);
DECLARE @ApprovedYearName NVARCHAR(50);
DECLARE @ActualYearName NVARCHAR(50);

DECLARE @SQL NVARCHAR(MAX);

------------------------------------------------------------
-- Get Base Year
------------------------------------------------------------
SELECT @Year = [Year]
FROM FiscalYears
WHERE Id = @FiscalYearId;

SET @EstimatedYear = @Year;
SET @ApprovedYear  = @Year - 1;
SET @ActualYear    = @Year - 2;

------------------------------------------------------------
-- Year Names
------------------------------------------------------------
SELECT @EstimatedYearName = YearName FROM FiscalYears WHERE [Year] = @EstimatedYear;
SELECT @ApprovedYearName  = YearName FROM FiscalYears WHERE [Year] = @ApprovedYear;
SELECT @ActualYearName    = YearName FROM FiscalYears WHERE [Year] = @ActualYear;

------------------------------------------------------------
-- Dynamic Query
------------------------------------------------------------
SET @SQL = '

SELECT 
    Particular,
    TotalQuantity
FROM
(
    SELECT 
        ''Estimated(' + @EstimatedYearName + ')'' AS Particular,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + '
                 AND PB.BudgetType = ''Estimated''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END) AS TotalQuantity,
        1 AS SortOrder
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    WHERE FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Revised(' + @ApprovedYearName + ')'' ,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 AND PB.BudgetType = ''Revised''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END),
        2
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Approved(' + @ApprovedYearName + ')'' ,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 AND PB.BudgetType = ''Approved''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END),
        3
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''1st 6 Months Actual(' + @ApprovedYearName + ')'' ,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 AND PB.BudgetType = ''1st_6months_actual''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END),
        4
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''2nd 6 Months Actual(' + @ApprovedYearName + ')'' ,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 AND PB.BudgetType = ''2nd_6months_actual''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END),
        5
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Original'',
        SUM(CASE WHEN PB.BudgetType = ''Original''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END),
        6
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' WHERE PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Actual(' + @ActualYearName + ')'' ,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + '
                 AND PB.BudgetType = ''Actual''
                 THEN ISNULL(PB.BLQuantityMT,0) ELSE 0 END),
        7
    FROM ProductBudgets PB
    INNER JOIN FiscalYears FY ON FY.Id = PB.GLFiscalYearId
    WHERE FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND PB.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

) X
ORDER BY SortOrder;
';

EXEC sp_executesql @SQL;
";

                //---------------------------------------------------
                // Execute
                //---------------------------------------------------
                SqlDataAdapter da = new SqlDataAdapter(sql, conn);
                da.SelectCommand.Transaction = transaction;

                da.SelectCommand.Parameters.AddWithValue("@BId", branchId);
                da.SelectCommand.Parameters.AddWithValue("@FiscalYearId", fiscalYearId);

                da.Fill(dataTable);

                //---------------------------------------------------
                // Mapping
                //---------------------------------------------------
                var modelList = dataTable.AsEnumerable()
                    .Select(r => new PieChartVM
                    {
                        Particular = r["Particular"]?.ToString(),
                        TotalValue = r["TotalQuantity"] != DBNull.Value
                            ? Convert.ToDecimal(r["TotalQuantity"])
                            : 0
                    })
                    .ToList();

                result.Status = "Success";
                result.Message = "Data retrieved successfully";
                result.DataVM = modelList;

                return await Task.FromResult(result);
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> GetSaleBudgetPieChart(CommonVM vm,string[] conditionalFields,string[] conditionalValues,SqlConnection conn = null,SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();

            ResultVM result = new ResultVM
            {
                Status = "Fail",
                Message = "Error"
            };

            try
            {
                if (conn == null)
                    throw new Exception("Database connection fail!");

                //---------------------------------------------------
                // Parameters
                //---------------------------------------------------
                int branchId = 1;
                int fiscalYearId = 6;

                if (conditionalValues != null)
                {
                    if (conditionalValues.Length > 0 && !string.IsNullOrWhiteSpace(conditionalValues[0]))
                        int.TryParse(conditionalValues[0], out branchId);

                    if (conditionalValues.Length > 1 && !string.IsNullOrWhiteSpace(conditionalValues[1]))
                        int.TryParse(conditionalValues[1], out fiscalYearId);
                }

                //---------------------------------------------------
                // SQL (YOUR SALE QUERY WRAPPED PROPERLY)
                //---------------------------------------------------
                string sql = @"
DECLARE @BranchId INT = @BId;
DECLARE @FiscalYearId INT = @FiscalYearIdParam;

DECLARE @Year INT;
DECLARE @EstimatedYear INT;
DECLARE @ApprovedYear INT;
DECLARE @ActualYear INT;

DECLARE @EstimatedYearName NVARCHAR(50);
DECLARE @ApprovedYearName NVARCHAR(50);
DECLARE @ActualYearName NVARCHAR(50);

DECLARE @SQL NVARCHAR(MAX);

------------------------------------------------------------
-- Base Year
------------------------------------------------------------
SELECT @Year = [Year]
FROM FiscalYears
WHERE Id = @FiscalYearId;

SET @EstimatedYear = @Year;
SET @ApprovedYear  = @Year - 1;
SET @ActualYear    = @Year - 2;

------------------------------------------------------------
-- Year Names
------------------------------------------------------------
SELECT @EstimatedYearName = YearName FROM FiscalYears WHERE [Year] = @EstimatedYear;
SELECT @ApprovedYearName  = YearName FROM FiscalYears WHERE [Year] = @ApprovedYear;
SELECT @ActualYearName    = YearName FROM FiscalYears WHERE [Year] = @ActualYear;

------------------------------------------------------------
-- Dynamic SQL
------------------------------------------------------------
SET @SQL = '

SELECT 
    Particular,
    TotalSales
FROM
(
    SELECT 
        ''Estimated(' + @EstimatedYearName + ')'' AS Particular,
        SUM(CASE WHEN FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + '
                 THEN ISNULL(D.TotalValueTK_LAC,0) ELSE 0 END) AS TotalSales,
        1 AS SortOrder
    FROM SaleDetails D
    INNER JOIN SaleHeaders SH ON SH.Id = D.SaleHeaderId
    INNER JOIN FiscalYears FY ON FY.Id = SH.FiscalYearId
    WHERE FY.[Year] = ' + CAST(@EstimatedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND SH.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Revised(' + @ApprovedYearName + ')'',
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 THEN ISNULL(D.TotalValueTK_LAC,0) ELSE 0 END),
        2
    FROM SaleDetails D
    INNER JOIN SaleHeaders SH ON SH.Id = D.SaleHeaderId
    INNER JOIN FiscalYears FY ON FY.Id = SH.FiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND SH.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''1st 6 Months Actual(' + @ApprovedYearName + ')'',
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 THEN ISNULL(D.TotalValueTK_LAC,0) ELSE 0 END),
        3
    FROM SaleDetails D
    INNER JOIN SaleHeaders SH ON SH.Id = D.SaleHeaderId
    INNER JOIN FiscalYears FY ON FY.Id = SH.FiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND SH.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''2nd 6 Months Actual(' + @ApprovedYearName + ')'',
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
                 THEN ISNULL(D.TotalValueTK_LAC,0) ELSE 0 END),
        4
    FROM SaleDetails D
    INNER JOIN SaleHeaders SH ON SH.Id = D.SaleHeaderId
    INNER JOIN FiscalYears FY ON FY.Id = SH.FiscalYearId
    WHERE FY.[Year] = ' + CAST(@ApprovedYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND SH.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Original'',
        SUM(ISNULL(D.TotalValueTK_LAC,0)),
        5
    FROM SaleDetails D
    INNER JOIN SaleHeaders SH ON SH.Id = D.SaleHeaderId
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' WHERE SH.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

    UNION ALL

    SELECT 
        ''Actual(' + @ActualYearName + ')'',
        SUM(CASE WHEN FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + '
                 THEN ISNULL(D.TotalValueTK_LAC,0) ELSE 0 END),
        6
    FROM SaleDetails D
    INNER JOIN SaleHeaders SH ON SH.Id = D.SaleHeaderId
    INNER JOIN FiscalYears FY ON FY.Id = SH.FiscalYearId
    WHERE FY.[Year] = ' + CAST(@ActualYear AS NVARCHAR) + '
    ' + CASE WHEN @BranchId IS NOT NULL
        THEN ' AND SH.BranchId = ' + CAST(@BranchId AS NVARCHAR) ELSE '' END + '

) X
ORDER BY SortOrder;
';

EXEC sp_executesql @SQL;
";

                //---------------------------------------------------
                // Execute
                //---------------------------------------------------
                SqlDataAdapter da = new SqlDataAdapter(sql, conn);
                da.SelectCommand.Transaction = transaction;

                da.SelectCommand.Parameters.AddWithValue("@BId", branchId);
                da.SelectCommand.Parameters.AddWithValue("@FiscalYearIdParam", fiscalYearId);

                da.Fill(dataTable);

                //---------------------------------------------------
                // Mapping
                //---------------------------------------------------
                var modelList = dataTable.AsEnumerable()
                    .Select(r => new PieChartVM
                    {
                        Particular = r["Particular"]?.ToString(),
                        TotalValue = r["TotalSales"] != DBNull.Value
                            ? Convert.ToDecimal(r["TotalSales"])
                            : 0
                    })
                    .ToList();

                result.Status = "Success";
                result.Message = "Data retrieved successfully";
                result.DataVM = modelList;

                return await Task.FromResult(result);
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }
    }
}
