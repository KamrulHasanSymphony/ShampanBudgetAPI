using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.Repository.Reports
{
    public class ReportRepository : CommonRepository
    {
        public async Task<ResultVM> IncomeStatementAllReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
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
WHERE 1=1
and isnull(COA.IsNonOperatingIncome,''0'')=''1''
and c.BudgetType IN
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

        public async Task<ResultVM> ProductIncomeStatementReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
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
WHERE 1=1
and isnull(COA.IsNonOperatingIncome,''0'')=''1''
and c.BudgetType IN
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

        public async Task<ResultVM> CashFlowStatementReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
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
WHERE 1=1
and isnull(COA.IsNonOperatingIncome,''0'')=''1''
and c.BudgetType IN
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
        public async Task<ResultVM> PaymentToGovernmentStatementReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
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
WHERE 1=1
and isnull(COA.IsNonOperatingIncome,''0'')=''1''
and c.BudgetType IN
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

        public async Task<ResultVM> OperatingDetailStatementReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
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
WHERE 1=1
and isnull(COA.IsNonOperatingIncome,''0'')=''1''
and c.BudgetType IN
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

        public async Task<ResultVM> NonOperatingIncomeReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
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
WHERE 1=1
and isnull(COA.IsNonOperatingIncome,''0'')=''1''
and c.BudgetType IN
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

        public async Task<ResultVM> IncomeStatementReport(CommonVM vm, string[] conditionalFields, string[] conditionalValues, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"

DECLARE @BranchId INT = @BId;
DECLARE @GLFiscalYearId INT =@FYId;
DECLARE @BudgetType NVARCHAR(50) = @BType;

CREATE TABLE #IncomeStatement
(
    PARTICULARS          NVARCHAR(200),
    [ImportedRefined]   DECIMAL(18,2),
    [CrudeCondensate]    DECIMAL(18,2),
    [LubeOil]           DECIMAL(18,2),
    [LocalRefined]      DECIMAL(18,2)
);
  INSERT INTO #IncomeStatement
SELECT
    'CONVERSION (BBL/M.TON)',

    -- Imported Refined
    CASE 
        WHEN SUM(CASE WHEN ChargeGroup = 'ImportedRefined' THEN BLQuantityMT ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN ChargeGroup = 'ImportedRefined' THEN BLQuantityBBL ELSE 0 END)
            /
            SUM(CASE WHEN ChargeGroup = 'ImportedRefined' THEN BLQuantityMT ELSE 0 END),
        6)
    END,

    -- Crude + Condensate
    CASE 
        WHEN SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate') THEN BLQuantityMT ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate') THEN BLQuantityBBL ELSE 0 END)
            /
            SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate') THEN BLQuantityMT ELSE 0 END),
        6)
    END,

    0,

    -- Local Refined
    CASE 
        WHEN SUM(CASE WHEN ChargeGroup = 'LocalRefined' THEN BLQuantityMT ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN ChargeGroup = 'LocalRefined' THEN BLQuantityBBL ELSE 0 END)
            /
            SUM(CASE WHEN ChargeGroup = 'LocalRefined' THEN BLQuantityMT ELSE 0 END),
        6)
    END
FROM ProductBudgets
WHERE BranchId = @BranchId
  AND GLFiscalYearId = @GLFiscalYearId
  AND BudgetType = @BudgetType;

INSERT INTO #IncomeStatement
SELECT
    'B/L QUANTITY(M.TON)',

    SUM(CASE WHEN ChargeGroup = 'ImportedRefined'
             THEN BLQuantityMT ELSE 0 END),

    SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate')
             THEN BLQuantityMT ELSE 0 END),

    0,

    SUM(CASE WHEN ChargeGroup = 'LocalRefined'
             THEN BLQuantityMT ELSE 0 END)
FROM ProductBudgets
WHERE BranchId = @BranchId
  AND GLFiscalYearId = @GLFiscalYearId
  AND BudgetType = @BudgetType;

  INSERT INTO #IncomeStatement
SELECT
    'B/L QUANTITY(BBL)',

    SUM(CASE WHEN ChargeGroup = 'ImportedRefined'
             THEN BLQuantityBBL ELSE 0 END),

    SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate')
             THEN BLQuantityBBL ELSE 0 END),

    0,

    SUM(CASE WHEN ChargeGroup = 'LocalRefined'
             THEN BLQuantityBBL ELSE 0 END)
FROM ProductBudgets
WHERE BranchId = @BranchId
  AND GLFiscalYearId = @GLFiscalYearId
  AND BudgetType = @BudgetType;

  INSERT INTO #IncomeStatement
SELECT
    'CIF/C&F PRICE' AS PARTICULARS,

    -- Imported Refined
    CASE 
        WHEN SUM(CASE WHEN ChargeGroup = 'ImportedRefined' THEN BLQuantityBBL ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN ChargeGroup = 'ImportedRefined' THEN CifUsdValue ELSE 0 END)
            /
            SUM(CASE WHEN ChargeGroup = 'ImportedRefined' THEN BLQuantityBBL ELSE 0 END),
        6)
    END AS ImportedRefined,

    -- Imported Crude + Condensate
    SUM(CASE WHEN ChargeGroup IN ('ImportedCrude')
             THEN CfrPriceUsd ELSE 0 END) AS CrudeCondensate,

    -- Lube Oil
    0 AS LubeOil,

    -- Local Refined
    CASE 
        WHEN SUM(CASE WHEN ChargeGroup = 'LocalRefined' THEN ReceiveQuantityBBL ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN ChargeGroup = 'LocalRefined' THEN CifUsdValue ELSE 0 END)
            /
            SUM(CASE WHEN ChargeGroup = 'LocalRefined' THEN ReceiveQuantityBBL ELSE 0 END),
        6)
    END AS LocalRefined

FROM ProductBudgets
WHERE BranchId = @BranchId
  AND GLFiscalYearId = @GLFiscalYearId
  AND BudgetType = @BudgetType;

  INSERT INTO #IncomeStatement
SELECT
    'LANDED COST EX IMPORT' AS PARTICULARS,

    -- Imported Refined
    ROUND(
        SUM(CASE WHEN ChargeGroup = 'ImportedRefined'
                 THEN TotalCost ELSE 0 END) / 100000.0,
    6) AS ImportedRefined,

    -- Imported Crude + Condensate
    ROUND(
        (
            SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate')
                     THEN TotalCost ELSE 0 END)
          + SUM(CASE WHEN ChargeGroup IN ('ImportedCrude','Condensate')
                     THEN TotalCost ELSE 0 END)
        ) / 100000.0,
    6) AS CrudeCondensate,

    -- Lube Oil
    0 AS LubeOil,

    -- Local Refined
    ROUND(
        SUM(CASE WHEN ChargeGroup = 'LocalRefined'
                 THEN TotalCost ELSE 0 END) / 100000.0,
    6) AS LocalRefined

FROM ProductBudgets
WHERE BranchId = @BranchId
  AND GLFiscalYearId = @GLFiscalYearId
  AND BudgetType = @BudgetType


  SELECT * FROM #IncomeStatement

  drop table #IncomeStatement

";

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand.Parameters.AddWithValue("@FYId", vm.YearId);

                if (!string.IsNullOrEmpty(vm.BranchId))
                    adapter.SelectCommand.Parameters.AddWithValue("@BId", vm.BranchId);

                if (!string.IsNullOrEmpty(vm.BudgetType))
                    adapter.SelectCommand.Parameters.AddWithValue("@BType", vm.BudgetType);

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
