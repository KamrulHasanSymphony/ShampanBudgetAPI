using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using System.Data.SqlClient;
using System.Data;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.KendoCommon;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Options;

namespace ShampanBFRS.Repository.Ceiling
{
    public class ProductBudgetRepository : CommonRepository
    {

        public async Task<ResultVM> Insert(ProductBudgetMasterVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string sqlText = "";
                int count = 0;

                string checkQuery = @"
SELECT COUNT(Id) FROM ProductBudgets WHERE BranchId = @BranchId 
AND GLFiscalYearId = @GLFiscalYearId 
AND BudgetSetNo = @BudgetSetNo 
AND BudgetType = @BudgetType 

";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.GLFiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    throw new Exception("Already Exists!");
                }

                sqlText = @" 

SELECT TOP 0 *
INTO #ProductBudgetTemp
FROM ProductBudgets;

INSERT INTO #ProductBudgetTemp (

 CompanyId
,BranchId
,GLFiscalYearId
,ProductId
,BLQuantityMT
,BudgetSetNo
,BudgetType
--,TransactionType            
) VALUES 
(
 @CompanyId
,@BranchId
,@GLFiscalYearId
,@ProductId
,@BLQuantityMT
,@BudgetType
--,@TransactionType
 )


update #ProductBudgetTemp set 
 BLQuantityBBL = 0
,ReceiveQuantityMT = 0
,ReceiveQuantityBBL = 0
,CifUsdValue = 0
,CifBdt=0
,InsuranceValue=0
,BankChargeValue = 0
,BankChargeValue_R20 = 0
,OceanLossValue = 0
,CPAChargeValue = 0
,HandelingChargeValue= 0
,LightChargeValue = 0
,SurveyValue= 0
,TotalCost = 0
,CostBblExImport = 0
,CostLiterExImportValue = 0
,CostLiterExErl = 0
,DutyValue = 0
,DutyOnTariffValuePerLiter= 0
,SDValue = 0
,ATValue = 0
,VATValue = 0
,VATPerLiterValue = 0
,TotalCostAfterDuties = 0
,VATExcludingExtraVAT = 0
,TotalCostVATExcluded = 0
;

UPDATE T
SET 
    T.ConversionFactor      = P.ConversionFactor,
    T.CIFCharge             = P.CIFCharge,
    T.ExchangeRateUsd       = P.ExchangeRateUsd,
    T.InsuranceRate         = P.InsuranceRate,
    T.BankCharge            = P.BankCharge,
    T.OceanLoss             = P.OceanLoss,
    T.CPACharge             = P.CPACharge,
    T.HandelingCharge       = P.HandelingCharge,
    T.LightCharge           = P.LightCharge,
    T.Survey                = P.Survey,
    T.CostLiterExImport     = P.CostLiterExImport,
    T.ExERLRate             = P.ExERLRate,
    T.DutyPerLiter          = P.DutyPerLiter,
	T.Refined               = P.Refined,
    T.Crude                 = P.Crude,
    T.SDRate                = P.SDRate,
    T.DutyInTariff          = P.DutyInTariff,
    T.ATRate                = P.ATRate,
    T.VATRate               = P.VATRate
FROM #ProductBudgetTemp T
INNER JOIN Products P ON P.Id = T.ProductId;


update #ProductBudgetTemp set 
 BLQuantityBBL = BLQuantityMT * ConversionFactor
,ReceiveQuantityMT = BLQuantityMT
,ReceiveQuantityBBL = (BLQuantityMT * ConversionFactor)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CifUsdValue = ReceiveQuantityBBL * CIFCharge
,CifBdt=(ReceiveQuantityBBL * CIFCharge) * ExchangeRateUsd
,InsuranceValue=0
where BLQuantityMT>0

 ;

 update #ProductBudgetTemp set 
 BankChargeValue = CifBdt * (BankCharge / 100) * 1.15
,BankChargeValue_R20 = (CifBdt * BankCharge) * (100+0)/100
where BLQuantityMT>0

;

 
update #ProductBudgetTemp set 
 OceanLossValue = (CifBdt + BankChargeValue) * (OceanLoss/100)
,CPAChargeValue = BLQuantityMT * CPACharge
,HandelingChargeValue= ReceiveQuantityMT * HandelingCharge * 1.15
,LightChargeValue = 0
,SurveyValue= ReceiveQuantityMT * Survey
where BLQuantityMT>0

;

update #ProductBudgetTemp set 
 TotalCost = CifBdt + InsuranceValue + BankChargeValue + OceanLossValue 
 + CPAChargeValue + HandelingChargeValue + LightChargeValue + SurveyValue
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CostBblExImport = TotalCost / ReceiveQuantityBBL
,CostLiterExImportValue = (TotalCost / ReceiveQuantityBBL) / CostLiterExImport
,CostLiterExErl = 91.84 -- need to clear
,DutyValue = Refined * ExchangeRateUsd * (DutyPerLiter / 100) * (11.00 / 100)
where BLQuantityMT>0

;

update #ProductBudgetTemp set 
 DutyOnTariffValuePerLiter= ReceiveQuantityBBL * DutyValue * 159
,SDValue = BLQuantityBBL * SDRate * 159	-- need to clear
,ATValue = (ReceiveQuantityBBL * 159) * ( ExchangeRateUsd * Refined + DutyValue) * ATRate
,VATValue = (ReceiveQuantityBBL * 159) * ( ExchangeRateUsd * Refined + DutyValue) * VATRate
,VATPerLiterValue = (ExchangeRateUsd * Refined) * 1.1 * VATRate * (11.00/100)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 TotalCostAfterDuties = TotalCost + DutyOnTariffValuePerLiter + SDValue + ATValue + VATValue
,VATExcludingExtraVAT = VATValue
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
TotalCostVATExcluded = TotalCostAfterDuties - VATExcludingExtraVAT
where BLQuantityMT>0
;

insert into ProductBudgets(
 [CompanyId]
,[BranchId]
,[GLFiscalYearId]
,[BudgetSetNo]
,[BudgetType]
,[ProductId]
,[ConversionFactor]
,[CIFCharge]
,[BLQuantityMT]
,[BLQuantityBBL]
,[ReceiveQuantityMT]
,[ReceiveQuantityBBL]
,[ExchangeRateUsd]
,[CifUsdValue]
,[CifBdt]
,[InsuranceRate]
,[InsuranceValue]
,[BankCharge]
,[BankChargeValue]
,[BankChargeValue_R20]
,[OceanLoss]
,[OceanLossValue]
,[CPACharge]
,[CPAChargeValue]
,[HandelingCharge]
,[HandelingChargeValue]
,[LightCharge]
,[LightChargeValue]
,[Survey]
,[SurveyValue]
,[TotalCost]
,[CostBblExImport]
,[CostLiterExImport]
,[CostLiterExImportValue]
,[ExERLRate]
,[CostLiterExErl]
,[DutyPerLiter]
,[DutyValue]
,[Refined]
,[Crude]
,[SDRate]
,[SDValue]
,[DutyOnTariffValuePerLiter]
,[DutyInTariff3]
,[DutyInTariff2]
,[DutyInTariff1]
,[DutyInTariff]
,[ATRate]
,[ATValue]
,[VATRate]
,[VATValue]
,[VATPerLiterValue]
,[TotalCostAfterDuties]
,[VATExcludingExtraVAT]
,[TotalCostVATExcluded])

SELECT 
 [CompanyId]
,[BranchId]
,[GLFiscalYearId]
,[BudgetSetNo]
,[BudgetType]
,[ProductId]
,[ConversionFactor]
,[CIFCharge]
,[BLQuantityMT]
,[BLQuantityBBL]
,[ReceiveQuantityMT]
,[ReceiveQuantityBBL]
,[ExchangeRateUsd]
,[CifUsdValue]
,[CifBdt]
,[InsuranceRate]
,[InsuranceValue]
,[BankCharge]
,[BankChargeValue]
,[BankChargeValue_R20]
,[OceanLoss]
,[OceanLossValue]
,[CPACharge]
,[CPAChargeValue]
,[HandelingCharge]
,[HandelingChargeValue]
,[LightCharge]
,[LightChargeValue]
,[Survey]
,[SurveyValue]
,[TotalCost]
,[CostBblExImport]
,[CostLiterExImport]
,[CostLiterExImportValue]
,[ExERLRate]
,[CostLiterExErl]
,[DutyPerLiter]
,[DutyValue]
,[Refined]
,[Crude]
,[SDRate]
,[SDValue]
,[DutyOnTariffValuePerLiter]
,[DutyInTariff3]
,[DutyInTariff2]
,[DutyInTariff1]
,[DutyInTariff]
,[ATRate]
,[ATValue]
,[VATRate]
,[VATValue]
,[VATPerLiterValue]
,[TotalCostAfterDuties]
,[VATExcludingExtraVAT]
,[TotalCostVATExcluded]
FROM #ProductBudgetTemp

drop table #ProductBudgetTemp

";

                SqlCommand command = new SqlCommand();

                foreach (var item in objMaster.DetailList)
                {
                    command = new SqlCommand(sqlText, conn, transaction);
                    command.Parameters.Add("@GLFiscalYearId", SqlDbType.NChar).Value = objMaster.GLFiscalYearId;
                    command.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                    command.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.BudgetType) ? (object)DBNull.Value : objMaster.BudgetType.Trim();
                    command.Parameters.Add("@BranchId", SqlDbType.Int).Value = objMaster.BranchId;
                    command.Parameters.Add("@CompanyId", SqlDbType.Int).Value = objMaster.CompanyId;
                    command.Parameters.Add("@ProductId", SqlDbType.Int).Value = item.ProductId;
                    command.Parameters.Add("@BLQuantityMT", SqlDbType.Int).Value = item.BLQuantityMT;
                    ////command.Parameters.Add("@TransactionType", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.TransactionType) ? (object)DBNull.Value : objMaster.TransactionType.Trim();

                    command.ExecuteNonQuery();
                }

                //objMaster.Id = Convert.ToInt32(command.ExecuteScalar());

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                //result.Id = objMaster.Id.ToString();
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


        public async Task<ResultVM> ExitCheck(ProductBudgetVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                int count = 0;

                string checkQuery = @"
SELECT COUNT(Id) FROM ProductBudgets WHERE BranchId = @BranchId 
AND GLFiscalYearId = @GLFiscalYearId 
AND BudgetSetNo = @BudgetSetNo 
AND BudgetType = @BudgetType 

";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.GLFiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                result.Count = count;

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

        public async Task<ResultVM> ProductBudgetList(string[] conditionalFields, string[] conditionalValues, ProductBudgetVM vm = null,
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
 ISNULL(PB.Id, 0) AS Id
,ISNULL(PB.CompanyId, 0) AS CompanyId
,ISNULL(PB.BranchId, 0) AS BranchId
,ISNULL(PB.GLFiscalYearId, 0) AS GLFiscalYearId
,ISNULL(PB.BudgetSetNo, 0) AS BudgetSetNo
,ISNULL(PB.BudgetType, '') AS BudgetType
,ISNULL(PB.ProductId, 0) AS ProductId
,ISNULL(PB.ConversionFactor, 0) AS ConversionFactor
,ISNULL(PB.BLQuantityMT, 0) AS BLQuantityMT
,ISNULL(PB.BLQuantityBBL, 0) AS BLQuantityBBL
,ISNULL(PB.ReceiveQuantityMT, 0) AS ReceiveQuantityMT
,ISNULL(PB.ReceiveQuantityBBL, 0) AS ReceiveQuantityBBL
,ISNULL(PB.CIFCharge, 0) AS CIFCharge
,ISNULL(PB.CifBdt, 0) AS CifBdt
,ISNULL(PB.CIFCharge, 0) AS CIFCharge
,ISNULL(PB.CifUsdValue, 0) AS CifUsdValue
,ISNULL(PB.InsuranceRate, 0) AS InsuranceRate
,ISNULL(PB.InsuranceValue, 0) AS InsuranceValue
,ISNULL(PB.BankCharge, 0) AS BankCharge
,ISNULL(PB.BankChargeValue, 0) AS BankChargeValue
,ISNULL(PB.OceanLoss, 0) AS OceanLoss
,ISNULL(PB.OceanLossValue, 0) AS OceanLossValue
,ISNULL(PB.CPACharge, 0) AS CPACharge
,ISNULL(PB.CPAChargeValue, 0) AS CPAChargeValue
,ISNULL(PB.HandelingCharge, 0) AS HandelingCharge
,ISNULL(PB.HandelingChargeValue, 0) AS HandelingChargeValue
,ISNULL(PB.LightCharge, 0) AS LightCharge
,ISNULL(PB.LightChargeValue, 0) AS LightChargeValue
,ISNULL(PB.Survey, 0) AS Survey
,ISNULL(PB.SurveyValue, 0) AS SurveyValue
,ISNULL(PB.TotalCost, 0) AS TotalCost
,ISNULL(PB.CostBblExImport, 0) AS CostBblExImport
,ISNULL(PB.CostLiterExImport, 0) AS CostLiterExImport
,ISNULL(PB.CostLiterExErl, 0) AS CostLiterExErl
,ISNULL(PB.ExERLRate, 0) AS ExERLRate
,ISNULL(PB.DutyPerLiter, 0) AS DutyPerLiter
,ISNULL(PB.DutyValue, 0) AS DutyValue
,ISNULL(PB.SDRate, 0) AS SDRate
,ISNULL(PB.SDValue, 0) AS SDValue
,ISNULL(PB.DutyOnTariffValuePerLiter, 0) AS DutyOnTariffValuePerLiter
,ISNULL(PB.DutyInTariff3, 0) AS DutyInTariff3
,ISNULL(PB.DutyInTariff2, 0) AS DutyInTariff2
,ISNULL(PB.DutyInTariff1, 0) AS DutyInTariff1
,ISNULL(PB.DutyInTariff, 0) AS DutyInTariff
,ISNULL(PB.ATRate, 0) AS ATRate
,ISNULL(PB.ATValue, 0) AS ATValue
,ISNULL(PB.VATRate, 0) AS VATRate
,ISNULL(PB.VATValue, 0) AS VATValue
,ISNULL(PB.VATPerLiterValue, 0) AS VATPerLiterValue
,ISNULL(PB.TotalCostAfterDuties, 0) AS TotalCostAfterDuties
,ISNULL(PB.VATExcludingExtraVAT, 0) AS VATExcludingExtraVAT
,ISNULL(PB.TotalCostVATExcluded, 0) AS TotalCostVATExcluded
FROM ProductBudgets PB 
left outer join Products p on p.Id = PB.ProductId
left outer join ProductGroups pg on pg.Id = p.ProductGroupId

WHERE 1 = 1

 ";

                if (vm.Id > 0)
                    query += " AND PB.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm.Id > 0)
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new ProductBudgetVM
                {
                    Id = row.Field<int>("Id"),
                    CompanyId = row.Field<int?>("CompanyId"),
                    BranchId = row.Field<int?>("BranchId"),
                    GLFiscalYearId = row.Field<int?>("GLFiscalYearId"),
                    BudgetSetNo = row.Field<int?>("BudgetSetNo"),
                    BudgetType = row.Field<string>("BudgetType"),
                    ProductId = row.Field<int?>("ProductId"),

                    ConversionFactor = row.Field<decimal?>("ConversionFactor") ?? 0,
                    BLQuantityMT = row.Field<decimal?>("BLQuantityMT") ?? 0,
                    BLQuantityBBL = row.Field<decimal?>("BLQuantityBBL") ?? 0,
                    ReceiveQuantityMT = row.Field<decimal?>("ReceiveQuantityMT") ?? 0,
                    ReceiveQuantityBBL = row.Field<decimal?>("ReceiveQuantityBBL") ?? 0,
                    CIFCharge = row.Field<decimal?>("CIFCharge") ?? 0,
                    CifBdt = row.Field<decimal?>("CifBdt") ?? 0,
                    ExchangeRateUsd = row.Field<decimal?>("ExchangeRateUsd") ?? 0,
                    CifUsdValue = row.Field<decimal?>("CifUsdValue") ?? 0,
                    InsuranceRate = row.Field<decimal?>("InsuranceRate") ?? 0,
                    InsuranceValue = row.Field<decimal?>("InsuranceValue") ?? 0,
                    BankCharge = row.Field<decimal?>("BankCharge") ?? 0,
                    BankChargeValue = row.Field<decimal?>("BankChargeValue") ?? 0,
                    OceanLoss = row.Field<decimal?>("OceanLoss") ?? 0,
                    OceanLossValue = row.Field<decimal?>("OceanLossValue") ?? 0,
                    CPACharge = row.Field<decimal?>("CPACharge") ?? 0,
                    CPAChargeValue = row.Field<decimal?>("CPAChargeValue") ?? 0,
                    HandelingCharge = row.Field<decimal?>("HandelingCharge") ?? 0,
                    HandelingChargeValue = row.Field<decimal?>("HandelingChargeValue") ?? 0,
                    LightCharge = row.Field<decimal?>("LightCharge") ?? 0,
                    LightChargeValue = row.Field<decimal?>("LightChargeValue") ?? 0,
                    Survey = row.Field<decimal?>("Survey") ?? 0,
                    SurveyValue = row.Field<decimal?>("SurveyValue") ?? 0,
                    TotalCost = row.Field<decimal?>("TotalCost") ?? 0,
                    CostBblExImport = row.Field<decimal?>("CostBblExImport") ?? 0,
                    CostLiterExImport = row.Field<decimal?>("CostLiterExImport") ?? 0,
                    CostLiterExErl = row.Field<decimal?>("CostLiterExErl") ?? 0,
                    ExERLRate = row.Field<decimal?>("ExERLRate") ?? 0,
                    DutyPerLiter = row.Field<decimal?>("DutyPerLiter") ?? 0,
                    DutyValue = row.Field<decimal?>("DutyValue") ?? 0,
                    SDRate = row.Field<decimal?>("SDRate") ?? 0,
                    SDValue = row.Field<decimal?>("SDValue") ?? 0,
                    DutyOnTariffValuePerLiter = row.Field<decimal?>("DutyOnTariffValuePerLiter") ?? 0,
                    DutyInTariff3 = row.Field<decimal?>("DutyInTariff3") ?? 0,
                    DutyInTariff2 = row.Field<decimal?>("DutyInTariff2") ?? 0,
                    DutyInTariff1 = row.Field<decimal?>("DutyInTariff1") ?? 0,
                    DutyInTariff = row.Field<decimal?>("DutyInTariff") ?? 0,
                    ATRate = row.Field<decimal?>("ATRate") ?? 0,
                    ATValue = row.Field<decimal?>("ATValue") ?? 0,
                    VATRate = row.Field<decimal?>("VATRate") ?? 0,
                    VATValue = row.Field<decimal?>("VATValue") ?? 0,
                    VATPerLiterValue = row.Field<decimal?>("VATPerLiterValue") ?? 0,
                    TotalCostAfterDuties = row.Field<decimal?>("TotalCostAfterDuties") ?? 0,
                    VATExcludingExtraVAT = row.Field<decimal?>("VATExcludingExtraVAT") ?? 0,
                    TotalCostVATExcluded = row.Field<decimal?>("TotalCostVATExcluded") ?? 0
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

        public async Task<ResultVM> ProductBudgetListForNew(string[] conditionalFields, string[] conditionalValues, ProductBudgetVM vm = null,
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
ROW_NUMBER() OVER (ORDER BY p.Id) AS Serial
,ISNULL(PB.Id, 0) AS Id
,ISNULL(PB.CompanyId, 0) AS CompanyId
,ISNULL(PB.BranchId, 0) AS BranchId
,ISNULL(PB.GLFiscalYearId, 0) AS GLFiscalYearId
,ISNULL(PB.BudgetSetNo, 0) AS BudgetSetNo
,ISNULL(PB.BudgetType, '') AS BudgetType
,ISNULL(p.Code, 0) AS ProductCode
,ISNULL(p.Name, 0) AS ProductName
,ISNULL(P.ProductGroupId, 0) AS ProductGroupId
,ISNULL(P.Id, 0) AS ProductId
,ISNULL(PB.ConversionFactor, 0) AS ConversionFactor
,ISNULL(PB.BLQuantityMT, 0) AS BLQuantityMT
,ISNULL(PB.BLQuantityBBL, 0) AS BLQuantityBBL
,ISNULL(PB.ReceiveQuantityMT, 0) AS ReceiveQuantityMT
,ISNULL(PB.ReceiveQuantityBBL, 0) AS ReceiveQuantityBBL
,ISNULL(PB.CIFCharge, 0) AS CIFCharge
,ISNULL(PB.CifBdt, 0) AS CifBdt
,ISNULL(PB.CIFCharge, 0) AS CIFCharge
,ISNULL(PB.CifUsdValue, 0) AS CifUsdValue
,ISNULL(PB.InsuranceRate, 0) AS InsuranceRate
,ISNULL(PB.InsuranceValue, 0) AS InsuranceValue
,ISNULL(PB.BankCharge, 0) AS BankCharge
,ISNULL(PB.BankChargeValue, 0) AS BankChargeValue
,ISNULL(PB.OceanLoss, 0) AS OceanLoss
,ISNULL(PB.OceanLossValue, 0) AS OceanLossValue
,ISNULL(PB.CPACharge, 0) AS CPACharge
,ISNULL(PB.CPAChargeValue, 0) AS CPAChargeValue
,ISNULL(PB.HandelingCharge, 0) AS HandelingCharge
,ISNULL(PB.HandelingChargeValue, 0) AS HandelingChargeValue
,ISNULL(PB.LightCharge, 0) AS LightCharge
,ISNULL(PB.LightChargeValue, 0) AS LightChargeValue
,ISNULL(PB.Survey, 0) AS Survey
,ISNULL(PB.SurveyValue, 0) AS SurveyValue
,ISNULL(PB.TotalCost, 0) AS TotalCost
,ISNULL(PB.CostBblExImport, 0) AS CostBblExImport
,ISNULL(PB.CostLiterExImport, 0) AS CostLiterExImport
,ISNULL(PB.CostLiterExImportValue, 0) AS CostLiterExImportValue
,ISNULL(PB.Crude, 0) AS Crude
,ISNULL(PB.Refined, 0) AS Refined
,ISNULL(PB.ExchangeRateUsd, 0) AS ExchangeRateUsd
,ISNULL(PB.CostLiterExErl, 0) AS CostLiterExErl
,ISNULL(PB.ExERLRate, 0) AS ExERLRate
,ISNULL(PB.DutyPerLiter, 0) AS DutyPerLiter
,ISNULL(PB.DutyValue, 0) AS DutyValue
,ISNULL(PB.SDRate, 0) AS SDRate
,ISNULL(PB.SDValue, 0) AS SDValue
,ISNULL(PB.DutyOnTariffValuePerLiter, 0) AS DutyOnTariffValuePerLiter
,ISNULL(PB.DutyInTariff3, 0) AS DutyInTariff3
,ISNULL(PB.DutyInTariff2, 0) AS DutyInTariff2
,ISNULL(PB.DutyInTariff1, 0) AS DutyInTariff1
,ISNULL(PB.DutyInTariff, 0) AS DutyInTariff
,ISNULL(PB.ATRate, 0) AS ATRate
,ISNULL(PB.ATValue, 0) AS ATValue
,ISNULL(PB.VATRate, 0) AS VATRate
,ISNULL(PB.VATValue, 0) AS VATValue
,ISNULL(PB.VATPerLiterValue, 0) AS VATPerLiterValue
,ISNULL(PB.TotalCostAfterDuties, 0) AS TotalCostAfterDuties
,ISNULL(PB.VATExcludingExtraVAT, 0) AS VATExcludingExtraVAT
,ISNULL(PB.TotalCostVATExcluded, 0) AS TotalCostVATExcluded
FROM Products p  
left outer join ProductGroups pg on pg.Id = p.ProductGroupId
left outer join ProductBudgets PB on p.Id = PB.ProductId

WHERE 1 = 1

 ";

                if (vm.Id > 0)
                    query += " AND p.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm.Id > 0)
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var data = new GridEntity<ProductBudgetVM>();

                data = KendoGrid<ProductBudgetVM>.GetGridDataFromTable(dt);

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


    }
}
