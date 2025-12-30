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
    public class BudgetRepository : CommonRepository
    {

        public async Task<ResultVM> Insert(BudgetHeaderVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
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
AND TransactionDate = @TransactionDate 
AND TransactionType = @TransactionType 

";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@FiscalYearId", SqlDbType.NVarChar).Value = objMaster.FiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                checkCommand.Parameters.Add("@TransactionDate", SqlDbType.NVarChar).Value = objMaster.TransactionDate;
                checkCommand.Parameters.Add("@TransactionType", SqlDbType.NVarChar).Value = objMaster.TransactionType;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    throw new Exception("Already Exists!");
                }

                string TempTable = @"
SELECT TOP 0 *
INTO #ProductBudgetTemp
FROM BudgetHeaders;
";

                #region TempInsert

                string TempInsert = @"
INSERT INTO #ProductBudgetTemp (

 CompanyId
,BranchId
,GLFiscalYearId
,ProductId
,BLQuantityMT
,BudgetSetNo
,BudgetType
,ChargeGroup
--,TransactionType            
) VALUES 
(
 @CompanyId
,@BranchId
,@GLFiscalYearId
,@ProductId
,@BLQuantityMT
,@BudgetSetNo
,@BudgetType
,@ChargeGroup
--,@TransactionType
 )
";

                #endregion

                #region Process

                sqlText = @" 
update #ProductBudgetTemp set 
 BLQuantityBBL = 0
,ReceiveQuantityMT = 0
,ReceiveQuantityBBL = 0
,CifUsdValue = 0
,CifBdt=0
,CifPriceValue=0
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
,DutyInTariff3 = 0
,DutyInTariff2 = 0
,DutyInTariff1 = 0
,AITValue = 0
,ArrearDuty = 0
,RiverDuesValue = 0
,CostBblValue = 0
,CostLiterValue = 0
,TariffRate = 0
,ProcessQuantityMT = 0
,ProcessQuantityBBL = 0
,ProductionMT = 0
,ProductionBBL = 0
,FobPriceBBL = 0
,FobPriceMT = 0
,FobValueUsd = 0
,FobValueBdt = 0
,FreightUsd = 0
,FreightBdt = 0
,FreightMT = 0
,FreightBBL = 0
,ServiceCharge = 0
,ServiceChargeValueUsd = 0
,ServiceChargeBdt = 0
,LightChargeValueUsd = 0
,CfrPriceUsd = 0
,CfrPriceBBL = 0
,CfrPriceBdt = 0
,ProcessFee = 0
,ProcessFeeValue = 0
,RcoTreatmentFee = 0
,RcoTreatmentFeeValue = 0
,AbpTreatmentFee = 0
,AbpTreatmentFeeValue = 0
,ProductImprovementFeeValue = 0
,FinancingCharge = 0
;

UPDATE T
SET 
T.ConversionFactor               = P.ConversionFactor,
T.CIFCharge                      = cd.CIFCharge,
T.ExchangeRateUsd                = cd.ExchangeRateUsd,
T.InsuranceRate                  = cd.InsuranceRate,
T.BankCharge                     = cd.BankCharge,
T.OceanLoss                      = cd.OceanLoss,
T.CPACharge                      = cd.CPACharge,
T.HandelingCharge                = cd.HandelingCharge,
T.LightCharge                    = cd.LightCharge,
T.Survey                         = cd.Survey,
T.CostLiterExImport              = cd.CostLiterExImport,
T.ExERLRate                      = cd.ExERLRate,
T.DutyPerLiter                   = cd.DutyPerLiter,
T.Refined                        = cd.Refined,
T.Crude                          = cd.Crude,
T.SDRate                         = cd.SDRate,
T.DutyInTariff                   = cd.DutyInTariff,
T.ATRate                         = cd.ATRate,
T.AITRate                        = cd.AITRate,
T.VATRateFixed                   = cd.VATRateFixed,
T.ConversionFactorFixedValue     = cd.ConversionFactorFixedValue,
T.VATRate                        = cd.VATRate,
T.RiverDues                      = cd.RiverDues,
T.TariffRate                     = cd.TariffRate,
T.FobPriceBBL                    = cd.FobPriceBBL,
T.FreightUsd                     = cd.FreightUsd,
T.ServiceCharge                  = cd.ServiceCharge,
T.ProcessFee                     = cd.ProcessFee,
T.RcoTreatmentFee                = cd.RcoTreatmentFee,
T.AbpTreatmentFee                = cd.AbpTreatmentFee

FROM #ProductBudgetTemp T
INNER JOIN Products P ON P.Id = T.ProductId
INNER JOIN ChargeDetails cd ON P.Id = cd.ProductId
INNER JOIN ChargeHeaders ch ON ch.Id = cd.ChargeHeaderId and ch.ChargeGroup=T.ChargeGroup
;

";

                if (objMaster.BudgetType.ToLower() == "importedrefined")
                {
                    #region Calculations    

                    sqlText += @"

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
 DutyValue = CifBdt * (DutyPerLiter / 100)
where BLQuantityMT>0
 ;

 update #ProductBudgetTemp set 
 VATValue = (CifBdt + DutyValue)*(VATRate/100)
,ATValue = (CifBdt + DutyValue)*(ATRate/100)
,AITValue = CifBdt *(AITRate/100)
,ArrearDuty = 1 * ReceiveQuantityBBL * ConversionFactorFixedValue
,HandelingChargeValue= HandelingCharge * ReceiveQuantityMT *  VATRateFixed
,RiverDuesValue= RiverDues * ExchangeRateUsd * ReceiveQuantityMT *  VATRateFixed
,SurveyValue= Survey * ReceiveQuantityBBL *  ConversionFactorFixedValue
,OceanLossValue= CifBdt * (OceanLoss/100)
,BankChargeValue = CifBdt * (BankCharge / 100) * VATRateFixed
where BLQuantityMT>0
 ;

update #ProductBudgetTemp set 
 TotalCost = CifBdt + DutyValue + VATValue + ATValue +  AITValue + ArrearDuty + HandelingChargeValue + RiverDuesValue + SurveyValue
 + OceanLossValue + BankChargeValue 
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CostBblValue = TotalCost / ReceiveQuantityBBL
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CostLiterValue = CostBblValue / ConversionFactorFixedValue
where BLQuantityMT>0
;

";

                    #endregion
                }
                else if (objMaster.BudgetType.ToLower() == "localrefined")
                {
                    #region Calculations    

                    sqlText += @"


update #ProductBudgetTemp set 
 BLQuantityBBL = BLQuantityMT * ConversionFactor
,ReceiveQuantityMT = BLQuantityMT
,ReceiveQuantityBBL = (BLQuantityMT * ConversionFactor)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
CifPriceValue=CIFCharge / ExchangeRateUsd * ConversionFactorFixedValue
where BLQuantityMT>0
 ;

 update #ProductBudgetTemp set 
 CifUsdValue = ReceiveQuantityBBL * CifPriceValue
,CifBdt= CIFCharge * ReceiveQuantityBBL * ConversionFactorFixedValue
where BLQuantityMT>0
 ;

update #ProductBudgetTemp set 
 VATValue = CifBdt*(VATRate/(100+VATRate))
,ArrearDuty = 1 * ReceiveQuantityBBL * ConversionFactorFixedValue
,HandelingChargeValue= HandelingCharge * ReceiveQuantityBBL *  ConversionFactorFixedValue
,SurveyValue= Survey * ReceiveQuantityBBL *  ConversionFactorFixedValue
where BLQuantityMT>0
 ;

update #ProductBudgetTemp set 
 TotalCost = CifBdt + VATValue + ArrearDuty + HandelingChargeValue + SurveyValue
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CostBblValue = TotalCost / ReceiveQuantityBBL
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CostLiterValue = CostBblValue / ConversionFactorFixedValue
where BLQuantityMT>0
;

";

                    #endregion
                }

                else if (objMaster.BudgetType.ToLower() == "ImportedCrude".ToLower())
                {
                    #region Calculations    

                    sqlText += @"

update #ProductBudgetTemp set 
 BLQuantityBBL = BLQuantityMT * ConversionFactor
,ReceiveQuantityMT = BLQuantityMT * (99.5 / 100)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
ReceiveQuantityBBL = BLQuantityBBL * (99.5 / 100)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 ProcessQuantityMT = ReceiveQuantityMT
,ProcessQuantityBBL = ReceiveQuantityBBL
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 ProductionMT = ProcessQuantityMT * (97.2 / 100)
,ProductionBBL = ProcessQuantityBBL * (97.2 / 100)
,FobPriceMT = FobPriceBBL * ConversionFactor
,FobValueUsd = FobPriceBBL * BLQuantityBBL
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 FobValueBdt = FobValueUsd * ExchangeRateUsd
,FreightBdt = FreightUsd * ExchangeRateUsd
,FreightMT = FreightUsd * BLQuantityMT
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 FreightBBL = FreightMT / ConversionFactor
,
where BLQuantityMT>0
;

";

                    #endregion
                }

                #region insert ProductBudgets

                sqlText += @"


insert into ProductBudgets(
 [CompanyId]
,[BranchId]
,[GLFiscalYearId]
,[BudgetSetNo]
,[BudgetType]
,ChargeGroup
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
,[AITRate]
,[AITValue]
,[VATRate]
,[VATValue]
,[VATPerLiterValue]
,[TotalCostAfterDuties]
,[VATExcludingExtraVAT]
,[TotalCostVATExcluded]
,[ConversionFactorFixedValue]
,[ArrearDuty]
,[VATRateFixed]
,[RiverDues]
,[RiverDuesValue]
,[CostBblValue]
,[CostLiterValue]
,CifPriceValue
,[TariffRate]
,[ProcessQuantityMT]
,[ProcessQuantityBBL]
,[ProductionMT]
,[ProductionBBL]
,[FobPriceBBL]
,[FobPriceMT]
,[FobValueUsd]
,[FobValueBdt]
,[FreightUsd]
,[FreightBdt]
,[FreightMT]
,[FreightBBL]
,[ServiceCharge]
,[ServiceChargeValueUsd]
,[ServiceChargeBdt]
,[LightChargeValueUsd]
,[CfrPriceUsd]
,[CfrPriceBBL]
,[CfrPriceBdt]
,[ProcessFee]
,[ProcessFeeValue]
,[RcoTreatmentFee]
,[RcoTreatmentFeeValue]
,[AbpTreatmentFee]
,[AbpTreatmentFeeValue]
,[ProductImprovementFeeValue]
,[FinancingCharge]
)

SELECT 
 [CompanyId]
,[BranchId]
,[GLFiscalYearId]
,[BudgetSetNo]
,[BudgetType]
,ChargeGroup
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
,[AITRate]
,[AITValue]
,[VATRate]
,[VATValue]
,[VATPerLiterValue]
,[TotalCostAfterDuties]
,[VATExcludingExtraVAT]
,[TotalCostVATExcluded]
,[ConversionFactorFixedValue]
,[ArrearDuty]
,[VATRateFixed]
,[RiverDues]
,[RiverDuesValue]
,[CostBblValue]
,[CostLiterValue]
,CifPriceValue
,[TariffRate]
,[ProcessQuantityMT]
,[ProcessQuantityBBL]
,[ProductionMT]
,[ProductionBBL]
,[FobPriceBBL]
,[FobPriceMT]
,[FobValueUsd]
,[FobValueBdt]
,[FreightUsd]
,[FreightBdt]
,[FreightMT]
,[FreightBBL]
,[ServiceCharge]
,[ServiceChargeValueUsd]
,[ServiceChargeBdt]
,[LightChargeValueUsd]
,[CfrPriceUsd]
,[CfrPriceBBL]
,[CfrPriceBdt]
,[ProcessFee]
,[ProcessFeeValue]
,[RcoTreatmentFee]
,[RcoTreatmentFeeValue]
,[AbpTreatmentFee]
,[AbpTreatmentFeeValue]
,[ProductImprovementFeeValue]
,[FinancingCharge]
FROM #ProductBudgetTemp

drop table #ProductBudgetTemp

";

                #endregion

                #endregion

                SqlCommand command = new SqlCommand();

                command = new SqlCommand(TempTable, conn, transaction);
                command.ExecuteNonQuery();

                foreach (var item in objMaster.DetailList)
                {
                    command = new SqlCommand(TempInsert, conn, transaction);
                    command.Parameters.Add("@GLFiscalYearId", SqlDbType.NChar).Value = objMaster.FiscalYearId;
                    command.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                    command.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.BudgetType) ? (object)DBNull.Value : objMaster.BudgetType.Trim();
                    command.Parameters.Add("@TransactionType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.TransactionType) ? (object)DBNull.Value : objMaster.TransactionType.Trim();
                    command.Parameters.Add("@BranchId", SqlDbType.Int).Value = objMaster.BranchId;
                    command.Parameters.Add("@CompanyId", SqlDbType.Int).Value = objMaster.CompanyId;
                   // command.Parameters.Add("@ProductId", SqlDbType.Int).Value = item.ProductId;
                    //command.Parameters.Add("@BLQuantityMT", SqlDbType.Int).Value = item.BLQuantityMT;
                    ////command.Parameters.Add("@TransactionType", SqlDbType.NChar).Value = string.IsNullOrEmpty(objMaster.TransactionType) ? (object)DBNull.Value : objMaster.TransactionType.Trim();

                    command.ExecuteNonQuery();
                }

                command = new SqlCommand(sqlText, conn, transaction);
                command.ExecuteNonQuery();

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


        public async Task<ResultVM> ExitCheck(BudgetHeaderVM objMaster, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                int count = 0;

                string checkQuery = @"
SELECT COUNT(Id) FROM BudgetHeaders WHERE BranchId = @BranchId 
AND FiscalYearId = @FiscalYearId 
AND BudgetSetNo = @BudgetSetNo 
AND BudgetType = @BudgetType
AND TransactionType = @TransactionType 


";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@FiscalYearId", SqlDbType.NVarChar).Value = objMaster.FiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                checkCommand.Parameters.Add("@TransactionType", SqlDbType.NVarChar).Value = objMaster.TransactionType;
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

        public async Task<ResultVM> BudgetList(string[] conditionalFields, string[] conditionalValues, BudgetHeaderVM vm = null,
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
,ISNULL(PB.Code, '') AS Code
,ISNULL(PB.TransactionDate, 0) AS TransactionDate
,ISNULL(PB.TransactionType, 0) AS TransactionType

,ISNULL(PB.IsPost, 0) AS IsPost
,ISNULL(PB.Remarks, 0) AS Remarks
,ISNULL(PB.IsActive,0) IsActive
,ISNULL(PB.IsArchive,0) IsArchive
,ISNULL(PB.CreatedBy,'') CreatedBy
,ISNULL(FORMAT(PB.CreatedOn,'yyyy-MM-dd HH:mm:ss'),'1900-01-01') CreatedOn
,ISNULL(PB.CreatedFrom,'') CreatedFrom
,ISNULL(PB.LastUpdateBy,'') LastUpdateBy
,ISNULL(FORMAT(PB.LastUpdateOn,'yyyy-MM-dd HH:mm:ss'),'1900-01-01') LastUpdateOn
,ISNULL(PB.LastUpdateFrom,'') LastUpdateFrom
,ISNULL(PB.PostedBy,'') PostedBy
,ISNULL(FORMAT(PB.PostedOn,'yyyy-MM-dd HH:mm:ss'),'1900-01-01') PostedOn
,ISNULL(PB.PostedFrom,'') PostedFrom

,ISNULL(PB.ApproveLevelRequired, 0) AS ApproveLevelRequired
,ISNULL(PB.CompletedApproveLevel, 0) AS CompletedApproveLevel
,ISNULL(PB.ApprovalStatus, '') AS ApprovalStatus
,ISNULL(PB.IsApproveFinal, 0) AS IsApproveFinal

FROM BudgetHeaders PB 

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

                var list = dt.AsEnumerable().Select(row => new BudgetHeaderVM
                {
                    Id = row.Field<int>("Id"),
                    CompanyId = row.Field<int?>("CompanyId"),
                    BranchId = row.Field<int?>("BranchId"),

                    BudgetSetNo = row.Field<int?>("BudgetSetNo"),
                    BudgetType = row.Field<string>("BudgetType"),


                    
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

        public async Task<ResultVM> BudgetListForNew(string[] conditionalFields, string[] conditionalValues, BudgetHeaderVM vm = null,
          SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"
 
 
 SELECT DISTINCT
 ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS Serial
,Sabres.Id SabreId
,COAs.Code  iBASCode
,COAs.Name  iBASName
,Sabres.Code SabreCode
,Sabres.[Name] SabreName
,0 InputTotal

FROM Sabres
LEFT OUTER JOIN COAs on COAs.Id=Sabres.COAId
INNER JOIN DepartmentSabres DS ON DS.SabreId = Sabres.Id
INNER JOIN UserInformations UI ON UI.DepartmentId = DS.DepartmentId

where 1=1

AND UI.UserName = 'erp'

 ";

                if (vm.Id > 0)
                    query += " AND p.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                adapter.SelectCommand.Parameters.AddWithValue("@FiscalYearId", vm.FiscalYearId);
                adapter.SelectCommand.Parameters.AddWithValue("@BudgetType", vm.BudgetType);

                if (vm.Id > 0)
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var data = new GridEntity<BudgetHeaderVM>();

                data = KendoGrid<BudgetHeaderVM>.GetGridDataFromTable(dt);

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

        public async Task<ResultVM> BudgeDistincttList(string[] conditionalFields, string[] conditionalValues, ProductBudgetMasterVM vm = null,
           SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"
 SELECT DISTINCT
 ISNULL(PB.Id, 0) AS Id
,ISNULL(PB.CompanyId, 0) AS CompanyId
,ISNULL(PB.BranchId, 0) AS BranchId
,ISNULL(PB.GLFiscalYearId, 0) AS GLFiscalYearId
,ISNULL(fy.YearName, 0) AS YearName
,ISNULL(PB.BudgetSetNo, 0) AS BudgetSetNo
,ISNULL(PB.BudgetType, '') AS BudgetType
,ISNULL(p.ProductGroupId, '') AS ProductGroupId
,ISNULL(pg.Name, '') AS ProductGroupName

FROM ProductBudgets PB 
left outer join FiscalYears fy on fy.Id = PB.GLFiscalYearId
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

                var list = dt.AsEnumerable().Select(row => new ProductBudgetMasterVM
                {
                    Id = row.Field<int>("Id"),
                    CompanyId = row.Field<int?>("CompanyId"),
                    BranchId = row.Field<int?>("BranchId"),
                    GLFiscalYearId = row.Field<int?>("GLFiscalYearId"),
                    ProductGroupId = row.Field<int?>("ProductGroupId"),
                    BudgetSetNo = row.Field<int?>("BudgetSetNo"),
                    BudgetType = row.Field<string>("BudgetType"),
                    YearName = row.Field<string>("YearName"),
                    ProductGroupName = row.Field<string>("ProductGroupName"),

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

        public async Task<ResultVM> GetGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                var data = new GridEntity<ProductBudgetMasterVM>();

                string sqlQuery = $@"
                    
                    SELECT COUNT(DISTINCT PB.ChargeGroup) AS totalcount
                FROM ProductBudgets PB 
               LEFT OUTER JOIN ChargeGroups CG 
    ON CG.ChargeGroupValue = PB.ChargeGroup
                WHERE 1=1
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ProductBudgetMasterVM>.FilterCondition(options.filter) + ")" : "");

                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"

            SELECT *
FROM (
    SELECT ROW_NUMBER() OVER(ORDER BY " +
        (options.sort.Count > 0
            ? "t." + options.sort[0].field + " " + options.sort[0].dir
            : "t.GLFiscalYearId DESC") + @") AS rowindex,
        t.CompanyId,
           t.BranchId,
           t.GLFiscalYearId,
           t.YearName,
           t.BudgetSetNo,
           t.BudgetType,
           t.ProductGroupId,
           t.ProductGroupName,
           t.ChargeGroup as ChargeGroup,
           t.ChargeGroupText as ChargeGroupText
    FROM (
        SELECT DISTINCT
               ISNULL(PB.CompanyId,0) AS CompanyId,
               ISNULL(PB.BranchId,0) AS BranchId,
               ISNULL(PB.GLFiscalYearId,0) AS GLFiscalYearId,
               ISNULL(fy.YearName,'') AS YearName,
               ISNULL(PB.BudgetSetNo,0) AS BudgetSetNo,
               ISNULL(PB.BudgetType,'') AS BudgetType,
               ISNULL(p.ProductGroupId,'') AS ProductGroupId,
               ISNULL(pg.Name,'') AS ProductGroupName,
               ISNULL(PB.ChargeGroup,0) AS ChargeGroup,
               ISNULL(CG.ChargeGroupText, '') AS ChargeGroupText
        FROM ProductBudgets PB
        LEFT OUTER JOIN ChargeGroups CG 
            ON CG.Id = PB.ChargeGroup
        LEFT JOIN FiscalYears fy 
            ON fy.Id = PB.GLFiscalYearId
        LEFT JOIN Products p 
            ON p.Id = PB.ProductId
        LEFT JOIN ProductGroups pg 
            ON pg.Id = p.ProductGroupId
        WHERE 1=1
        " + (options.filter.Filters.Count > 0
               ? " AND (" + GridQueryBuilder<ProductBudgetMasterVM>.FilterCondition(options.filter) + ")"
               : "") + @"
        " + ApplyConditions("", conditionalFields, conditionalValues, false) + @"
    ) t
) a
WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
        ";

                data = KendoGrid<ProductBudgetMasterVM>.GetTransactionalGridData_CMD(options, sqlQuery, "PB.BudgetType", conditionalFields, conditionalValues);

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

        public async Task<ResultVM> GetBudgetDataForDetailsNew(
            GridOptions options,
            string[] conditionalFields,
            string[] conditionalValues,
            SqlConnection conn,
            SqlTransaction transaction)
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
    }
}
