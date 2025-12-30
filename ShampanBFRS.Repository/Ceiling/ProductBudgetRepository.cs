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
AND ChargeGroup = @ChargeGroup 

";
                SqlCommand checkCommand = new SqlCommand(checkQuery, conn, transaction);
                checkCommand.Parameters.Add("@BranchId", SqlDbType.NVarChar).Value = objMaster.BranchId;
                checkCommand.Parameters.Add("@GLFiscalYearId", SqlDbType.NVarChar).Value = objMaster.GLFiscalYearId;
                checkCommand.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                checkCommand.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = objMaster.BudgetType;
                checkCommand.Parameters.Add("@ChargeGroup", SqlDbType.NVarChar).Value = objMaster.ChargeGroup;
                count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    throw new Exception("Already Exists!");
                }

                string TempTable = @"
SELECT TOP 0 *
INTO #ProductBudgetTemp
FROM ProductBudgets;
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
,TotalDutyVat = 0
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
T.ProcessFeeRate                 = cd.ProcessFeeRate,
T.RcoTreatmentFee                = cd.RcoTreatmentFee,
T.RcoTreatmentFeeRate            = cd.RcoTreatmentFeeRate,
T.AbpTreatmentFee                = cd.AbpTreatmentFee,
T.AbpTreatmentFeeRate            = cd.AbpTreatmentFeeRate,
T.ProductImprovementFee          = cd.ProductImprovementFee

FROM #ProductBudgetTemp T
INNER JOIN Products P ON P.Id = T.ProductId
INNER JOIN ChargeDetails cd ON P.Id = cd.ProductId
INNER JOIN ChargeHeaders ch ON ch.Id = cd.ChargeHeaderId and ch.ChargeGroup=T.ChargeGroup
;

";

                if (objMaster.ChargeGroup.ToLower() == "importedrefined")
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
                else if (objMaster.ChargeGroup.ToLower() == "localrefined")
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
                else if (objMaster.ChargeGroup.ToLower() == "ImportedCrude".ToLower())
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
,ServiceChargeValueUsd = FreightUsd * (ServiceCharge / 100) * 1.05
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 ServiceChargeBdt = ServiceChargeValueUsd * ExchangeRateUsd
,LightChargeValueUsd = LightCharge * BLQuantityMT * 1.05
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
LightChargeValue = LightChargeValueUsd * ExchangeRateUsd
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
CfrPriceUsd = FobValueUsd + FreightUsd + LightChargeValueUsd + ServiceChargeValueUsd
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
CfrPriceBBL = CfrPriceUsd / BLQuantityBBL
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CfrPriceBdt = FobValueBdt + FreightBdt + LightChargeValue + ServiceChargeBdt
,DutyValue = FobValueBdt * (DutyPerLiter/100)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 VATValue = (FobValueBdt + DutyValue) * (VATRate/100)
,ATValue = (FobValueBdt + DutyValue) * (ATRate/100)
,AITValue = FobValueBdt * (AITRate/100)
,ArrearDuty = 1 * ProductionBBL * ConversionFactorFixedValue
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 TotalDutyVat = DutyValue + VATValue + ATValue + AITValue + ArrearDuty
,InsuranceValue = FobValueBdt * (InsuranceRate/100) * 1.15
,BankChargeValue = FobValueBdt * (BankCharge/100) * 1.15
,OceanLossValue = FobValueBdt * (OceanLoss/100)
,CPAChargeValue = BLQuantityMT * CPACharge * ExchangeRateUsd * 1.15
,HandelingChargeValue = BLQuantityBBL * HandelingCharge * 1.15
,SurveyValue = Survey * BLQuantityBBL * ConversionFactorFixedValue
,ProcessFeeValue = ProcessQuantityBBL * (ProcessFeeRate/100) * ProcessFee
,RcoTreatmentFeeValue = ProcessQuantityBBL * (RcoTreatmentFeeRate/100) * RcoTreatmentFee
,AbpTreatmentFeeValue = ProcessQuantityBBL * (AbpTreatmentFeeRate/100) * AbpTreatmentFee
,ProductImprovementFeeValue = ProductImprovementFee * ProcessQuantityBBL

where BLQuantityMT>0
;


update #ProductBudgetTemp set 
 TotalCost = CfrPriceBdt + TotalDutyVat + InsuranceValue + BankChargeValue + OceanLossValue + CPAChargeValue + HandelingChargeValue + SurveyValue 
			+ ProcessFeeValue + RcoTreatmentFeeValue + AbpTreatmentFeeValue + ProductImprovementFeeValue
where BLQuantityMT>0
;

";

                    #endregion
                }
                else if (objMaster.ChargeGroup.ToLower() == "ImportedCrude".ToLower())
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
,ServiceChargeValueUsd = FreightUsd * (ServiceCharge / 100) * 1.05
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 ServiceChargeBdt = ServiceChargeValueUsd * ExchangeRateUsd
,LightChargeValueUsd = LightCharge * BLQuantityMT * 1.05
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
LightChargeValue = LightChargeValueUsd * ExchangeRateUsd
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
CfrPriceUsd = FobValueUsd + FreightUsd + LightChargeValueUsd + ServiceChargeValueUsd
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
CfrPriceBBL = CfrPriceUsd / BLQuantityBBL
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 CfrPriceBdt = FobValueBdt + FreightBdt + LightChargeValue + ServiceChargeBdt
,DutyValue = FobValueBdt * (DutyPerLiter/100)
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 VATValue = (FobValueBdt + DutyValue) * (VATRate/100)
,ATValue = (FobValueBdt + DutyValue) * (ATRate/100)
,AITValue = FobValueBdt * (AITRate/100)
,ArrearDuty = 1 * ProductionBBL * ConversionFactorFixedValue
where BLQuantityMT>0
;

update #ProductBudgetTemp set 
 TotalDutyVat = DutyValue + VATValue + ATValue + AITValue + ArrearDuty
,InsuranceValue = FobValueBdt * (InsuranceRate/100) * 1.15
,BankChargeValue = FobValueBdt * (BankCharge/100) * 1.15
,OceanLossValue = FobValueBdt * (OceanLoss/100)
,CPAChargeValue = BLQuantityMT * CPACharge * ExchangeRateUsd * 1.15
,HandelingChargeValue = BLQuantityBBL * HandelingCharge * 1.15
,SurveyValue = Survey * BLQuantityBBL * ConversionFactorFixedValue
,ProcessFeeValue = ProcessQuantityBBL * (ProcessFeeRate/100) * ProcessFee
,RcoTreatmentFeeValue = ProcessQuantityBBL * (RcoTreatmentFeeRate/100) * RcoTreatmentFee
,AbpTreatmentFeeValue = ProcessQuantityBBL * (AbpTreatmentFeeRate/100) * AbpTreatmentFee
,ProductImprovementFeeValue = ProductImprovementFee * ProcessQuantityBBL

where BLQuantityMT>0
;


update #ProductBudgetTemp set 
 TotalCost = CfrPriceBdt + TotalDutyVat + InsuranceValue + BankChargeValue + OceanLossValue + CPAChargeValue + HandelingChargeValue + SurveyValue 
			+ ProcessFeeValue + RcoTreatmentFeeValue + AbpTreatmentFeeValue + ProductImprovementFeeValue
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
,TotalDutyVat
,ProcessFeeRate
,RcoTreatmentFeeRate
,AbpTreatmentFeeRate
,ProductImprovementFee
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
,TotalDutyVat
,ProcessFeeRate
,RcoTreatmentFeeRate
,AbpTreatmentFeeRate
,ProductImprovementFee
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
                    command.Parameters.Add("@GLFiscalYearId", SqlDbType.NChar).Value = objMaster.GLFiscalYearId;
                    command.Parameters.Add("@BudgetSetNo", SqlDbType.NVarChar).Value = objMaster.BudgetSetNo;
                    command.Parameters.Add("@BudgetType", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.BudgetType) ? (object)DBNull.Value : objMaster.BudgetType.Trim();
                    command.Parameters.Add("@ChargeGroup", SqlDbType.NVarChar).Value = string.IsNullOrEmpty(objMaster.ChargeGroup) ? (object)DBNull.Value : objMaster.ChargeGroup.Trim();
                    command.Parameters.Add("@BranchId", SqlDbType.Int).Value = objMaster.BranchId;
                    command.Parameters.Add("@CompanyId", SqlDbType.Int).Value = objMaster.CompanyId;
                    command.Parameters.Add("@ProductId", SqlDbType.Int).Value = item.ProductId;
                    command.Parameters.Add("@BLQuantityMT", SqlDbType.Int).Value = item.BLQuantityMT;
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

        public async Task<ResultVM> ProductBudgetList(string[] conditionalFields, string[] conditionalValues, ProductBudgetMasterVM vm = null,
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
ROW_NUMBER() OVER (ORDER BY p.Id) AS Serial,
ISNULL(PB.Id, 0) AS Id,
ISNULL(PB.CompanyId, 0) AS CompanyId,
ISNULL(PB.BranchId, 0) AS BranchId,
ISNULL(PB.GLFiscalYearId, 0) AS GLFiscalYearId,
ISNULL(PB.BudgetSetNo, 0) AS BudgetSetNo,
ISNULL(PB.BudgetType, '') AS BudgetType,
ISNULL(p.Code, '') AS ProductCode,
ISNULL(p.Name, '') AS ProductName,
ISNULL(p.ProductGroupId, 0) AS ProductGroupId,
ISNULL(p.Id, 0) AS ProductId,
ISNULL(PB.ConversionFactor, 0) AS ConversionFactor,
ISNULL(PB.BLQuantityMT, 0) AS BLQuantityMT,
ISNULL(PB.BLQuantityBBL, 0) AS BLQuantityBBL,
ISNULL(PB.ReceiveQuantityMT, 0) AS ReceiveQuantityMT,
ISNULL(PB.ReceiveQuantityBBL, 0) AS ReceiveQuantityBBL,
ISNULL(PB.CIFCharge, 0) AS CIFCharge,
ISNULL(PB.CifUsdValue, 0) AS CifUsdValue,
ISNULL(PB.CifBdt, 0) AS CifBdt,
ISNULL(PB.InsuranceRate, 0) AS InsuranceRate,
ISNULL(PB.InsuranceValue, 0) AS InsuranceValue,
ISNULL(PB.BankCharge, 0) AS BankCharge,
ISNULL(PB.BankChargeValue, 0) AS BankChargeValue,
ISNULL(PB.OceanLoss, 0) AS OceanLoss,
ISNULL(PB.OceanLossValue, 0) AS OceanLossValue,
ISNULL(PB.CPACharge, 0) AS CPACharge,
ISNULL(PB.CPAChargeValue, 0) AS CPAChargeValue,
ISNULL(PB.HandelingCharge, 0) AS HandelingCharge,
ISNULL(PB.HandelingChargeValue, 0) AS HandelingChargeValue,
ISNULL(PB.LightCharge, 0) AS LightCharge,
ISNULL(PB.LightChargeValue, 0) AS LightChargeValue,
ISNULL(PB.Survey, 0) AS Survey,
ISNULL(PB.SurveyValue, 0) AS SurveyValue,
ISNULL(PB.TotalCost, 0) AS TotalCost,
ISNULL(PB.CostBblExImport, 0) AS CostBblExImport,
ISNULL(PB.CostLiterExImport, 0) AS CostLiterExImport,
ISNULL(PB.CostLiterExImportValue, 0) AS CostLiterExImportValue,
ISNULL(PB.Crude, 0) AS Crude,
ISNULL(PB.Refined, 0) AS Refined,
ISNULL(PB.ExchangeRateUsd, 0) AS ExchangeRateUsd,
ISNULL(PB.CostLiterExErl, 0) AS CostLiterExErl,
ISNULL(PB.ExERLRate, 0) AS ExERLRate,
ISNULL(PB.DutyPerLiter, 0) AS DutyPerLiter,
ISNULL(PB.DutyValue, 0) AS DutyValue,
ISNULL(PB.SDRate, 0) AS SDRate,
ISNULL(PB.SDValue, 0) AS SDValue,
ISNULL(PB.DutyOnTariffValuePerLiter, 0) AS DutyOnTariffValuePerLiter,
ISNULL(PB.DutyInTariff3, 0) AS DutyInTariff3,
ISNULL(PB.DutyInTariff2, 0) AS DutyInTariff2,
ISNULL(PB.DutyInTariff1, 0) AS DutyInTariff1,
ISNULL(PB.DutyInTariff, 0) AS DutyInTariff,
ISNULL(PB.ATRate, 0) AS ATRate,
ISNULL(PB.ATValue, 0) AS ATValue,
ISNULL(PB.VATRate, 0) AS VATRate,
ISNULL(PB.VATValue, 0) AS VATValue,
ISNULL(PB.VATPerLiterValue, 0) AS VATPerLiterValue,
ISNULL(PB.TotalCostAfterDuties, 0) AS TotalCostAfterDuties,
ISNULL(PB.VATExcludingExtraVAT, 0) AS VATExcludingExtraVAT,
ISNULL(PB.TotalCostVATExcluded, 0) AS TotalCostVATExcluded
FROM ChargeHeaders ch
LEFT JOIN ChargeDetails cd ON ch.Id = cd.ChargeHeaderId
LEFT JOIN Products p ON cd.ProductId = p.Id
LEFT JOIN ProductGroups pg ON pg.Id = p.ProductGroupId
LEFT JOIN ProductBudgets PB ON p.Id = PB.ProductId 
       AND PB.GLFiscalYearId = @GLFiscalYearId
       AND PB.BudgetType = @BudgetType
       AND ch.ChargeGroup = PB.ChargeGroup

WHERE 1 = 1

 ";

                if (vm.Id > 0)
                    query += " AND p.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                adapter.SelectCommand.Parameters.AddWithValue("@GLFiscalYearId", vm.GLFiscalYearId);
                adapter.SelectCommand.Parameters.AddWithValue("@BudgetType", vm.BudgetType);

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

        public async Task<ResultVM> ProductBudgeDistincttList(string[] conditionalFields, string[] conditionalValues, ProductBudgetMasterVM vm = null,
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


    }
}
