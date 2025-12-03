using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using System.Data.SqlClient;
using System.Data;
using ShampanBFRS.ViewModel.Ceiling;

namespace ShampanBFRS.Repository.Ceiling
{
    public class ProductBudgetRepository : CommonRepository
    {


        
        public async Task<ResultVM> ProductBudgetList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
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
    ,ISNULL(PB.CifUsdRate, 0) AS CifUsdRate
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
WHERE 1 = 1;

 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND PB.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
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



    }
}
