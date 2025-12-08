using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.Ceiling
{
    public class ProductBudgetVM
    {

        public int Id { get; set; }
        public int? CompanyId { get; set; }
        public int? BranchId { get; set; }
        public int? GLFiscalYearId { get; set; }
        public int? BudgetSetNo { get; set; }
        public string BudgetType { get; set; }
        public int ProductGroupId { get; set; }
        public int? ProductId { get; set; }

        public decimal? ConversionFactor { get; set; }
        public decimal? BLQuantityMT { get; set; }
        public decimal? BLQuantityBBL { get; set; }
        public decimal? ReceiveQuantityMT { get; set; }
        public decimal? ReceiveQuantityBBL { get; set; }
        public decimal? CIFCharge { get; set; }
        public decimal? CifBdt { get; set; }
        public decimal? ExchangeRateUsd { get; set; }
        public decimal? CifUsdValue { get; set; }
        public decimal? InsuranceRate { get; set; }
        public decimal? InsuranceValue { get; set; }
        public decimal? BankCharge { get; set; }
        public decimal? BankChargeValue { get; set; }
        public decimal? OceanLoss { get; set; }
        public decimal? OceanLossValue { get; set; }
        public decimal? CPACharge { get; set; }
        public decimal? CPAChargeValue { get; set; }
        public decimal? HandelingCharge { get; set; }
        public decimal? HandelingChargeValue { get; set; }
        public decimal? LightCharge { get; set; }
        public decimal? LightChargeValue { get; set; }
        public decimal? Survey { get; set; }
        public decimal? SurveyValue { get; set; }
        public decimal? TotalCost { get; set; }
        public decimal? CostBblExImport { get; set; }
        public decimal? CostLiterExImport { get; set; }
        public decimal? CostLiterExImportValue { get; set; }
        public decimal? CostLiterExErl { get; set; }
        public decimal? ExERLRate { get; set; }
        public decimal? DutyPerLiter { get; set; }
        public decimal? DutyValue { get; set; }
        public decimal? Refined { get; set; }
        public decimal? Crude { get; set; }
        public decimal? SDRate { get; set; }
        public decimal? SDValue { get; set; }
        public decimal? DutyOnTariffValuePerLiter { get; set; }
        public decimal? DutyInTariff3 { get; set; }
        public decimal? DutyInTariff2 { get; set; }
        public decimal? DutyInTariff1 { get; set; }
        public decimal? DutyInTariff { get; set; }
        public decimal? ATRate { get; set; }
        public decimal? ATValue { get; set; }
        public decimal? VATRate { get; set; }
        public decimal? VATValue { get; set; }
        public decimal? VATPerLiterValue { get; set; }
        public decimal? TotalCostAfterDuties { get; set; }
        public decimal? VATExcludingExtraVAT { get; set; }
        public decimal? TotalCostVATExcluded { get; set; }
        public string ProductCode { get; set; }
        public string ProductName { get; set; }

    }
}
