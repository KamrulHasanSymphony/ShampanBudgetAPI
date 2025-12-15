using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class ChargeDetailVM :AuditVM
    {
        public int Id { get; set; }
        public int ChargeHeaderId { get; set; }
        public int ProductId { get; set; }
        public string? ProductName { get; set; }
        public decimal? CIFCharge { get; set; }
        public decimal? ExchangeRateUsd { get; set; }
        public decimal? InsuranceRate { get; set; }
        public decimal? BankCharge { get; set; }
        public decimal? OceanLoss { get; set; }
        public decimal? CPACharge { get; set; }
        public decimal? HandelingCharge { get; set; }
        public decimal? LightCharge { get; set; }
        public decimal? Survey { get; set; }
        public decimal? CostLiterExImport { get; set; }
        public decimal? ExERLRate { get; set; }
        public decimal? DutyPerLiter { get; set; }
        public decimal? Refined { get; set; }
        public decimal? Crude { get; set; }
        public decimal? SDRate { get; set; }
        public decimal? DutyInTariff { get; set; }
        public decimal? ATRate { get; set; }
        public decimal? AITRate { get; set; }
        public decimal? VATRate { get; set; }
        public decimal? ConversionFactorFixedValue { get; set; }
        public decimal? VATRateFixed { get; set; }
        public decimal? RiverDues { get; set; }
    }
}
