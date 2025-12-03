using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data.SqlClient;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.Repository.Ceiling;

namespace ShampanBFRS.Service.Ceiling
{
    public class ProductBudgetService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(ProductBudgetVM model)
        {
            ProductRepository _repo = new ProductRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            ResultVM Proresult = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                #region Connection open

                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                #endregion

                //string CodeGroup = "Ceiling";
                //string CodeName = "Ceiling";

                if (model == null)
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.NotFoundForSave,
                    };
                }


                result = await new FiscalYearDetailRepository().List("", new[] { "FiscalYearId" }, new[] { model.GLFiscalYearId.ToString() }, null, conn, transaction);

                var FiscalYearDetailVM = (List<FiscalYearDetailVM>)result.DataVM;

                Proresult = await new ProductRepository().List(new[] { "P.Id" }, new[] { model.ProductId.ToString() }, null, conn, transaction);

                var ProductVMs = (List<ProductVM>)Proresult.DataVM;

                ProductVM pVM = new ProductVM();
                pVM = ProductVMs.FirstOrDefault();

                ProductBudgetVM pbvm = new ProductBudgetVM();

                pbvm.CompanyId = model.CompanyId;
                pbvm.BranchId = model.BranchId;
                pbvm.BudgetSetNo = model.BudgetSetNo;
                pbvm.BudgetType = model.BudgetType;
                pbvm.ProductId = model.ProductId;


                pbvm.ConversionFactor = pVM.ConversionFactor;

                pbvm.BLQuantityMT = 750000;

                pbvm.BLQuantityBBL = pbvm.BLQuantityMT * pVM.ConversionFactor;

                pbvm.ReceiveQuantityMT = pbvm.BLQuantityMT;
                pbvm.ReceiveQuantityBBL = pbvm.BLQuantityBBL;

                pbvm.CIFCharge = pVM.CIFCharge;

                pbvm.CifUsdValue = pbvm.ReceiveQuantityBBL * pVM.CIFCharge;

                pbvm.ExchangeRateUsd = pVM.ExchangeRateUsd;

                pbvm.CifBdt = pbvm.CifUsdValue * pVM.ExchangeRateUsd;

                pbvm.InsuranceRate = pVM.InsuranceRate;

                pbvm.InsuranceValue = 0; // how to calculate

                pbvm.BankCharge = pVM.BankCharge;

                decimal difCharge = 1.15m; // How

                pbvm.BankChargeValue = (pbvm.CifBdt * pVM.BankCharge) * difCharge;

                pbvm.OceanLoss = pVM.OceanLoss;
                pbvm.OceanLossValue = (pbvm.CifBdt + pbvm.BankChargeValue) * pbvm.OceanLoss;

                pbvm.CPACharge = pVM.CPACharge;
                pbvm.CPAChargeValue = pbvm.BLQuantityBBL * pVM.CPACharge;

                pbvm.HandelingCharge = pVM.HandelingCharge;
                pbvm.HandelingChargeValue = pbvm.ReceiveQuantityMT * pVM.HandelingCharge * difCharge;

                pbvm.LightCharge = pVM.LightCharge;
                pbvm.LightChargeValue = 0;

                pbvm.Survey = pVM.Survey;
                pbvm.SurveyValue = pbvm.ReceiveQuantityMT * pVM.Survey;

                pbvm.TotalCost = pbvm.CifBdt + pbvm.InsuranceValue + pbvm.BankChargeValue + pbvm.OceanLossValue + pbvm.CPAChargeValue +
                    pbvm.HandelingChargeValue + pbvm.LightChargeValue + pbvm.SurveyValue;

                pbvm.CostBblExImport = pbvm.TotalCost / pbvm.ReceiveQuantityBBL;

                pbvm.CostLiterExImport = pVM.CostLiterExImport;

                pbvm.CostLiterExImportValue = pbvm.CostBblExImport * pVM.CostLiterExImport;

                pbvm.ExERLRate = pVM.ExERLRate;

                pbvm.CostLiterExErl = pVM.ExERLRate; //  need to clear

                pbvm.DutyPerLiter = pVM.DutyPerLiter;
                pbvm.Refined = pVM.Refined;

                pbvm.DutyValue = pVM.Refined * pVM.ExchangeRateUsd * (pVM.DutyPerLiter / 100) * (11 / 100);

                pbvm.SDRate = pVM.SDRate;

                pbvm.DutyInTariff = pVM.DutyInTariff;

                //pbvm.

                pbvm.Crude = pVM.Crude;


                if (isNewConnection && result.Status == MessageModel.Success)
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                result.DataVM = model;

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection) transaction.Rollback();
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }




    }
}
