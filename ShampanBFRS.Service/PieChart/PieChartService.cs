using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.PieChart;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Service.PieChart
{
    public class PieChartService
    {
        CommonRepository _commonRepo = new CommonRepository();
        public async Task<ResultVM> GetBudgetPieChart(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            PieChartRepository _repo = new PieChartRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                #region Fiscal Year Id By Date

                int FiscalYearId = _commonRepo.FiscalYearIdByDate(vm.CurrentDate, conn, transaction);
                vm.FiscalYearId = FiscalYearId.ToString();

                #endregion

                result = await _repo.GetBudgetPieChart(vm, conditionalFields, conditionalValues, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.ToString();
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

        public async Task<ResultVM> GetSalaryAllowancePieChart(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            PieChartRepository _repo = new PieChartRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                #region Fiscal Year Id By Date

                int FiscalYearId = _commonRepo.FiscalYearIdByDate(vm.CurrentDate, conn, transaction);
                vm.FiscalYearId = FiscalYearId.ToString();

                #endregion

                result = await _repo.GetSalaryAllowancePieChart(vm, conditionalFields, conditionalValues, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.ToString();
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

        public async Task<ResultVM> GetProductBudgetPieChart(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            PieChartRepository _repo = new PieChartRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                #region Fiscal Year Id By Date
                int FiscalYearId = _commonRepo.FiscalYearIdByDate(vm.CurrentDate, conn, transaction);
                vm.FiscalYearId = FiscalYearId.ToString();
                #endregion

                result = await _repo.GetProductBudgetPieChart(vm, conditionalFields, conditionalValues, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.ToString();
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

        public async Task<ResultVM> GetSaleBudgetPieChart(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            PieChartRepository _repo = new PieChartRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                #region Fiscal Year Id By Date
                int FiscalYearId = _commonRepo.FiscalYearIdByDate(vm.CurrentDate, conn, transaction);
                vm.FiscalYearId = FiscalYearId.ToString();
                #endregion

                result = await _repo.GetSaleBudgetPieChart(vm, conditionalFields, conditionalValues, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.ToString();
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
