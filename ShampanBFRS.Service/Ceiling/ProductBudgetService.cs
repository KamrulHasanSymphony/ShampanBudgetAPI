using ShampanBFRS.Repository.Ceiling;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SalaryAllowance;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Service.Ceiling
{
    public class ProductBudgetService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(ProductBudgetMasterVM model)
        {
            ProductBudgetRepository _repo = new ProductBudgetRepository();
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

                result = await _repo.Insert(model, conn, transaction);

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


        public async Task<ResultVM> Update(ProductBudgetMasterVM productbudget,SqlTransaction Vtransaction = null,SqlConnection VcurrConn = null)
        {
            ProductBudgetRepository _repo = new ProductBudgetRepository();

            ResultVM result = new ResultVM
            {
                Status = MessageModel.Fail,
                Message = "Error",
                ExMessage = null,
                Id = productbudget.Id.ToString(),
                DataVM = productbudget
            };

            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                #region Open Connection & Transaction

                conn = VcurrConn ?? new SqlConnection(DatabaseHelper.GetConnectionString());

                if (conn.State != ConnectionState.Open)
                {
                    await conn.OpenAsync();
                }

                transaction = Vtransaction ?? conn.BeginTransaction();

                #endregion

                #region Delete Existing Data

                ResultVM deleteResult = await _repo.Delete(productbudget, conn, transaction);

                if (deleteResult.Status.ToLower() != MessageModel.Success.ToLower()
                    && deleteResult.Message != "No data found to delete.")
                {
                    throw new Exception(deleteResult.Message);
                }

                #endregion

                #region Insert Updated Data

                result = await _repo.Insert(productbudget, conn, transaction);

                if (result.Status.ToLower() != MessageModel.Success.ToLower())
                {
                    throw new Exception(result.Message);
                }

                #endregion

                #region Commit Transaction

                if (Vtransaction == null)
                {
                    transaction.Commit();
                }

                #endregion

                result.Status = MessageModel.Success;
                result.Message = MessageModel.UpdateSuccess;

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && Vtransaction == null)
                {
                    transaction.Rollback();
                }

                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();

                return result;
            }
            finally
            {
                if (VcurrConn == null && conn != null)
                {
                    if (conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }

                    conn.Dispose();
                }
            }
        }

        public async Task<ResultVM> GetProductBudgetDataForDetailsNew(ProductBudgetVM model)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

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

                string[] conditionalFields = new[] { "ch.ChargeGroup" };
                string[] conditionalValues = new[] { model.ChargeGroup.ToString() };

                result = await new ProductBudgetRepository().ProductBudgetListForNew(conditionalFields, conditionalValues, model, conn, transaction);

                if (isNewConnection && result.Status == MessageModel.Success)
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                result.Status = MessageModel.Success;
                return result;
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }

        public async Task<ResultVM> ProductBudgetList(ProductBudgetMasterVM model)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

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

                string[] conditionalFields = new[] { "PB.GLFiscalYearId", "PB.BudgetType", "p.ProductGroupId", "PB.BranchId" };
                string[] conditionalValues = new[] { model.GLFiscalYearId.ToString(), model.BudgetType, model.ProductGroupId.ToString(), model.BranchId.ToString() };

                result = await new ProductBudgetRepository().ProductBudgetList(conditionalFields, conditionalValues, model, conn, transaction);


                if (isNewConnection && result.Status == MessageModel.Success)
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                result.Status = MessageModel.Success;
                return result;
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }

        public async Task<ResultVM> ProductBudgeDistincttList(ProductBudgetMasterVM model)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

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

                string[] conditionalFields = new[] { "PB.GLFiscalYearId", "PB.BudgetType", "p.ProductGroupId", "PB.BranchId" };
                string[] conditionalValues = new[] { model.GLFiscalYearId.ToString(), model.BudgetType, model.ProductGroupId.ToString(), model.BranchId.ToString() };

                result = await new ProductBudgetRepository().ProductBudgeDistincttList(conditionalFields, conditionalValues, model, conn, transaction);

                if (isNewConnection && result.Status == MessageModel.Success)
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                result.Status = MessageModel.Success;
                return result;
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }

        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ProductBudgetRepository _repo = new ProductBudgetRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                string[] conditionalFields = new[] { "PB.BudgetType" };
                string[] conditionalValues = new[] { options.vm.BudgetType };

                result = await _repo.GetGridData(options, conditionalFields, conditionalValues, conn, transaction);

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

        public async Task<ResultVM> ReportPreview(CommonVM vm)
        {
            ProductBudgetRepository _repo = new ProductBudgetRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.ReportPreview(vm, conn, transaction);

                if (isNewConnection && result.Status == MessageModel.Success)
                    transaction.Commit();
                else
                    throw new Exception(result.Message);

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection) transaction.Rollback();
                result.Status = MessageModel.Fail;
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
