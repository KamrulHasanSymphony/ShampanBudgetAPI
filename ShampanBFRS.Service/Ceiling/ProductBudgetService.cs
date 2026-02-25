using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data.SqlClient;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.Repository.Ceiling;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SalaryAllowance;

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

        public async Task<ResultVM> Update(ProductBudgetVM master)
        {
            CeilingRepository _repo = new CeilingRepository();
            _commonRepo = new CommonRepository();
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

                result = await new FiscalYearDetailRepository().List("", new[] { "FiscalYearId" }, new[] { master.GLFiscalYearId.ToString() }, null, conn, transaction);

                var FiscalYearDetailVM = (List<FiscalYearDetailVM>)result.DataVM;

                //result = await _repo.Update(master, conn, transaction);

                if (result.Status == MessageModel.Fail)
                {
                    throw new Exception(MessageModel.UpdateFail);
                }

                if (master.Id > 0)
                {
                    //result = await _repo.DeleteDetails(master, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                    {
                        throw new Exception(MessageModel.DeleteFail);

                    }

                    //SplitCeilingByFiscalPeriods(master, FiscalYearDetailVM);

                    //foreach (var detail in master.CeilingDetailList)
                    //{
                    //    detail.GLCeilingId = master.Id;

                    //    result = await _repo.InsertDetails(detail, conn, transaction);

                    //    if (result.Status == MessageModel.Fail)
                    //        throw new Exception(result.Message);
                    //}

                }
                //



                if (isNewConnection && result.Status == "Success")
                    transaction.Commit();
                else
                    throw new Exception(result.Message);

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
