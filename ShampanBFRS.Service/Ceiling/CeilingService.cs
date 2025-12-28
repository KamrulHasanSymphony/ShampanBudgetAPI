using Newtonsoft.Json.Linq;
using ShampanBFRS.Repository.Ceiling;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.Question;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static ShampanBFRS.ViewModel.KendoCommon.UtilityCommon;

namespace ShampanBFRS.Service.Ceiling
{
    public class CeilingService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(CeilingVM model)
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

                string CodeGroup = "Ceiling";
                string CodeName = "Ceiling";

                if (model == null)
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.NotFoundForSave,
                    };
                }
                else if (!model.CeilingDetailList.Any())
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.DetailsNotFoundForSave,
                    };
                }

                result = await new FiscalYearDetailRepository().List("", new[] { "FiscalYearId" }, new[] { model.GLFiscalYearId.ToString() }, null, conn, transaction);

                var FiscalYearDetailVM = (List<FiscalYearDetailVM>)result.DataVM;

                string code = _commonRepo.CodeGenerationNo(CodeGroup, CodeName, conn, transaction);

                if (code != "" || code != null)
                {
                    model.Code = code;

                    result = await _repo.Insert(model, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                        throw new Exception(result.Message);

                    SplitCeilingByFiscalPeriods(model, FiscalYearDetailVM);

                    if (model.CeilingDetailList.Count > 0)
                    {
                        if (result.Status == MessageModel.Fail)
                            throw new Exception(MessageModel.DetailInsertFailed);
                    }

                    foreach (var detail in model.CeilingDetailList)
                    {
                        detail.GLCeilingId = model.Id;

                        result = await _repo.InsertDetails(detail, conn, transaction);

                        if (result.Status == MessageModel.Fail)
                            throw new Exception(result.Message);
                    }

                }
                else
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.DataLoadedFailed,

                    };
                }

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

        public async Task<ResultVM> Update(CeilingVM master)
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

                result = await _repo.Update(master, conn, transaction);

                if (result.Status == MessageModel.Fail)
                {
                    throw new Exception(MessageModel.UpdateFail);
                }

                if (master.Id > 0)
                {
                    result = await _repo.DeleteDetails(master, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                    {
                        throw new Exception(MessageModel.DeleteFail);

                    }

                    SplitCeilingByFiscalPeriods(master, FiscalYearDetailVM);

                    foreach (var detail in master.CeilingDetailList)
                    {
                        detail.GLCeilingId = master.Id;

                        result = await _repo.InsertDetails(detail, conn, transaction);

                        if (result.Status == MessageModel.Fail)
                            throw new Exception(result.Message);
                    }

                }

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

        private void SplitCeilingByFiscalPeriods(CeilingVM model, List<FiscalYearDetailVM> fiscalYearDetais)
        {
            try
            {
                CeilingDetailVM dVM;
                List<CeilingDetailVM> lst = new List<CeilingDetailVM>();

                foreach (CeilingDetailVM detailVM in model.CeilingDetailList)
                {
                    #region Data Assign
                    foreach (FiscalYearDetailVM item in fiscalYearDetais)
                    {
                        dVM = new CeilingDetailVM();
                        dVM.GLFiscalYearDetailId = item.Id;
                        dVM.AccountId = detailVM.AccountId;
                        dVM.InputTotal = detailVM.InputTotal;
                        dVM.PeriodSl = item.PeriodSl;
                        dVM.PeriodStart = item.MonthStart;
                        dVM.PeriodEnd = item.MonthEnd;
                        #region Switching
                        switch (item.PeriodSl.ToLower())
                        {
                            case "a":
                                dVM.Amount = detailVM.January;
                                break;
                            case "b":
                                dVM.Amount = detailVM.February;
                                break;
                            case "c":
                                dVM.Amount = detailVM.March;
                                break;
                            case "d":
                                dVM.Amount = detailVM.April;
                                break;
                            case "e":
                                dVM.Amount = detailVM.May;
                                break;
                            case "f":
                                dVM.Amount = detailVM.June;
                                break;
                            case "g":
                                dVM.Amount = detailVM.July;
                                break;
                            case "h":
                                dVM.Amount = detailVM.August;
                                break;
                            case "i":
                                dVM.Amount = detailVM.September;
                                break;
                            case "j":
                                dVM.Amount = detailVM.October;
                                break;
                            case "k":
                                dVM.Amount = detailVM.November;
                                break;
                            case "l":
                                dVM.Amount = detailVM.December;
                                break;
                        }
                        #endregion
                        lst.Add(dVM);
                    }
                    #endregion
                }

                model.CeilingDetailList = new List<CeilingDetailVM>();
                model.CeilingDetailList = lst;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            CeilingRepository _repo = new CeilingRepository();
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

                string[] conditionalFields = new[] { "c.CreatedBy", "c.TransactionType", "c.BudgetType" };
                string[] conditionalValues = new[] { options.vm.UserId, options.vm.TransactionType, options.vm.BudgetType };
                //string[] conditionalFields = new[] { "c.TransactionType", "c.BudgetType" };
                //string[] conditionalValues = new[] {  options.vm.TransactionType, options.vm.BudgetType };

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

        public async Task<ResultVM> GetAllSabreDataForDetails(GridOptions options)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                var data = new GridEntity<CeilingDetailVM>();
                result.DataVM = KendoGrid<CeilingDetailVM>.GetGridData_5(options, "GetAllSabreDataForDetails"
                    , "get_summary", "c.Id", options.vm.BranchId, options.vm.YearId, options.vm.BudgetSetNo
                    , options.vm.BudgetType, options.vm.UserId);

                result.Status = MessageModel.Success;
                return result;
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task<ResultVM> CeilingList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            CeilingRepository _repo = new CeilingRepository();
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

                result = await _repo.CeilingList(conditionalFields, conditionalValues, vm, conn, transaction);

                if (isNewConnection && result.Status == "Success")
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

        public async Task<ResultVM> BudgetFinalReport(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            CeilingRepository _repo = new CeilingRepository();
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

                result = await _repo.BudgetFinalReport(vm, conditionalFields, conditionalValues, conn, transaction);

                if (isNewConnection && result.Status == "Success")
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

        public async Task<ResultVM> GridDataReportType(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            CeilingRepository _repo = new CeilingRepository();
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

                result = await _repo.GridDataReportType(vm, conditionalFields, conditionalValues, conn, transaction);

                if (isNewConnection && result.Status == "Success")
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

        public async Task<ResultVM> GetCeilingDetailDataById(GridOptions options, int masterId, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            CeilingRepository _repo = new CeilingRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = null, DataVM = null };

            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                #region open connection and transaction
                if (VcurrConn != null)
                {
                    conn = VcurrConn;
                }
                if (Vtransaction != null)
                {
                    transaction = Vtransaction;
                }
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction("");
                }
                #endregion open connection and transaction

                result = await _repo.GetCeilingDetailDataById(options, masterId, conn, transaction);

                #region Commit
                if (Vtransaction == null && transaction != null)
                {
                    if (result.Status == "Success")
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        throw new Exception(result.Message);
                    }
                }
                #endregion Commit
            }
            #region Catch & Finally
            catch (Exception ex)
            {
                if (transaction != null && Vtransaction == null) { transaction.Rollback(); }

                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
            }
            finally
            {
                if (VcurrConn == null)
                {
                    if (conn != null)
                    {
                        if (conn.State == ConnectionState.Open)
                        {
                            conn.Close();
                        }
                    }
                }
            }
            #endregion Catch & Finally
            return result;
        }

        public async Task<ResultVM> BudgetTransfer(CeilingVM model)
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

                string CodeGroup = "Ceiling";
                string CodeName = "Ceiling";

                if (model == null)
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.NotFoundForSave,
                    };
                }
                //else if (!model.CeilingDetailList.Any())
                //{
                //    return new ResultVM()
                //    {
                //        Status = MessageModel.Fail,
                //        Message = MessageModel.DetailsNotFoundForSave,
                //    };
                //}

                string code = _commonRepo.CodeGenerationNo(CodeGroup, CodeName, conn, transaction);

                if (code != "" || code != null)
                {
                    model.Code = code;

                    result = await _repo.BudgetTransferHeader(model, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                        throw new Exception(result.Message);

                    //detail.GLCeilingId = model.Id;

                    result = await _repo.BudgetTransferDetails(model, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                        throw new Exception(result.Message);

                }
                else
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.DataLoadedFailed,

                    };
                }

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

        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            CeilingRepository _repo = new CeilingRepository();
            ResultVM result = new() { Status = "Fail", Message = "Error", IDs = vm.IDs, DataVM = null };

            SqlConnection conn = VcurrConn ?? new SqlConnection(DatabaseHelper.GetConnectionString());
            SqlTransaction transaction = Vtransaction;

            try
            {
                if (conn.State != ConnectionState.Open)
                    conn.Open();

                if (transaction == null)
                    transaction = conn.BeginTransaction();

                result = await _repo.MultiplePost(vm, conn, transaction);

                if (Vtransaction == null && transaction != null)
                {
                    if (result.Status == "Success")
                        transaction.Commit();
                    else
                        transaction.Rollback();
                }
            }
            catch (System.Exception ex)
            {
                if (transaction != null && Vtransaction == null)
                    transaction.Rollback();

                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }
            finally
            {
                if (VcurrConn == null && conn != null && conn.State == ConnectionState.Open)
                    conn.Close();
            }

            return result;
        }




    }
}
