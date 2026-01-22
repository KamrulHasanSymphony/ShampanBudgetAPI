using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data.SqlClient;
using ShampanBFRS.Repository.Ceiling;
using ShampanBFRS.ViewModel.KendoCommon;
using System.Data;
using Newtonsoft.Json;

namespace ShampanBFRS.Service.Ceiling
{
    public class BudgetService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(BudgetHeaderVM model)
        {
            BudgetRepository _repo = new BudgetRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            ResultVM Proresult = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            string CodeGroup = "Budget";
            string CodeName = "Budget";

            try
            {
                #region Connection open

                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                #region Check Exist Data

                string tableName = "BudgetHeaders";
                string[] conditionField = { "FiscalYearId", "CreatedBy" };
                string[] conditionValue = { model.FiscalYearId.ToString(), model.CreatedBy.ToString() };

                bool exist = _commonRepo.CheckExists(tableName, conditionField, conditionValue, conn, transaction);

                if (exist)
                {
                    return new ResultVM
                    {
                        Status = MessageModel.Fail,
                        Message = "You have already added FiscalYear For Budget"
                    };
                }

                #endregion
                string code = _commonRepo.CodeGenerationNo(CodeGroup, CodeName, conn, transaction);
                model.Code = code;

                #endregion


                if (model == null)
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.NotFoundForSave,
                    };
                }

                result = await _repo.Insert(model, conn, transaction);

                if (result.Status == MessageModel.Fail)
                    throw new Exception(result.Message);

                foreach (var detail in model.DetailList)
                {
                    detail.BudgetHeaderId = model.Id;

                    if (detail.InputTotal.HasValue)
                    {
                        decimal input = detail.InputTotal.Value;

                        // Monthly
                        decimal monthly = Math.Round(input / 12, 2);
                        detail.M1 = detail.M2 = detail.M3 = detail.M4 =
                        detail.M5 = detail.M6 = detail.M7 = detail.M8 =
                        detail.M9 = detail.M10 = detail.M11 = detail.M12 = monthly;

                        // Quarterly
                        decimal quarterly = Math.Round(input / 4, 2);
                        detail.Q1 = detail.Q2 = detail.Q3 = detail.Q4 = quarterly;

                        // Half-Yearly
                        decimal halfYearly = Math.Round(input / 2, 2);
                        detail.H1 = detail.H2 = halfYearly;

                        // Yearly
                        detail.Yearly = input;
                    }

                    result = await _repo.InsertDetails(detail, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                        throw new Exception(result.Message);
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
        public async Task<ResultVM> Update(BudgetHeaderVM bugetHeader, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            BudgetRepository _repo = new BudgetRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = bugetHeader.Id.ToString(), DataVM = bugetHeader };

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




                var record = _commonRepo.DetailsDelete("BudgetDetails", new[] { "BudgetHeaderId" }, new[] { bugetHeader.Id.ToString() }, conn, transaction);

                if (record.Status == "Fail")
                {
                    throw new Exception("Error in Delete for Details Data.");
                }

                result = await _repo.Update(bugetHeader, conn, transaction);
                if (result.Status.ToLower() == "success")
                {
                    foreach (var details in bugetHeader.DetailList)
                    {
                        var headerId = bugetHeader.Id;
                        details.BudgetHeaderId = headerId;
                        var detailresult = await _repo.InsertDetails(details, conn, transaction);

                        if (detailresult.Status.ToLower() != "success")
                        {
                            throw new Exception(detailresult.Message);
                        }

                    }

                }
                else
                {
                    throw new Exception(result.Message);
                }
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
                return result;
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
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            BudgetRepository _repo = new BudgetRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

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

                result = await _repo.List(conditionalFields, conditionalValues, conn, transaction, vm);

                var lst = new List<BudgetHeaderVM>();

                string data = JsonConvert.SerializeObject(result.DataVM);

                lst = JsonConvert.DeserializeObject<List<BudgetHeaderVM>>(data);

                var detailsDataList = await _repo.DetailsList(new[] { "D.BudgetHeaderId" }, conditionalValues, vm, conn, transaction);

                if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable dt)
                {
                    string json = JsonConvert.SerializeObject(dt);
                    var details = JsonConvert.DeserializeObject<List<BudgetDetailVM>>(json);

                    lst.FirstOrDefault().DetailList = details;
                    result.DataVM = lst;
                }
                #region Commit
                if (Vtransaction == null && transaction != null)
                {
                    if (result.Status == "Success")
                    {
                        transaction.Commit();
                        return result;
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
                return result;
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

        public async Task<ResultVM> GetBudgetDataForDetailsNew(GridOptions options)
        {
            BudgetRepository _repo = new BudgetRepository();
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

                string[] conditionalFields = new[] { "UI.UserName" };
                string[] conditionalValues = new[] { options.vm.UserId };

                result = await _repo.GetBudgetDataForDetailsNew(options, conditionalFields, conditionalValues, conn, transaction);

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
        public async Task<ResultVM> GetGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues)
        {
            BudgetRepository _repo = new BudgetRepository();
            ResultVM result = new() { Status = "Fail", Message = "Error", Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.GetGridData(options, conditionalFields, conditionalValues, conn, transaction);

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

            return result;
        }
        public async Task<ResultVM> GetDetailDataById(GridOptions options, int masterId, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            BudgetRepository _repo = new BudgetRepository();
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

                result = await _repo.GetDetailDataById(options, masterId, conn, transaction);

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

        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            BudgetRepository _repo = new BudgetRepository();
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

        public async Task<ResultVM> BudgetFinalReport(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            BudgetRepository _repo = new BudgetRepository();
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

        public async Task<ResultVM> BudgetTransfer(BudgetHeaderVM model)
        {
            BudgetRepository _repo = new BudgetRepository();
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

                string CodeGroup = "Budget";
                string CodeName = "Budget";

                if (model == null)
                {
                    return new ResultVM()
                    {
                        Status = MessageModel.Fail,
                        Message = MessageModel.NotFoundForSave,
                    };
                }

                string code = _commonRepo.CodeGenerationNo(CodeGroup, CodeName, conn, transaction);

                if (code != "" || code != null)
                {
                    model.Code = code;

                    result = await _repo.BudgetTransferHeader(model, conn, transaction);

                    if (result.Status == MessageModel.Fail)
                        throw new Exception(result.Message);

                    model.Id = Convert.ToInt32(result.Id);

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

        public async Task<ResultVM> BudgetLoadFinalReport(CommonVM vm, string[] conditionalFields = null, string[] conditionalValues = null)
        {
            BudgetRepository _repo = new BudgetRepository();
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

                result = await _repo.BudgetLoadFinalReport(vm, conditionalFields, conditionalValues, conn, transaction);

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

        #region Budget All

        public async Task<ResultVM> GetGridDataBudgetAll(GridOptions options, string[] conditionalFields, string[] conditionalValues)
        {
            BudgetRepository _repo = new BudgetRepository();
            ResultVM result = new() { Status = "Fail", Message = "Error", Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.GetGridDataBudgetAll(options, conditionalFields, conditionalValues, conn, transaction);

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

            //return result;
        }

        public async Task<ResultVM> BudgetListAll(CommonVM vm = null, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            BudgetRepository _repo = new BudgetRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

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

                string[] conditionalFields = new string[] { "M.FiscalYearId", "M.BudgetType" };
                string[] conditionalValues = new string[] {vm.FiscalYearId,vm.BudgetType };

                result = await _repo.BudgetListAll(conditionalFields, conditionalValues, conn, transaction, vm);

                var lst = new List<BudgetHeaderVM>();

                string data = JsonConvert.SerializeObject(result.DataVM);

                lst = JsonConvert.DeserializeObject<List<BudgetHeaderVM>>(data);

                var detailsDataList = await _repo.BudgetAllDetailsList(vm, conn, transaction);

                if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable dt)
                {
                    string json = JsonConvert.SerializeObject(dt);
                    var details = JsonConvert.DeserializeObject<List<BudgetDetailVM>>(json);

                    lst.FirstOrDefault().DetailList = details;
                    result.DataVM = lst;
                }
                #region Commit
                if (Vtransaction == null && transaction != null)
                {
                    if (result.Status == "Success")
                    {
                        transaction.Commit();
                        return result;
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
                return result;
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


        #endregion


    }
}
