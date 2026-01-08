using Newtonsoft.Json;
using ShampanBFRS.Repository.Ceiling;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.SalaryAllowance;
using ShampanBFRS.Repository.Sale;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SalaryAllowance;
using ShampanBFRS.ViewModel.Sale;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Service.Sale
{
    public class SaleService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(SaleHeaderVM saleHeader, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            string CodeGroup = "Sale";
            string CodeName = "Sale";

            SaleRepository _repo = new SaleRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                #region open connection and transaction
                conn = VcurrConn ?? new SqlConnection(DatabaseHelper.GetConnectionString());
                transaction = Vtransaction;


                if (conn.State != ConnectionState.Open) conn.Open();

                if (transaction == null) transaction = conn.BeginTransaction();
                #endregion open connection and transaction
                // check

                string code = _commonRepo.CodeGenerationNo(CodeGroup, CodeName, conn, transaction);

                string[] conditionField = { "Code" };
                string[] conditionValue = { code ?? string.Empty };

                bool exists = _commonRepo.CheckExists("SaleHeaders", conditionField, conditionValue, conn, transaction);
                if (exists)
                    throw new System.Exception("SaleHeaders Already Exist!");

                if (!string.IsNullOrEmpty(code))
                {
                    saleHeader.Code = code;
                    result = await _repo.Insert(saleHeader, conn, transaction);





                    saleHeader.Id = Convert.ToInt32(result.Id);

                    if (result.Status.ToLower() == "success")
                    {
                        foreach (var details in saleHeader.SaleDetail)
                        {
                            details.SaleHeaderId = saleHeader.Id;

                            details.PriceMT = details.PriceLTR * details.ConversionFactor;
                            details.SalesExERLValue = details.PriceMT * details.ProductionMT;
                            details.SalesExImport_LocalValue = details.SalesExImport_LocalMT * details.PriceMT;

                            details.TotalMT = details.ProductionMT + details.SalesExImport_LocalMT;
                            details.TotalValueTK_LAC = (details.SalesExERLValue + details.SalesExImport_LocalValue) / 100000;

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

        public async Task<ResultVM> Update(SaleHeaderVM saleHeader, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = saleHeader.Id.ToString(), DataVM = saleHeader };

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

                ResultVM rvm = await List(new[] { "M.Id" }, new[] { saleHeader.Id.ToString() }, null);

                if (rvm.DataVM == null || !(rvm.DataVM is List<SaleHeaderVM>)) 
                {
                    throw new Exception("No data found for the given ID.");
                }

                List<SaleHeaderVM> mrvmList = (List<SaleHeaderVM>)rvm.DataVM;
                SaleHeaderVM mrvm = mrvmList.FirstOrDefault();

                // Check if the data is already posted
                if (mrvm == null || mrvm.IsPost == "1")
                {
                    throw new Exception("Data already posted.Updates not allowed.");
                }


                var record = _commonRepo.DetailsDelete("SaleDetails", new[] { "SaleHeaderId" }, new[] { saleHeader.Id.ToString() }, conn, transaction);

                if (record.Status == "Fail")
                {
                    throw new Exception("Error in Delete for Details Data.");
                }

                result = await _repo.Update(saleHeader, conn, transaction);
                if (result.Status.ToLower() == "success")
                {
                    foreach (var details in saleHeader.SaleDetail)
                    {
                        details.SaleHeaderId = saleHeader.Id;
                        details.PriceMT = details.PriceLTR * details.ConversionFactor;
                        details.SalesExERLValue = details.PriceMT * details.ProductionMT;
                        details.SalesExImport_LocalValue = details.SalesExImport_LocalMT * details.PriceMT;

                        details.TotalMT = details.ProductionMT + details.SalesExImport_LocalMT;
                        details.TotalValueTK_LAC = (details.SalesExERLValue + details.SalesExImport_LocalValue) / 100000;

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

        public async Task<ResultVM> MultipleDelete(CommonVM vm, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = vm.IDs, DataVM = null };

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

                result = await _repo.MultipleDelete(vm, conn, transaction);

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

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
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

                var lst = new List<SaleHeaderVM>();

                string data = JsonConvert.SerializeObject(result.DataVM);

                lst = JsonConvert.DeserializeObject<List<SaleHeaderVM>>(data);

                var detailsDataList = await _repo.DetailsList(new[] { "D.SaleHeaderId" }, conditionalValues, vm, conn, transaction);

                if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable dt)
                {
                    string json = JsonConvert.SerializeObject(dt);
                    var details = JsonConvert.DeserializeObject<List<SaleDetailVM>>(json);

                    lst.FirstOrDefault().SaleDetail = details;
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

        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
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

                result = await _repo.ListAsDataTable(conditionalFields, conditionalValues, conn, transaction, vm);

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

        public async Task<ResultVM> Dropdown(SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
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

                result = await _repo.Dropdown(conn, transaction);

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

                return result;
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

        public async Task<ResultVM> GetGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues)
        {
            SaleRepository _repo = new SaleRepository();
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

        // MultiplePost Method
        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = vm.IDs, DataVM = null };

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

                result = await _repo.MultiplePost(vm, conn, transaction);

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

        public async Task<ResultVM> GetDetailDataById(GridOptions options, int masterId, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
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

        public async Task<ResultVM> GetDetailsGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            SaleRepository _repo = new SaleRepository();
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

                result = await _repo.GetDetailsGridData(options, conditionalFields, conditionalValues, conn, transaction);

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
        //report
        public async Task<ResultVM> ReportPreview(CommonVM vm)
        {
            SaleRepository _repo = new SaleRepository();
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

