using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.Question;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.Repository.SetUp;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Newtonsoft.Json;

namespace ShampanBFRS.Service.SetUp
{
    public class ChargeHeaderService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(ChargeHeaderVM chargeHeader, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {

            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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

                string[] conditionField = { "ChargeGroup"};
                string[] conditionValue = { chargeHeader.ChargeGroup ?? string.Empty };

                bool exists = _commonRepo.CheckExists("ChargeHeaders", conditionField, conditionValue, conn, transaction);
                if (exists)
                    throw new System.Exception("Group Name Already Exist!");



                result = await _repo.Insert(chargeHeader, conn, transaction);
                    chargeHeader.Id = Convert.ToInt32(result.Id);

                    if (result.Status.ToLower() == "success")
                    {
                        foreach (var details in chargeHeader.ChargeDetails)
                        {
                            details.ChargeHeaderId = chargeHeader.Id;



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

        public async Task<ResultVM> Update(ChargeHeaderVM chargeHeader, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = chargeHeader.Id.ToString(), DataVM = chargeHeader };

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



                var record = _commonRepo.DetailsDelete("ChargeDetails", new[] { "ChargeHeaderId" }, new[] { chargeHeader.Id.ToString() }, conn, transaction);

                if (record.Status == "Fail")
                {
                    throw new Exception("Error in Delete for Details Data.");
                }

                result = await _repo.Update(chargeHeader, conn, transaction);
                if (result.Status.ToLower() == "success")
                {
                    foreach (var details in chargeHeader.ChargeDetails)
                    {
                        details.ChargeHeaderId = chargeHeader.Id;


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
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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

                var lst = new List<ChargeHeaderVM>();

                string data = JsonConvert.SerializeObject(result.DataVM);
                lst = JsonConvert.DeserializeObject<List<ChargeHeaderVM>>(data);

                var detailsDataList = await _repo.DetailsList(new[] { "D.ChargeHeaderId" }, conditionalValues, vm, conn, transaction);

                if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable dt)
                {
                    string json = JsonConvert.SerializeObject(dt);
                    var details = JsonConvert.DeserializeObject<List<ChargeDetailVM>>(json);

                    lst.FirstOrDefault().ChargeDetails = details;
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
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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

        // ReportPreview Method
        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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

                result = await _repo.ReportPreview(conditionalFields, conditionalValues, conn, transaction, vm);

                var companyData = await new CompanyProfileRepository().List(new[] { "H.Id" }, new[] { vm.CompanyId }, null, conn, transaction);
                string companyName = string.Empty;
                if (companyData.Status == "Success" && companyData.DataVM is List<CompanyProfileVM> company)
                {
                    companyName = company.FirstOrDefault()?.CompanyName;
                }

                if (result.Status == "Success" && !string.IsNullOrEmpty(companyName) && result.DataVM is DataTable dataTable)
                {
                    if (!dataTable.Columns.Contains("CompanyName"))
                    {
                        var CompanyName = new DataColumn("CompanyName") { DefaultValue = companyName };
                        dataTable.Columns.Add(CompanyName);
                    }

                    if (!dataTable.Columns.Contains("ReportType"))
                    {
                        var ReportType = new DataColumn("ReportType") { DefaultValue = "PurchaseHeader" };
                        dataTable.Columns.Add(ReportType);
                    }

                    result.DataVM = dataTable;
                    transaction.Commit();
                }

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

        // MultiplePost Method
        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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

        public async Task<ResultVM> GetChargeDetailDataById(GridOptions options, int masterId, SqlTransaction Vtransaction = null, SqlConnection VcurrConn = null)
        {
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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

                result = await _repo.GetChargeDetailDataById(options, masterId, conn, transaction);

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
            ChargeHeaderRepository _repo = new ChargeHeaderRepository();
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
    }
}
