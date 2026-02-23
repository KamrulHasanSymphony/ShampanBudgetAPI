using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.Repository.SetUp;

namespace ShampanBFRS.Service.SetUp
{
    public class DepartmentService
    {
        CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(DepartmentVM department)
        {
            string CodeGroup = "Department";
            string CodeName = "Department";

            DepartmentRepository _repo = new DepartmentRepository();
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
                string code = _commonRepo.CodeGenerationNo(CodeGroup, CodeName, conn, transaction);


                if (!string.IsNullOrEmpty(code))
                {
                    department.Code = code;

                    result = await _repo.Insert(department, conn, transaction);

                    //

                    if (result.Status.ToLower() == "success")
                    {
                        foreach (var detail in department.SabreList)
                        {
                            detail.DepartmentId = department.Id;

                            var resultDetail = await _repo.InsertDetails(detail, conn, transaction);

                            if (resultDetail.Status.ToLower() != "success")
                            {
                                throw new Exception(resultDetail.Message);
                            }
                        }
                    }
                    else
                    {
                        throw new Exception(result.Message);
                    }


                    //

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
                else
                {
                    throw new Exception("Code Generation Failed!");
                }
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

        //public async Task<ResultVM> Update(DepartmentVM department)
        //{
        //    DepartmentRepository _repo = new DepartmentRepository();
        //    _commonRepo = new CommonRepository();
        //    ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

        //    bool isNewConnection = false;
        //    SqlConnection conn = null;
        //    SqlTransaction transaction = null;

        //    try
        //    {
        //        conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
        //        conn.Open();
        //        isNewConnection = true;
        //        transaction = conn.BeginTransaction();

        //        result = await _repo.Update(department, conn, transaction);

        //        if (isNewConnection && result.Status == "Success")
        //            transaction.Commit();
        //        else
        //            throw new Exception(result.Message);

        //        return result;
        //    }
        //    catch (Exception ex)
        //    {
        //        if (transaction != null && isNewConnection) transaction.Rollback();
        //        result.Message = ex.Message;
        //        result.ExMessage = ex.ToString();
        //        return result;
        //    }
        //    finally
        //    {
        //        if (isNewConnection && conn != null) conn.Close();
        //    }
        //}

        public async Task<ResultVM> Update(DepartmentVM department)
        {
            DepartmentRepository _repo = new DepartmentRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                // Ensure that the result has valid data
                ResultVM rvm = await List(new[] { "M.Id" }, new[] { department.Id.ToString() }, null);

                if (rvm.DataVM == null || !(rvm.DataVM is List<DepartmentVM>))
                {
                    throw new Exception("No data found for the given ID.");
                }

                List<DepartmentVM> dpList = (List<DepartmentVM>)rvm.DataVM;

                DepartmentVM mrvm = dpList.FirstOrDefault();  // Get the first item if present

                _commonRepo.DetailsDelete("DepartmentSabres", new[] { "DepartmentId" }, new[] { department.Id.ToString() }, conn, transaction);

                result = await _repo.Update(department, conn, transaction);

                if (result.Status.ToLower() == "success")
                {
                    foreach (var detail in department.SabreList)
                    {
                        detail.DepartmentId = department.Id;

                        var resultDetail = await _repo.InsertDetails(detail, conn, transaction);

                        if (resultDetail.Status.ToLower() != "success")
                        {
                            throw new Exception(resultDetail.Message);
                        }
                    }
                }
                else
                {
                    throw new Exception(result.Message);
                }

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }

                return result;
            }
            catch (Exception ex)
            {
                transaction?.Rollback();
                result.Message = ex.Message;
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
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            DepartmentRepository _repo = new DepartmentRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", IDs = vm.IDs };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.Delete(vm, conn, transaction);

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

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            DepartmentRepository _repo = new DepartmentRepository();
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

                result = await _repo.List(conditionalFields, conditionalValues, vm, conn, transaction);

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

        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            DepartmentRepository _repo = new DepartmentRepository();
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

                result = await _repo.ListAsDataTable(conditionalFields, conditionalValues, vm, conn, transaction);

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

        public async Task<ResultVM> Dropdown()
        {
            DepartmentRepository _repo = new DepartmentRepository();
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

                result = await _repo.Dropdown(conn, transaction);

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

        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            DepartmentRepository _repo = new DepartmentRepository();
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

                result = await _repo.GetGridData(options, conn, transaction);

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

        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            DepartmentRepository _repo = new DepartmentRepository();
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

                //result = await _repo.ReportPreview(conditionalFields, conditionalValues, vm, conn, transaction);

                if (result.Status == "Success" && result.DataVM is DataTable dataTable)
                {
                    if (!dataTable.Columns.Contains("ReportType"))
                    {
                        var ReportType = new DataColumn("ReportType") { DefaultValue = "Examinee" };
                        dataTable.Columns.Add(ReportType);
                    }
                    result.DataVM = dataTable;
                }

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
