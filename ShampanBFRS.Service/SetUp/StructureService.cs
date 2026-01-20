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
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace ShampanBFRS.Service.SetUp
{
    public class StructureService
    {
        private readonly CommonRepository _commonRepo = new CommonRepository();

        public async Task<ResultVM> Insert(StructureVM vm)
        {
            string codeGroup = "Structure";
            string codeName = "Structure";
            StructureRepository _repo = new StructureRepository();
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

                string code = _commonRepo.CodeGenerationNo(codeGroup, codeName, conn, transaction);

                if (string.IsNullOrEmpty(code))
                {
                    throw new Exception("Code generation failed!");
                }

                vm.Code = code;

                result = await _repo.Insert(vm, conn, transaction);

                if (result.Status.ToLower() == "success")
                {
                    foreach (var detail in vm.StructureDetails)
                    {
                        detail.StructureId = vm.Id;

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

        public async Task<ResultVM> Update(StructureVM vm)
        {
            StructureRepository _repo = new StructureRepository();
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
                ResultVM rvm = await List(new[] { "M.Id" }, new[] { vm.Id.ToString() }, null);

                if (rvm.DataVM == null || !(rvm.DataVM is List<StructureVM>))
                {
                    throw new Exception("No data found for the given ID.");
                }

                // Cast DataVM to List<MaintenanceRequisitionVM> and take the first item
                List<StructureVM> mrvmList = (List<StructureVM>)rvm.DataVM;
                StructureVM mrvm = mrvmList.FirstOrDefault();  // Get the first item if present

                // Check if the data is already posted
                //if (mrvm == null || mrvm.IsPost)
                //{
                //    throw new Exception("Data already posted.Updates not allowed.");
                //}

                _commonRepo.DetailsDelete("StructureDetails", new[] { "StructureId" }, new[] { vm.Id.ToString() }, conn, transaction);

                result = await _repo.Update(vm, conn, transaction);

                if (result.Status.ToLower() == "success")
                {
                    foreach (var detail in vm.StructureDetails)
                    {
                        detail.StructureId = vm.Id;

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

        public async Task<ResultVM> Delete(string[] Id)
        {
            StructureRepository _repo = new StructureRepository();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null};

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.Delete(Id, conn, transaction);

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
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            StructureRepository _repo = new StructureRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.List(conditionalFields, conditionalValues, vm, conn, transaction);

                if (isNewConnection)
                {
                    transaction.Commit();
                }
                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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

        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            StructureRepository _repo = new StructureRepository();
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

                result = await _repo.ListAsDataTable(conditionalFields, conditionalValues, vm, conn, transaction);

                if (isNewConnection)
                {
                    transaction.Commit();
                }
                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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

        public async Task<ResultVM> Dropdown()
        {
            StructureRepository _repo = new StructureRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.Dropdown(conn, transaction);

                if (isNewConnection)
                {
                    transaction.Commit();
                }
                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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

        public async Task<ResultVM> MultiplePost(CommonVM vm)
        {
            StructureRepository _repo = new StructureRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.MultiplePost(vm, conn, transaction);

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
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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

        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            StructureRepository _repo = new StructureRepository();
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

                result = await _repo.GetGridData(options, conn, transaction);

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
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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

        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            StructureRepository _repo = new StructureRepository();
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

                result = await _repo.ReportPreview(conditionalFields, conditionalValues, vm, conn, transaction);

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
                        var companyNameCol = new DataColumn("CompanyName") { DefaultValue = companyName };
                        dataTable.Columns.Add(companyNameCol);
                    }
                    if (!dataTable.Columns.Contains("ReportType"))
                    {
                        var reportTypeCol = new DataColumn("ReportType") { DefaultValue = "Maintenance Requisition Invoice" };
                        dataTable.Columns.Add(reportTypeCol);
                    }
                    result.DataVM = dataTable;
                }

                if (isNewConnection)
                {
                    transaction.Commit();
                }
                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.ToString();
                result.Message = ex.Message;
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
