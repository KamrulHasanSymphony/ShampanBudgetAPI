using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.Question;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.ExtensionMethods;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRSAPI.Configuration;
using static ShampanBFRSAPI.Configuration.HttpRequestHelper;
using System.Data;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class DepartmentSabreController : ControllerBase
    {
        DepartmentSabreService _departmentsabreservice = new DepartmentSabreService();
        CommonService _common = new CommonService();

        // POST: api/DepartmentSabre/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(DepartmentSabreVM departmentsabre)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.Insert(departmentsabre);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = departmentsabre };
            }
        }

        // POST: api/DepartmentSabre/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(DepartmentSabreVM departmentsabre)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.Update(departmentsabre);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = departmentsabre };
            }
        }

        // POST: api/DepartmentSabre/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.Delete(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // POST: api/DepartmentSabre/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // GET: api/DepartmentSabre/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(DepartmentSabreVM departmentsabre)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = departmentsabre };
            }
        }

        // GET: api/DepartmentSabre/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.Dropdown();
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/DepartmentSabre/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _departmentsabreservice = new DepartmentSabreService();
                resultVM = await _departmentsabreservice.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/DepartmentSabre/ReportPreview
        [HttpPost("ReportPreview")]
        public async Task<FileStreamResult> ReportPreview(CommonVM vm)
        {
            _common = new CommonService();
            ResultVM settingResult = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                string baseUrl = "";

                settingResult = await _common.SettingsValue(new[] { "SettingGroup", "SettingName" },
                                                           new[] { "DMSReportUrl", "DMSReportUrl" }, null);

                if (settingResult.Status == "Success" && settingResult.DataVM is DataTable settingValue)
                {
                    if (settingValue.Rows.Count > 0)
                    {
                        baseUrl = settingValue.Rows[0]["SettingValue"].ToString();
                    }
                }

                if (string.IsNullOrEmpty(baseUrl))
                    throw new Exception("Report API Url Not Found!");

                _departmentsabreservice = new DepartmentSabreService();

                PeramModel peramModel = new PeramModel { CompanyId = vm.CompanyId };

                var resultVM = await _departmentsabreservice.ReportPreview(
                    new[] { "H.Id", "H.BranchId" }, new[] { vm.Id, vm.BranchId }, peramModel);

                if (resultVM.Status == "Success" && resultVM.DataVM is DataTable dt && dt.Rows.Count > 0)
                {
                    string json = ExtensionMethods.DataTableToJson(dt);
                    HttpRequestHelper httpRequestHelper = new HttpRequestHelper();

                    var authModel = httpRequestHelper.GetAuthentication(new CredentialModel
                    {
                        ApiKey = DatabaseHelper.GetKey(),
                        PathName = baseUrl
                    });

                    var stream = httpRequestHelper.PostDataReport(baseUrl + "/api/Examinee/GetExaminee", authModel, json);

                    if (stream == null) throw new Exception("Failed to generate report.");

                    return new FileStreamResult(stream, "application/pdf")
                    {
                        FileDownloadName = "ExamineeReport.pdf"
                    };
                }

                throw new Exception("No data found.");
            }
            catch (Exception ex)
            {
                throw new Exception($"Error generating report: {ex.Message}");
            }
        }
    }
}

