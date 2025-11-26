using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.Question;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.ExtensionMethods;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRSAPI.Configuration;
using System.Data;
using static ShampanBFRSAPI.Configuration.HttpRequestHelper;

namespace ShampanBFRSAPI.Controllers.Question
{
    [Route("api/[controller]")]
    [ApiController]
    public class ExamineeGroupController : ControllerBase
    {
        ExamineeGroupService _examineeGroupService = new ExamineeGroupService();
        CommonService _common = new CommonService();

        // POST: api/ExamineeGroup/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(ExamineeGroupVM examineeGroup)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.Insert(examineeGroup);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = examineeGroup };
            }
        }

        // POST: api/ExamineeGroup/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(ExamineeGroupVM examineeGroup)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.Update(examineeGroup);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = examineeGroup };
            }
        }

        // POST: api/ExamineeGroup/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.Delete(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // POST: api/ExamineeGroup/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // GET: api/ExamineeGroup/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(ExamineeGroupVM examineeGroup)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = examineeGroup };
            }
        }

        // POST: api/ExamineeGroup/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }


        // POST: api/ExamineeGroup/Dropdown
        [HttpPost("Dropdown")]
        public async Task<ResultVM> Dropdown(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _examineeGroupService = new ExamineeGroupService();
                resultVM = await _examineeGroupService.Dropdown(new[] { "" }, new[] { "" }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }


        // POST: api/ExamineeGroup/ReportPreview
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

                _examineeGroupService = new ExamineeGroupService();
                PeramModel peramModel = new PeramModel { CompanyId = vm.CompanyId };

                var resultVM = await _examineeGroupService.ReportPreview(
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

                    var stream = httpRequestHelper.PostDataReport(baseUrl + "/api/ExamineeGroup/GetExamineeGroup", authModel, json);

                    if (stream == null) throw new Exception("Failed to generate report.");

                    return new FileStreamResult(stream, "application/pdf")
                    {
                        FileDownloadName = "ExamineeGroup.pdf"
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
