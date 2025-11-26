using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.Question;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.ExtensionMethods;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRSAPI.Configuration;
using System.Data;
using static ShampanBFRSAPI.Configuration.HttpRequestHelper;

namespace ShampanBFRSAPI.Controllers.Question
{
    [Route("api/[controller]")]
    [ApiController]
    public class QuestionCategoryController : ControllerBase
    {
        QuestionCategoryService _questionCategoryService = new QuestionCategoryService();
        CommonService _common = new CommonService();

        // POST: api/QuestionCategory/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(QuestionCategoryVM questionCategory)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.Insert(questionCategory);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionCategory };
            }
        }

        // POST: api/QuestionCategory/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(QuestionCategoryVM questionCategory)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.Update(questionCategory);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionCategory };
            }
        }

        // POST: api/QuestionCategory/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.Delete(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // POST: api/QuestionCategory/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // GET: api/QuestionCategory/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(QuestionCategoryVM questionCategory)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionCategory };
            }
        }

        // GET: api/QuestionCategory/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.Dropdown();
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/QuestionCategory/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _questionCategoryService = new QuestionCategoryService();
                resultVM = await _questionCategoryService.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/QuestionCategory/ReportPreview
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

                _questionCategoryService = new QuestionCategoryService();
                PeramModel peramModel = new PeramModel { CompanyId = vm.CompanyId };

                var resultVM = await _questionCategoryService.ReportPreview(
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

                    var stream = httpRequestHelper.PostDataReport(baseUrl + "/api/QuestionCategory/GetQuestionCategory", authModel, json);

                    if (stream == null) throw new Exception("Failed to generate report.");

                    return new FileStreamResult(stream, "application/pdf")
                    {
                        FileDownloadName = "QuestionCategory.pdf"
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
