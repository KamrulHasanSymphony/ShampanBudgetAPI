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
    public class QuestionChapterController : ControllerBase
    {
        QuestionChapterService _questionChapterService = new QuestionChapterService();
        CommonService _common = new CommonService();

        // POST: api/QuestionChapter/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(QuestionChapterVM questionChapter)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.Insert(questionChapter);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionChapter };
            }
        }

        // POST: api/QuestionChapter/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(QuestionChapterVM questionChapter)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.Update(questionChapter);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionChapter };
            }
        }

        // POST: api/QuestionChapter/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.Delete(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // POST: api/QuestionChapter/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // GET: api/QuestionChapter/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(QuestionChapterVM questionChapter)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionChapter };
            }
        }

        // GET: api/QuestionChapter/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.Dropdown();
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/QuestionChapter/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                resultVM = await _questionChapterService.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/QuestionChapter/ReportPreview
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

                var resultVM = await _questionChapterService.ReportPreview(
                    new[] { "H.Id", "H.BranchId" }, new[] { vm.Id, vm.BranchId }, null);

                if (resultVM.Status == "Success" && resultVM.DataVM is DataTable dt && dt.Rows.Count > 0)
                {
                    string json = ExtensionMethods.DataTableToJson(dt);
                    HttpRequestHelper httpRequestHelper = new HttpRequestHelper();

                    var authModel = httpRequestHelper.GetAuthentication(new CredentialModel
                    {
                        ApiKey = DatabaseHelper.GetKey(),
                        PathName = baseUrl
                    });

                    var stream = httpRequestHelper.PostDataReport(baseUrl + "/api/QuestionChapter/GetQuestionChapter", authModel, json);

                    if (stream == null) throw new Exception("Failed to generate report.");

                    return new FileStreamResult(stream, "application/pdf")
                    {
                        FileDownloadName = "QuestionChapter.pdf"
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
