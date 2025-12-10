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
    public class SegmentController : ControllerBase
    {
        SegmentService _segmentService = new SegmentService();
        CommonService _common = new CommonService();

        // POST: api/Segment/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(SegmentVM segment)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.Insert(segment);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = segment };
            }
        }

        // POST: api/Segment/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(SegmentVM segment)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.Update(segment);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = segment };
            }
        }

        // POST: api/Segment/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.Delete(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // POST: api/Segment/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // GET: api/Segment/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(SegmentVM segment)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = segment };
            }
        }

        // GET: api/Segment/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.Dropdown();
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/Segment/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _segmentService = new SegmentService();
                resultVM = await _segmentService.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        // POST: api/Segment/ReportPreview
        [HttpPost("ReportPreview")]
        public async Task<FileStreamResult> ReportPreview(CommonVM vm)
        {
            _common = new CommonService();
            ResultVM settingResult = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

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

                _segmentService = new SegmentService();

                PeramModel peramModel = new PeramModel { CompanyId = vm.CompanyId };

                var resultVM = await _segmentService.ReportPreview(
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

                    var stream = httpRequestHelper.PostDataReport(baseUrl + "/api/COAGroup/GetCOAGroup", authModel, json);

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

