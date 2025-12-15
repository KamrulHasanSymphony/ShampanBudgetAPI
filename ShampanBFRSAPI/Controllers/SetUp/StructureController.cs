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
using Microsoft.AspNetCore.Cors.Infrastructure;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class StructureController : ControllerBase
    {
        StructureService _service = new StructureService();
        CommonService _common = new CommonService();

        // POST: api/MaintenanceRecord/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(StructureVM vm)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail };
            try
            {
                result = await _service.Insert(vm);
                return result;
            }
            catch (Exception ex)
            {
                result.Message =MessageModel.InsertFail;
                result.ExMessage = ex.Message;
                result.DataVM = vm;
                return result;
            }
        }

        // POST: api/MaintenanceRecord/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(StructureVM vm)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail };
            try
            {
                result = await _service.Update(vm);
                return result;
            }
            catch (Exception ex)
            {
                result.Message = MessageModel.UpdateFail;
                result.ExMessage = ex.Message;
                result.DataVM = vm;
                return result;
            }
        }

        // POST: api/MaintenanceRecord/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail };
            try
            {
                result = await _service.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return result;
            }
            catch (Exception ex)
            {
                result.Message = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.DataVM = vm;
                return result;
            }
        }
        // POST: api/MaintenanceRecord/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(StructureVM vm)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = null, DataVM = null };
            try
            {
                _service = new StructureService();

                string?[] Id = new string?[] { vm.Id.ToString() };
                result = await _service.Delete(Id);

                return result;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }
        // GET: api/MaintenanceRecord/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable()
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail };
            try
            {
                result = await _service.ListAsDataTable(new[] { "" }, new[] { "" });
                return result;
            }
            catch (Exception ex)
            {
                result.Message =MessageModel.Fail;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        [HttpPost("MultiplePost")]
        public async Task<ResultVM> MultiplePost(CommonVM vm)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail };
            try
            {
                result = await _service.MultiplePost(vm);
                return result;
            }
            catch (Exception ex)
            {
                result.Message = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.DataVM = vm;
                return result;
            }
        }

        // POST: api/MaintenanceRecord/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (options == null || options.vm == null)
                {
                    result.Message = "Invalid request: options or vm is null.";
                    return result;
                }

                //List<string> conditionFields = new List<string>
                //{
                //   "1"
                //};

                //List<string> conditionValues = new List<string>
                //    {
                //        "1"
                //    };

                //string[] finalConditionFields = conditionFields.ToArray();
                //string[] finalConditionValues = conditionValues.ToArray();

                result = await _service.GetGridData(options);
                return result;
            }
            catch (Exception ex)
            {
                result.Message = MessageModel.Fail;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        // POST: api/MaintenanceRecord/ReportPreview
        [HttpPost("ReportPreview")]
        public async Task<FileStreamResult> ReportPreview(CommonVM vm)
        {
            ResultVM settingResult = new ResultVM { Status = "Fail" };
            try
            {
                string baseUrl = "";
                settingResult = await _common.SettingsValue(new[] { "SettingGroup", "SettingName" }, new[] { "DMSReportUrl", "DMSReportUrl" }, null);

                if (settingResult.Status == "Success" && settingResult.DataVM is DataTable settingValue && settingValue.Rows.Count > 0)
                    baseUrl = settingValue.Rows[0]["SettingValue"].ToString();

                if (string.IsNullOrEmpty(baseUrl))
                    throw new Exception("Report API URL not found.");

                PeramModel param = new PeramModel { CompanyId = vm.CompanyId };
                var resultVM = await _service.ReportPreview(new[] { "M.Id", "M.BranchId", "M.WorkOrderDate between", "M.WorkOrderDate between" }, new[] { vm.Id, vm.BranchId, vm.FromDate, vm.ToDate }, param);

                if (resultVM.Status == "Success" && resultVM.DataVM is DataTable dt && dt.Rows.Count > 0)
                {
                    string json = ExtensionMethods.DataTableToJson(dt);
                    HttpRequestHelper helper = new HttpRequestHelper();
                    var authModel = helper.GetAuthentication(new CredentialModel { ApiKey = DatabaseHelper.GetKey(), PathName = baseUrl });

                    var stream = helper.PostDataReport(baseUrl + "/api/MM/GetMaintenanceRecord", authModel, json);
                    if (stream == null)
                        throw new Exception("Failed to generate report.");

                    return new FileStreamResult(stream, "application/pdf") { FileDownloadName = "MaintenanceRecord.pdf" };
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

