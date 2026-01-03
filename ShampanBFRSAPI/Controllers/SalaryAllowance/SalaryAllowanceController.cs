using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Ceiling;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.SalaryAllowance;
using ShampanBFRS.Service.Sale;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.ExtensionMethods;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SalaryAllowance;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRSAPI.Configuration;
using System.Data;
using static ShampanBFRSAPI.Configuration.HttpRequestHelper;

namespace ShampanBFRSAPI.Controllers.SalaryAllowance
{
    [Route("api/[controller]")]
    [ApiController]
    public class SalaryAllowanceController : ControllerBase
    {
        SalaryAllowanceService _salaryAllowanceService = new SalaryAllowanceService();
        CommonService _commonService = new CommonService();

        // POST: api/SalaryAllowance/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(SalaryAllowanceHeaderVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.Insert(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        // POST: api/SalaryAllowance/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(SalaryAllowanceHeaderVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.Update(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        // POST: api/SalaryAllowance/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, IDs = null, DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.MultipleDelete(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        // POST: api/SalaryAllowance/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.List(new[] { "M.Id" }, new[] { vm.Id.ToString() }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        // GET: api/SalaryAllowance/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable()
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        // GET: api/SalaryAllowance/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.Dropdown();
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        // POST: api/SalaryAllowance/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {

                List<string> conditionFields = new List<string>
                {
                    "M.BudgetType"
                };

                List<string> conditionValues = new List<string>
                {
                options.vm.BudgetType
                };
                string[] finalConditionFields = conditionFields.ToArray();
                string[] finalConditionValues = conditionValues.ToArray();

                return await _salaryAllowanceService.GetGridData(options, finalConditionFields, finalConditionValues);

                //resultVM = await _salaryAllowanceService.GetGridData(options, new[] { "" }, new[] { "" });

                //return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        // POST: api/SalaryAllowance/MultiplePost
        [HttpPost("MultiplePost")]
        public async Task<ResultVM> MultiplePost(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, IDs = null, DataVM = null };
            try
            {
                resultVM = await _salaryAllowanceService.MultiplePost(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        [HttpPost("GetDetailDataById")]
        public async Task<ResultVM> GetDetailDataById(GridOptions options, int masterId)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {

                resultVM = await _salaryAllowanceService.GetDetailDataById(options, masterId);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        [HttpPost("GetDetailsGridData")]
        public async Task<ResultVM> GetDetailsGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {

                resultVM = await _salaryAllowanceService.GetDetailsGridData(options, new[] { "H.BranchId", "H.IsPost", "H.OrderDate between", "H.OrderDate between" }, new[] { options.vm.BranchId.ToString(), options.vm.IsPost.ToString(), options.vm.FromDate.ToString(), options.vm.ToDate.ToString() });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        [HttpPost("ReportPreview")]
        public async Task<ResultVM> ReportPreview(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _salaryAllowanceService.ReportPreview(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

    }
}

