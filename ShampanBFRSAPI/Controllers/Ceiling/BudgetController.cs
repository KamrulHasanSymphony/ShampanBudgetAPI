using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Ceiling;
using ShampanBFRS.Service.Common;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;

namespace ShampanBFRSAPI.Controllers.Ceiling
{
    [Route("api/[controller]")]
    [ApiController]
    public class BudgetController : ControllerBase
    {
       BudgetService _Service = new BudgetService();
        CommonService _common = new CommonService();

        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(BudgetHeaderVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.Insert(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("Update")]
        public async Task<ResultVM> Update(BudgetHeaderVM VM)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.Update(VM);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = VM };
            }
        }

        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _Service.List(new[] { "M.Id" }, new[] { vm.Id.ToString() }, null);
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
    
        [HttpPost("GetBudgetDataForDetailsNew")]
        public async Task<ResultVM> GetBudgetDataForDetailsNew(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.GetBudgetDataForDetailsNew(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }
      
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options )
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {

                List<string> conditionFields = new List<string>
                {
                    "M.CreatedBy",
                    "M.BudgetType",         
                    "M.TransactionType"

                };

                List<string> conditionValues = new List<string>
                {
                    options.vm.UserId.ToString(), options.vm.BudgetType.ToString(),options.vm.TransactionType.ToString()
                };
                string[] finalConditionFields = conditionFields.ToArray();
                string[] finalConditionValues = conditionValues.ToArray();

                return await _Service.GetGridData(options, finalConditionFields, finalConditionValues);

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

        [HttpPost("GetDetailDataById")]
        public async Task<ResultVM> GetDetailDataById(GridOptions options, int masterId)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {

                resultVM = await _Service.GetDetailDataById(options, masterId);
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

        [HttpPost("MultiplePost")]
        public async Task<ResultVM> MultiplePost(CommonVM vm)
        {
            var result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = null, DataVM = null };

            try
            {
                result = await _Service.MultiplePost(vm);
                return result;
            }
            catch (System.Exception ex)
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

        [HttpPost("BudgetFinalReport")]
        public async Task<ResultVM> BudgetFinalReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.BudgetFinalReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("BudgetTransfer")]
        public async Task<ResultVM> BudgetTransfer(BudgetHeaderVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.BudgetTransfer(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

    }
}
