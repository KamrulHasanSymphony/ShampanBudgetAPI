using Microsoft.AspNetCore.Http;
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

        //[HttpPost("GetBudgetDataForDetailsNew")]
        //public async Task<ResultVM> GetBudgetDataForDetailsNew(BudgetHeaderVM model)
        //{
        //    ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
        //    try
        //    {
        //        _Service = new BudgetService();
        //        resultVM = await _Service.GetBudgetDataForDetailsNew(model);
        //        return resultVM;
        //    }
        //    catch (Exception ex)
        //    {
        //        return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
        //    }
        //}

        [HttpPost("BudgetList")]
        public async Task<ResultVM> BudgetList(BudgetHeaderVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.BudgetList(model);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("BudgeDistincttList")]
        public async Task<ResultVM> BudgeDistincttList(BudgetHeaderVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.BudgeDistincttList(model);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _Service = new BudgetService();
                resultVM = await _Service.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }


    }
}
