using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Ceiling;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.Question;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;

namespace ShampanBFRSAPI.Controllers.Ceiling
{
    [Route("api/[controller]")]
    [ApiController]
    public class CeilingController : ControllerBase
    {

        CeilingService _CeilingService = new CeilingService();
        CommonService _common = new CommonService();

        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(CeilingVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.Insert(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("Update")]
        public async Task<ResultVM> Update(CeilingVM VM)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.Update(VM);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = VM };
            }
        }

        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("GetAllSabreDataForDetails")]
        public async Task<ResultVM> GetAllSabreDataForDetails(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.GetAllSabreDataForDetails(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("CeilingList")]
        public async Task<ResultVM> CeilingList(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.CeilingList(new[] { "H.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("BudgetFinalReport")]
        public async Task<ResultVM> BudgetFinalReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.BudgetFinalReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

    }
}
