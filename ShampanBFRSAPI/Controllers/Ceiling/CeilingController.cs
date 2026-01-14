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
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
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
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
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
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
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

        [HttpPost("xxBudgetFinalReport")]
        public async Task<ResultVM> xxBudgetFinalReport(CommonVM vm)
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
        [HttpPost("BudgetLoadFinalReport")]
        public async Task<ResultVM> BudgetLoadFinalReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.BudgetLoadFinalReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("GridDataReportType")]
        public async Task<ResultVM> GridDataReportType(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.GridDataReportType(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("GetCeilingDetailDataById")]
        public async Task<ResultVM> GetCeilingDetailDataById(GridOptions options, int masterId)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {

                resultVM = await _CeilingService.GetCeilingDetailDataById(options, masterId);
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

        [HttpPost("xxxBudgetTransfer")]
        public async Task<ResultVM> xxBudgetTransfer(CeilingVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _CeilingService = new CeilingService();
                resultVM = await _CeilingService.BudgetTransfer(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("MultiplePost")]
        public async Task<ResultVM> MultiplePost(CommonVM vm)
        {
            var result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = null, DataVM = null };

            try
            {
                result = await _CeilingService.MultiplePost(vm);
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



    }
}
