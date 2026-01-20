using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Ceiling;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.Reports;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRSAPI.Controllers.Reports
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReportController : ControllerBase
    {
        ReportService _Service = new ReportService();
        CommonService _common = new CommonService();

        [HttpPost("IncomeStatementAllReport")]
        public async Task<ResultVM> IncomeStatementAllReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.IncomeStatementAllReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("ProductIncomeStatementReport")]
        public async Task<ResultVM> ProductIncomeStatementReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.ProductIncomeStatementReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("CashFlowStatementReport")]
        public async Task<ResultVM> CashFlowStatementReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.CashFlowStatementReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("PaymentToGovernmentStatementReport")]
        public async Task<ResultVM> PaymentToGovernmentStatementReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.PaymentToGovernmentStatementReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("OperatingDetailStatementReport")]
        public async Task<ResultVM> OperatingDetailStatementReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.OperatingDetailStatementReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("NonOperatingIncomeReport")]
        public async Task<ResultVM> NonOperatingIncomeReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.NonOperatingIncomeReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }


        [HttpPost("IncomeStatementReport")]
        public async Task<ResultVM> IncomeStatementReport(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.IncomeStatementReport(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }



    }
}
