using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Ceiling;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.SalaryAllowance;
using ShampanBFRS.ViewModel.Ceiling;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;

namespace ShampanBFRSAPI.Controllers.Ceiling
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductBudgetController : ControllerBase
    {
        ProductBudgetService _Service = new ProductBudgetService();
        CommonService _common = new CommonService();

        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(ProductBudgetMasterVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                _Service = new ProductBudgetService();
                resultVM = await _Service.Insert(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        [HttpPost("Update")]
        public async Task<ResultVM> Update(ProductBudgetVM VM)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new ProductBudgetService();
                resultVM = await _Service.Update(VM);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = VM };
            }
        }


        [HttpPost("GetProductBudgetDataForDetailsNew")]
        public async Task<ResultVM> GetProductBudgetDataForDetailsNew(ProductBudgetVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new ProductBudgetService();
                resultVM = await _Service.GetProductBudgetDataForDetailsNew(model);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("ProductBudgetList")]
        public async Task<ResultVM> ProductBudgetList(ProductBudgetMasterVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new ProductBudgetService();
                resultVM = await _Service.ProductBudgetList(model);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("ProductBudgeDistincttList")]
        public async Task<ResultVM> ProductBudgeDistincttList(ProductBudgetMasterVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new ProductBudgetService();
                resultVM = await _Service.ProductBudgeDistincttList(model);
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
                _Service = new ProductBudgetService();
                resultVM = await _Service.GetGridData(options);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
            }
        }

        [HttpPost("ReportPreview")]
        public async Task<ResultVM> ReportPreview(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            try
            {
                resultVM = await _Service.ReportPreview(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

    }
}
