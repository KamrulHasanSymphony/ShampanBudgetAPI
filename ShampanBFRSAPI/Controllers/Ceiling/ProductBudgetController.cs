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
    public class ProductBudgetController : ControllerBase
    {
        ProductBudgetService _Service = new ProductBudgetService();
        CommonService _common = new CommonService();


        [HttpPost("GetProductBudgetDataForDetailsLoad")]
        public async Task<ResultVM> GetProductBudgetDataForDetailsLoad(ProductBudgetVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                _Service = new ProductBudgetService();
                resultVM = await _Service.GetProductBudgetDataForDetailsLoad(model);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = MessageModel.Fail, Message = ex.Message, ExMessage = ex.Message };
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


    }
}
