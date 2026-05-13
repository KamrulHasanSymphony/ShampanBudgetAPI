using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.PieChart;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRSAPI.Controllers.PieChart
{
    [Route("api/[controller]")]
    [ApiController]
    public class PieChartController : ControllerBase
    {

        [HttpPost("GetBudgetPieChart")]
        public async Task<ResultVM> GetBudgetPieChart(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                PieChartService _piechartService = new PieChartService();

                resultVM = await _piechartService.GetBudgetPieChart(Vm);


                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        [HttpPost("GetSalaryAllowancePieChart")]
        public async Task<ResultVM> GetSalaryAllowancePieChart(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                PieChartService _piechartService = new PieChartService();

                resultVM = await _piechartService.GetSalaryAllowancePieChart(Vm);


                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        [HttpPost("GetProductBudgetPieChart")]
        public async Task<ResultVM> GetProductBudgetPieChart(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                PieChartService _piechartService = new PieChartService();

                resultVM = await _piechartService.GetProductBudgetPieChart(Vm);


                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        [HttpPost("GetSaleBudgetPieChart")]
        public async Task<ResultVM> GetSaleBudgetPieChart(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                PieChartService _piechartService = new PieChartService();

                resultVM = await _piechartService.GetSaleBudgetPieChart(Vm);


                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }
    }


}
