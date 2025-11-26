using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Common;
using ShampanBFRS.Service.Question;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;

namespace ShampanBFRSAPI.Controllers.Question
{
    [Route("api/[controller]")]
    [ApiController]
    public class QuestionSetController : ControllerBase
    {
        private readonly QuestionSetHeaderService _questionSetHeaderService;
        private readonly CommonService _common;

        public QuestionSetController()
        {
            _questionSetHeaderService = new QuestionSetHeaderService();
            _common = new CommonService();
        }

        // POST: api/QuestionSet/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(QuestionSetHeaderVM questionSetHeader)
        {
            try
            {
                return await _questionSetHeaderService.Insert(questionSetHeader);
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionSetHeader };
            }
        }

        // POST: api/QuestionSet/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(QuestionSetHeaderVM questionSetHeader)
        {
            try
            {
                return await _questionSetHeaderService.Update(questionSetHeader);
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = questionSetHeader };
            }
        }

        // POST: api/QuestionSet/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            try
            {
                return await _questionSetHeaderService.List(new[] { "M.Id" }, new[] { vm.Id }, null);
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message, DataVM = vm };
            }
        }

        // POST: api/QuestionSet/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            try
            {
                return await _questionSetHeaderService.GetGridData(options);
            }
            catch (Exception ex)
            {
                return new ResultVM { Status = "Fail", Message = ex.Message, ExMessage = ex.Message };
            }
        }
    }
}
