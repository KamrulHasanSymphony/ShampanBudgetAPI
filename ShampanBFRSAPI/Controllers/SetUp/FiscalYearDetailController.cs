using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class FiscalYearDetailController : ControllerBase
    {
        // POST: api/FiscalYearDetail/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(FiscalYearDetailVM fiscalYearDetail)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            FiscalYearDetailService _fiscalYearDetailService = new FiscalYearDetailService();

            try
            {
                resultVM = await _fiscalYearDetailService.Insert(fiscalYearDetail);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not created.",
                    ExMessage = ex.Message,
                    DataVM = fiscalYearDetail
                };
            }
        }

        // POST: api/FiscalYearDetail/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(FiscalYearDetailVM fiscalYearDetail)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                FiscalYearDetailService _fiscalYearDetailService = new FiscalYearDetailService();
                resultVM = await _fiscalYearDetailService.Update(fiscalYearDetail);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not updated.",
                    ExMessage = ex.Message,
                    DataVM = fiscalYearDetail
                };
            }
        }

        // POST: api/FiscalYearDetail/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(FiscalYearDetailVM fiscalYearDetail)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, IDs = null, DataVM = null };
            try
            {
                FiscalYearDetailService _fiscalYearDetailService = new FiscalYearDetailService();

                string?[] IDs = null;
                IDs = new string?[] { fiscalYearDetail.Id.ToString() };

                resultVM = await _fiscalYearDetailService.Delete(IDs);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not deleted.",
                    ExMessage = ex.Message,
                    DataVM = fiscalYearDetail
                };
            }
        }

        // POST: api/FiscalYearDetail/List
        [HttpPost("List")]
        public async Task<ResultVM> List(FiscalYearDetailVM fiscalYearDetail)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                FiscalYearDetailService _fiscalYearDetailService = new FiscalYearDetailService();
                resultVM = await _fiscalYearDetailService.List(new[] { "M.Id" }, new[] { fiscalYearDetail.Id.ToString() }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        // GET: api/FiscalYearDetail/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(FiscalYearDetailVM fiscalYearDetail)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                FiscalYearDetailService _fiscalYearDetailService = new FiscalYearDetailService();
                resultVM = await _fiscalYearDetailService.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        // GET: api/FiscalYearDetail/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                FiscalYearDetailService _fiscalYearDetailService = new FiscalYearDetailService();
                resultVM = await _fiscalYearDetailService.Dropdown(); // Adjust if Dropdown requires a different method
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }
    }
}
