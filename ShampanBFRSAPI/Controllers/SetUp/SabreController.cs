using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class SabreController : ControllerBase
    {
        SabreService _sabreService = new SabreService();

        // POST: api/Sabre/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(SabresVM vm)
        {
            try
            {
                return await _sabreService.Insert(vm);
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.ToString(),
                    DataVM = vm
                };
            }
        }

        // POST: api/Sabre/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(SabresVM vm)
        {
            try
            {
                return await _sabreService.Update(vm);
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.ToString(),
                    DataVM = vm
                };
            }
        }

        // POST: api/Sabre/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            try
            {
                return await _sabreService.Delete(vm);
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.ToString(),
                    DataVM = vm
                };
            }
        }

        // POST: api/Sabre/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                resultVM = await _sabreService.List(new[] { "H.Id" }, new[] { vm.Id.ToString() }, null);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = "Data not fetched.",
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        // POST: api/Sabre/ListAsDataTable
        [HttpPost("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(SabresVM vm)
        {
            try
            {
                return await _sabreService.ListAsDataTable(new[] { "" }, new[] { "" });
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.ToString(),
                    DataVM = vm
                };
            }
        }

        // GET: api/Sabre/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            try
            {
                return await _sabreService.Dropdown();
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.ToString(),
                    DataVM = null
                };
            }
        }

        // POST: api/Sabre/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                List<string> conditionFields = new List<string>
                {
                            
                };

                List<string> conditionValues = new List<string>
                {
                            
                };
                string[] finalConditionFields = conditionFields.ToArray();
                string[] finalConditionValues = conditionValues.ToArray();

                result = await _sabreService.GetGridData(options, finalConditionFields, finalConditionValues);
                return result;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = null
                };
            }
        }

        // POST: api/Sabre/ReportPreview
        [HttpPost("ReportPreview")]
        public async Task<ResultVM> ReportPreview(CommonVM vm)
        {
            try
            {
                PeramModel peramModel = new PeramModel();
                peramModel.CompanyId = vm.CompanyId;
                return await _sabreService.ReportPreview(new[] { "H.Id", "H.BranchId" }, new[] { vm.Id, vm.BranchId }, peramModel);
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.ToString(),
                    DataVM = vm
                };
            }
        }
    }
}

