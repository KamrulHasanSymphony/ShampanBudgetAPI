using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class SettingsController : ControllerBase
    {
        SettingsService _service = new SettingsService();

        // POST: api/Settings/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(SettingVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _service = new SettingsService();

            try
            {
                resultVM = await _service.Insert(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }

        // POST: api/Settings/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(SettingVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _service = new SettingsService();
                resultVM = await _service.Update(vm);
                return resultVM;
            }
            catch (Exception ex)
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

        // POST: api/Settings/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, IDs = null, DataVM = null };
            try
            {
                _service = new SettingsService();
              
                resultVM = await _service.Delete(vm);
                return resultVM;
            }
            catch (Exception ex)
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

        // POST: api/Settings/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _service = new SettingsService();
                resultVM = await _service.List(new[] { "M.Id" }, new[] { vm.Id }, null);
                return resultVM;
            }
            catch (Exception ex)
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

        // GET: api/Settings/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(SettingVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _service = new SettingsService();
                resultVM = await _service.ListAsDataTable(new[] { "" }, new[] { "" });
                return resultVM;
            }
            catch (Exception ex)
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

        // GET: api/Settings/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _service = new SettingsService();
                resultVM = await _service.Dropdown(); // Adjust if Dropdown requires a different method
                return resultVM;
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


        // POST: api/Settings/DbUpdate
        [HttpPost("DbUpdate")]
        public async Task<ResultVM> DbUpdate(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _service = new SettingsService();

            try
            {
                resultVM = await _service.DbUpdate(vm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = MessageModel.Fail,
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = vm
                };
            }
        }



    }
}
