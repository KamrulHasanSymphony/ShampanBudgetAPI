using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.AccountVMs;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserProfileController : ControllerBase
    {
        UserProfileService _service = new UserProfileService();

        // POST: api/UserProfile/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(UserProfileVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _service = new UserProfileService();

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

        [HttpPost("UserInformationsInsert")]
        public async Task<ResultVM> UserInformationsInsert(UserInformationVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _service = new UserProfileService();

            try
            {
                resultVM = await _service.UserInformationsInsert(vm);
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

        // POST: api/UserProfile/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(UserProfileVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                 _service = new UserProfileService();
                resultVM = await _service.Update(vm);
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

        // POST: api/UserProfile/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, IDs = null, DataVM = null };
            try
            {
                 _service = new UserProfileService();
                resultVM = await _service.Delete(vm);
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

        // POST: api/UserProfile/List
        [HttpPost("List")]
        public async Task<ResultVM> List(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                 _service = new UserProfileService();
                resultVM = await _service.List(new[] { "U.Id" }, new[] { vm.Id}, null);
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

        // GET: api/UserProfile/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                 _service = new UserProfileService();
                resultVM = await _service.ListAsDataTable(new[] { "" }, new[] { "" });
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

        // GET: api/UserProfile/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                UserProfileService _service = new UserProfileService();
                resultVM = await _service.Dropdown(); 
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

        // POST: api/UserProfile/GetGridData
        [HttpPost("GetGridData")]
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _service = new UserProfileService();
                resultVM = await _service.GetGridData(options);
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


    }
}
