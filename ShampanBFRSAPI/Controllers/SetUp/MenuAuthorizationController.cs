using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;

namespace ShampanBFRSAPI.Controllers.SetUp
{
    [Route("api/[controller]")]
    [ApiController]
    public class MenuAuthorizationController : ControllerBase
    {
        MenuAuthorizationService _menuAuthorization = new MenuAuthorizationService();


        // POST: api/MenuAuthorizationController/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(UserRoleVM urm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _menuAuthorization = new MenuAuthorizationService();

            try
            {
                resultVM = await _menuAuthorization.Insert(urm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = urm
                };
            }
        }
        // POST: api/MenuAuthorizationController/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(UserRoleVM urm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _menuAuthorization = new MenuAuthorizationService();

            try
            {
                resultVM = await _menuAuthorization.Update(urm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = urm
                };
            }
        }
        // POST: api/MenuAuthorizationController/GetRoleIndexData
        [HttpPost("GetRoleIndexData")]
        public async Task<ResultVM> GetRoleIndexData(GridOptions options)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                _menuAuthorization = new MenuAuthorizationService();
                resultVM = await _menuAuthorization.GetRoleIndexData(options);
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

        // POST: api/MenuAuthorization/RoleMenuInsert
        [HttpPost("RoleMenuInsert")]
        public async Task<ResultVM> RoleMenuInsert(RoleMenuVM urm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            _menuAuthorization = new MenuAuthorizationService();

            try
            {
                resultVM = await _menuAuthorization.RoleMenuInsert(urm);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = urm
                };
            }
        }

    }
}
