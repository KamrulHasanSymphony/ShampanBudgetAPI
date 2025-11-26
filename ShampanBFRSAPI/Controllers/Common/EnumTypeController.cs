using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;

namespace ShampanBFRSAPI.Controllers.Common
{
    [Route("api/[controller]")]
    [ApiController]
    public class EnumTypeController : ControllerBase
    {
        // POST: api/EnumType/Insert
        [HttpPost("Insert")]
        public async Task<ResultVM> Insert(EnumTypeVM enumType)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            EnumTypeService _enumTypeService = new EnumTypeService();

            try
            {
                resultVM = await _enumTypeService.Insert(enumType);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not created.",
                    ExMessage = ex.Message,
                    DataVM = enumType
                };
            }
        }

        // POST: api/EnumType/Update
        [HttpPost("Update")]
        public async Task<ResultVM> Update(EnumTypeVM enumType)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                EnumTypeService _enumTypeService = new EnumTypeService();
                resultVM = await _enumTypeService.Update(enumType);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not updated.",
                    ExMessage = ex.Message,
                    DataVM = enumType
                };
            }
        }

        // POST: api/EnumType/Delete
        [HttpPost("Delete")]
        public async Task<ResultVM> Delete(EnumTypeVM enumType)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = null, DataVM = null };
            try
            {
                EnumTypeService _enumTypeService = new EnumTypeService();

                string?[] IDs = null;
                IDs = new string?[] { enumType.Id.ToString() };

                resultVM = await _enumTypeService.Delete(IDs);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = "Data not deleted.",
                    ExMessage = ex.Message,
                    DataVM = enumType
                };
            }
        }

        // POST: api/EnumType/List
        [HttpPost("List")]
        public async Task<ResultVM> List(EnumTypeVM enumType)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                EnumTypeService _enumTypeService = new EnumTypeService();
                resultVM = await _enumTypeService.List(new[] { "M.Id" }, new[] { enumType.Id.ToString() }, null);
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

        // GET: api/EnumType/ListAsDataTable
        [HttpGet("ListAsDataTable")]
        public async Task<ResultVM> ListAsDataTable(EnumTypeVM enumType)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                EnumTypeService _enumTypeService = new EnumTypeService();
                resultVM = await _enumTypeService.ListAsDataTable(new[] { "" }, new[] { "" });
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

        // GET: api/EnumType/Dropdown
        [HttpGet("Dropdown")]
        public async Task<ResultVM> Dropdown()
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                EnumTypeService _enumTypeService = new EnumTypeService();
                resultVM = await _enumTypeService.Dropdown(); // Adjust if Dropdown requires a different method
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
