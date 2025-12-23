using Microsoft.AspNetCore.Mvc;
using ShampanBFRS.Service.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.ExtensionMethods;
using ShampanBFRS.ViewModel.SetUpVMs;
using System.Data;

namespace ShampanBFRSAPI.Controllers.Common
{
    [Route("api/[controller]")]
    [ApiController]
    public class CommonController : ControllerBase
    {

        [HttpPost("NextPrevious")]
        public async Task<ResultVM> NextPrevious(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.NextPrevious(vm.Id, vm.Status, vm.TableName, vm.Type);
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

        // POST: api/Common/GetSettingsValue
        [HttpPost("GetSettingsValue")]
        public async Task<ResultVM> GetSettingsValue(CommonVM vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.SettingsValue(new[] { "SettingGroup", "SettingName" }, new[] { vm.Group, vm.Name }, null);

                if (resultVM.Status == "Success" && resultVM.DataVM is DataTable settingValue)
                {
                    resultVM.DataVM = null;
                    resultVM.DataVM = ExtensionMethods.ConvertDataTableToList(settingValue);
                }

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

        // POST: api/Common/EnumList
        [HttpPost("EnumList")]
        public async Task<ResultVM> EnumList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                 resultVM = await _commonService.EnumList(
            new[] { "EnumType" },
            new[] { Vm.Value?.ToString() ?? string.Empty },
            null);
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

        //// POST: api/Common/CustomerList
        //[HttpPost("CustomerList")]
        //public async Task<ResultVM> CustomerList(CommonVM Vm)
        //{
        //    ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
        //    try
        //    {
        //        CommonService _commonService = new CommonService();
        //        resultVM = await _commonService.CustomerList(new[] { "" }, new[] { "" }, null);
        //        return resultVM;
        //    }
        //    catch (Exception ex)
        //    {
        //        return new ResultVM
        //        {
        //            Status = "Fail",
        //            Message = "Data not fetched.",
        //            ExMessage = ex.Message,
        //            DataVM = null
        //        };
        //    }
        //}

        // POST: api/Common/GetProductModalData
        [HttpPost("GetProductModalData")]
        public async Task<ResultVM> GetProductModalData(ProductVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                PeramModel vm = new PeramModel();


                resultVM = await _commonService.GetProductModalData(new[] { "P.Code like", "P.Name like" }, new[] { model.Code, model.Name }, model.PeramModel);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = model
                };
            }
        }
        // POST: api/Common/GetSegmentData
        [HttpPost("GetSegmentData")]
        public async Task<ResultVM> GetSegmentData(SegmentVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                PeramModel vm = new PeramModel();


                resultVM = await _commonService.GetSegmentData(new[] { "P.Code like", "P.Name like" }, new[] { model.Code, model.Name }, model.PeramModel);
                return resultVM;
            }
            catch (Exception ex)
            {
                return new ResultVM
                {
                    Status = "Fail",
                    Message = ex.Message,
                    ExMessage = ex.Message,
                    DataVM = model
                };
            }
        }

        [HttpPost("GetFiscalYearComboBox")]
        public async Task<ResultVM> GetFiscalYearComboBox(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.GetFiscalYearComboBox(new[] { "Id" }, new[] { "" }, null);
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

        [HttpPost("COAList")]
        public async Task<ResultVM> COAList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.COAList(new[] { "" }, new[] { "" }, null);
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

        [HttpPost("DepartmentList")]
        public async Task<ResultVM> DepartmentList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.DepartmentList(new[] { "" }, new[] { "" }, null);
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
        [HttpPost("SabreList")]
        public async Task<ResultVM> SabreList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.SabreList(new[] { "" }, new[] { "" }, null);
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

        [HttpPost("COAGroupList")]
        public async Task<ResultVM> COAGroupList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.COAGroupList(new[] { "" }, new[] { "" }, null);
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

        [HttpPost("StructureList")]
        public async Task<ResultVM> StructureList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.StructureList(new[] { "" }, new[] { "" }, null);
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

        [HttpPost("ProductGroupList")]
        public async Task<ResultVM> ProductGroupList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.ProductGroupList(new[] { "" }, new[] { "" }, null);
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

        [HttpPost("ProductList")]
        public async Task<ResultVM> ProductList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.ProductList(new[] { "" }, new[] { "" }, null);
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



        [HttpPost("PersonnelCategoriesList")]
        public async Task<ResultVM> PersonnelCategoriesList(CommonVM Vm)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                CommonService _commonService = new CommonService();
                resultVM = await _commonService.PersonnelCategoriesList(new[] { "" }, new[] { "" }, null);
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
