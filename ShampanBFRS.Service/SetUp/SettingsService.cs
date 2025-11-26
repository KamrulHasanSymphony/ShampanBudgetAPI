using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data.SqlClient;

namespace ShampanBFRS.Service.SetUp
{
    public class SettingsService
    {
        public async Task<ResultVM> Insert(SettingVM vm)
        {
            SettingsRepository _repo = new SettingsRepository();
            CommonRepository _commonRepo = new CommonRepository();

            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                string[] conditionField = { "SettingGroup", "SettingName" };
                string[] conditionValue = { vm.SettingGroup.Trim(), vm.SettingName.Trim() };

                bool exist = _commonRepo.CheckExists("Settings", conditionField, conditionValue,conn, transaction);

                if (exist)
                {
                    result.Message = "Data Already Exist!";
                }
                else
                {
                    result = await _repo.Insert(vm, conn, transaction);
                }                

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> Update(SettingVM vm)
        {
            SettingsRepository _repo = new SettingsRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.Update(vm, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> Delete(CommonVM vm)
        {
            SettingsRepository _repo = new SettingsRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = vm.IDs, DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.Delete(vm, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            SettingsRepository _repo = new SettingsRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.List(conditionalFields, conditionalValues, vm, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            SettingsRepository _repo = new SettingsRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.ListAsDataTable(conditionalFields, conditionalValues, vm, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> Dropdown()
        {
            SettingsRepository _repo = new SettingsRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();

                result = await _repo.Dropdown(conn, transaction);

                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> DbUpdate(CommonVM vm)
        {
            SettingsRepository _repo = new SettingsRepository();
            CommonRepository _commonRepo = new CommonRepository();

            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                transaction = conn.BeginTransaction();


                #region Codes Generate
                result = await _repo.CodesDataInsert(vm, "ProductGroup", "ProductGroup", "PG", "5", "1", conn, transaction);
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion

                #region Settings Generate

                result = await _repo.SettingsDataInsert(vm, "DMSApiUrl", "DMSApiUrl", "-", "String", true);
                result = await _repo.SettingsDataInsert(vm, "DMSReportUrl", "DMSReportUrl", "-", "String", true);
                result = await _repo.SettingsDataInsert(vm, "DecimalPlace", "DecimalPlace", "-", "String", true);
                result = await _repo.SettingsDataInsert(vm, "SaleDecimalPlace", "SaleDecimalPlace", "-", "String", true);
                result = await _repo.SettingsDataInsert(vm, "PurchaseDecimalPlace", "PurchaseDecimalPlace", "-", "String", true);

                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion

                #region New Table Add
                string sqlText = " ";

                #region Role
                sqlText = " ";

                sqlText = @"
CREATE TABLE [dbo].[Role](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](50) NULL,
	[CreatedBy] [nvarchar](50) NULL,
	[CreatedOn] [datetime] NULL,
	[CreatedFrom] [nvarchar](50) NULL,
	[LastModifiedBy] [nvarchar](50) NULL,
	[LastModifiedOn] [datetime] NULL,
	[LastUpdateFrom] [nvarchar](50) NULL,
 CONSTRAINT [PK_Role] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

";

                result = await _repo.NewTableAdd("Role", sqlText, conn, transaction);
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion

                #region RoleMenu
                sqlText = " ";

                sqlText = @"
CREATE TABLE [dbo].[RoleMenu](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[RoleId] [int] NULL,
	[UserGroupId] [int] NULL,
	[MenuId] [int] NULL,
	[List] [bit] NULL,
	[Insert] [bit] NULL,
	[Delete] [bit] NULL,
	[Post] [bit] NULL,
	[CreatedBy] [nvarchar](50) NULL,
	[CreatedOn] [datetime] NULL,
	[CreatedFrom] [nvarchar](50) NULL,
	[LastModifiedBy] [nvarchar](50) NULL,
	[LastModifiedOn] [datetime] NULL,
	[LastUpdateFrom] [nvarchar](50) NULL,
 CONSTRAINT [PK_RoleMenu] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

";

                result = await _repo.NewTableAdd("RoleMenu", sqlText, conn, transaction);
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion

                #region Menu
                sqlText = " ";

                sqlText = @"
CREATE TABLE [dbo].[Menu](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Url] [nvarchar](250) NULL,
	[Name] [nvarchar](250) NULL,
	[Module] [nvarchar](250) NULL,
	[Controller] [nvarchar](250) NULL,
	[ParentId] [int] NULL,
	[SubParentId] [int] NULL,
	[SubChildId] [int] NULL,
	[DisplayOrder] [int] NULL,
	[CreatedBy] [nvarchar](50) NULL,
	[CreatedOn] [datetime] NULL,
	[CreatedFrom] [nvarchar](50) NULL,
	[LastModifiedBy] [nvarchar](50) NULL,
	[LastModifiedOn] [datetime] NULL,
	[LastUpdateFrom] [nvarchar](50) NULL,
	[IconClass] [nvarchar](100) NULL,
	[IsActive] [bit] NOT NULL,
 CONSTRAINT [PK_Menu] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

";

                result = await _repo.NewTableAdd("Menu", sqlText, conn, transaction);
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion

                #region UserMenu
                sqlText = " ";

                sqlText = @"
CREATE TABLE [dbo].[UserMenu](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[UserId] [nvarchar](256) NULL,
	[RoleId] [int] NULL,
	[MenuId] [int] NULL,
	[CreatedBy] [nvarchar](50) NULL,
	[CreatedOn] [datetime] NULL,
	[CreatedFrom] [nvarchar](50) NULL,
	[LastModifiedBy] [nvarchar](50) NULL,
	[LastModifiedOn] [datetime] NULL,
	[LastUpdateFrom] [nvarchar](50) NULL,
	[List] [bit] NULL,
	[Insert] [bit] NULL,
	[Delete] [bit] NULL,
	[Post] [bit] NULL,
 CONSTRAINT [PK_UserMenu] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

";

                result = await _repo.NewTableAdd("UserMenu", sqlText, conn, transaction);
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion


                #endregion

                #region  AddField

                result = await _repo.DBTableFieldAdd("Settings", "IsActive", "bit", false, conn, transaction);
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion

                #region FieldAlter

                result = await _repo.DBTableFieldAlter("Settings", "IsActive", "bit");
                if (result.Status == "Fail")
                {
                    throw new Exception(result.Message);
                }
                #endregion


                if (isNewConnection && result.Status == "Success")
                {
                    transaction.Commit();
                }
                else
                {
                    throw new Exception(result.Message);
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.Status = "Fail";
                result.Message = ex.Message.ToString();
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }


    }


}
