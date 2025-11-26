using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class CompanyProfileRepository : CommonRepository
    {
        // Insert Method
         public async Task<ResultVM> Insert(CompanyProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
                INSERT INTO CompanyProfiles 
                    (
                        Code, CompanyName, CompanyBanglaName, CompanyLegalName, Address1, Address2, Address3, City, ZipCode, 
                        TelephoneNo, FaxNo, Email, ContactPerson, ContactPersonDesignation, ContactPersonTelephone, ContactPersonEmail,TINNo, VatRegistrationNo, Comments, IsArchive, IsActive, CreatedBy, CreatedOn,CreatedFrom,FYearStart, FYearEnd, BusinessNature, AccountingNature, CompanyTypeId, Section, BIN,IsVDSWithHolder, AppVersion, License
                    )
                    VALUES 
                    (
                      @Code, @CompanyName, @CompanyBanglaName, @CompanyLegalName, @Address1, @Address2, @Address3, @City, @ZipCode,
                      @TelephoneNo, @FaxNo, @Email, @ContactPerson, @ContactPersonDesignation, @ContactPersonTelephone, @ContactPersonEmail, @TINNo, @VatRegistrationNo, @Comments,@IsArchive, @IsActive, @CreatedBy, GETDATE(), @CreatedFrom, @FYearStart, @FYearEnd, @BusinessNature, @AccountingNature, @CompanyTypeId, @Section, @BIN, @IsVDSWithHolder, @AppVersion, @License
                    );
                    SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyName", vm.CompanyName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyBanglaName", vm.CompanyBanglaName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyLegalName", vm.CompanyLegalName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address1", vm.Address1 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address2", vm.Address2 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address3", vm.Address3 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@City", vm.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ZipCode", vm.ZipCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TelephoneNo", vm.TelephoneNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FaxNo", vm.FaxNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Email", vm.Email ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPerson", vm.ContactPerson ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonDesignation", vm.ContactPersonDesignation ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonTelephone", vm.ContactPersonTelephone ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonEmail", vm.ContactPersonEmail ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TINNo", vm.TINNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@VatRegistrationNo", vm.VatRegistrationNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Comments", vm.Comments ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearStart", vm.FYearStart ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearEnd", vm.FYearEnd ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BusinessNature", vm.BusinessNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@AccountingNature", vm.AccountingNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyTypeId", vm.CompanyTypeId);
                    cmd.Parameters.AddWithValue("@Section", vm.Section ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BIN", vm.BIN ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsVDSWithHolder", vm.IsVDSWithHolder);
                    cmd.Parameters.AddWithValue("@AppVersion", vm.AppVersion ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@License", vm.License ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
                    result.Id =vm.Id.ToString();
                    result.DataVM = vm;
                }


                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Update Method
         public async Task<ResultVM> Update(CompanyProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
UPDATE CompanyProfiles
SET
    CompanyName = @CompanyName,
    CompanyBanglaName = @CompanyBanglaName,
    CompanyLegalName = @CompanyLegalName,
    Address1 = @Address1,
    Address2 = @Address2,
    Address3 = @Address3,
    City = @City,
    ZipCode = @ZipCode,
    TelephoneNo = @TelephoneNo,
    FaxNo = @FaxNo,
    Email = @Email,
    ContactPerson = @ContactPerson,
    ContactPersonDesignation = @ContactPersonDesignation,
    ContactPersonTelephone = @ContactPersonTelephone,
    ContactPersonEmail = @ContactPersonEmail,
    TINNo = @TINNo,
    VatRegistrationNo = @VatRegistrationNo,
    Comments = @Comments,
    FYearStart = @FYearStart,
    FYearEnd = @FYearEnd,
    BusinessNature = @BusinessNature,
    AccountingNature = @AccountingNature,
    CompanyTypeId = @CompanyTypeId,
    Section = @Section,
    BIN = @BIN,
    IsVDSWithHolder = @IsVDSWithHolder,
    LastModifiedBy = @LastModifiedBy,    
    LastUpdateFrom = @LastUpdateFrom,
    LastModifiedOn = GETDATE(),
    AppVersion = @AppVersion,
    License = @License


WHERE Id = @Id ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@CompanyName", vm.CompanyName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyBanglaName", vm.CompanyBanglaName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyLegalName", vm.CompanyLegalName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address1", vm.Address1 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address2", vm.Address2 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address3", vm.Address3 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@City", vm.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ZipCode", vm.ZipCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TelephoneNo", vm.TelephoneNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FaxNo", vm.FaxNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Email", vm.Email ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPerson", vm.ContactPerson ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonDesignation", vm.ContactPersonDesignation ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonTelephone", vm.ContactPersonTelephone ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonEmail", vm.ContactPersonEmail ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TINNo", vm.TINNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@VatRegistrationNo", vm.VatRegistrationNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Comments", vm.Comments ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearStart", vm.FYearStart ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearEnd", vm.FYearEnd ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BusinessNature", vm.BusinessNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@AccountingNature", vm.AccountingNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyTypeId", vm.CompanyTypeId);
                    cmd.Parameters.AddWithValue("@Section", vm.Section ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BIN", vm.BIN ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsVDSWithHolder", vm.IsVDSWithHolder);
                    cmd.Parameters.AddWithValue("@AppVersion", vm.AppVersion ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@License", vm.License ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.LastModifiedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.UpdateSuccess;
                    }
                    else
                    {
                        result.Message = "No rows were updated.";
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Delete Method
         public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE CompanyProfiles SET IsArchive = 1, IsActive = 0,LastModifiedBy = @LastModifiedBy,LastUpdateFrom = @LastUpdateFrom ,LastModifiedOn =GETDATE() WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.ModifyFrom);

                    int rowsAffected = cmd.ExecuteNonQuery();


                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.DeleteSuccess;
                    }
                    else
                    {
                        throw new Exception("No rows were deleted.");
                    }
                }


                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // List Method
        public async Task<ResultVM> xxxxList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }


                string query = @"
SELECT 
    ISNULL(H.Id, 0) AS Id,
    ISNULL(H.Code, '') AS Code,
    ISNULL(H.CompanyName, '') AS CompanyName,
    ISNULL(H.CompanyBanglaName, '') AS CompanyBanglaName,
    ISNULL(H.CompanyLegalName, '') AS CompanyLegalName,
    ISNULL(H.Address1, '') AS Address1,
    ISNULL(H.Address2, '') AS Address2,
    ISNULL(H.Address3, '') AS Address3,
    ISNULL(H.City, '') AS City,
    ISNULL(H.ZipCode, '') AS ZipCode,
    ISNULL(H.TelephoneNo, '') AS TelephoneNo,
    ISNULL(H.FaxNo, '') AS FaxNo,
    ISNULL(H.Email, '') AS Email,
    ISNULL(H.ContactPerson, '') AS ContactPerson,
    ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation,
    ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone,
    ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail,
    ISNULL(H.TINNo, '') AS TINNo,
    ISNULL(H.VatRegistrationNo, '') AS VatRegistrationNo,
    ISNULL(H.Comments, '') AS Comments,
    ISNULL(H.IsArchive, 0) AS IsArchive,
    ISNULL(H.IsActive, 0) AS IsActive,
    ISNULL(H.CreatedBy, '') AS CreatedBy,
    ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS CreatedOn,
    ISNULL(H.LastModifiedBy, '') AS LastModifiedBy,
    ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS LastModifiedOn,
    ISNULL(H.CreatedFrom, '') AS CreatedFrom,
    ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom,
    ISNULL(FORMAT(H.FYearStart, 'yyyy-MM-dd'), '1900-01-01') AS FYearStart,
    ISNULL(FORMAT(H.FYearEnd, 'yyyy-MM-dd'), '1900-01-01') AS FYearEnd,
    ISNULL(H.BusinessNature, '') AS BusinessNature,
    ISNULL(H.AccountingNature, '') AS AccountingNature,
    ISNULL(H.CompanyTypeId, 0) AS CompanyTypeId,
    ISNULL(H.Section, '') AS Section,
    ISNULL(H.BIN, '') AS BIN,
    ISNULL(H.IsVDSWithHolder, 0) AS IsVDSWithHolder,
    ISNULL(H.AppVersion, '') AS AppVersion,
    ISNULL(H.License, '') AS License

FROM 
    CompanyProfiles AS H
WHERE 
    1 = 1

";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND H.Id = @Id ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new CompanyProfileVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Code = row["Code"].ToString(),
                    CompanyName = row["CompanyName"].ToString(),
                    CompanyBanglaName = row["CompanyBanglaName"].ToString(),
                    CompanyLegalName = row["CompanyLegalName"].ToString(),
                    Address1 = row["Address1"].ToString(),
                    Address2 = row["Address2"].ToString(),
                    Address3 = row["Address3"].ToString(),
                    City = row["City"].ToString(),
                    ZipCode = row["ZipCode"].ToString(),
                    TelephoneNo = row["TelephoneNo"].ToString(),
                    FaxNo = row["FaxNo"].ToString(),
                    Email = row["Email"].ToString(),
                    ContactPerson = row["ContactPerson"].ToString(),
                    ContactPersonDesignation = row["ContactPersonDesignation"].ToString(),
                    ContactPersonTelephone = row["ContactPersonTelephone"].ToString(),
                    ContactPersonEmail = row["ContactPersonEmail"].ToString(),
                    TINNo = row["TINNo"].ToString(),
                    VatRegistrationNo = row["VatRegistrationNo"].ToString(),
                    Comments = row["Comments"].ToString(),
                    IsArchive = Convert.ToBoolean(row["IsArchive"]),
                    IsActive = Convert.ToBoolean(row["IsActive"]),
                    CreatedBy = row["CreatedBy"].ToString(),
                    CreatedOn = row["CreatedOn"].ToString(),
                    LastModifiedBy = row["LastModifiedBy"].ToString(),
                    LastModifiedOn = row["LastModifiedOn"].ToString(),
                    CreatedFrom = row["CreatedFrom"].ToString(),
                    LastUpdateFrom = row["LastUpdateFrom"].ToString(),
                    FYearStart = row["FYearStart"].ToString(),
                    FYearEnd = row["FYearEnd"].ToString(),
                    BusinessNature = row["BusinessNature"].ToString(),
                    AccountingNature = row["AccountingNature"].ToString(),
                    CompanyTypeId = Convert.ToInt32(row["CompanyTypeId"]),
                    Section = row["Section"].ToString(),
                    BIN = row["BIN"].ToString(),
                    IsVDSWithHolder = Convert.ToBoolean(row["IsVDSWithHolder"]),
                    AppVersion = row["AppVersion"].ToString(),
                    License = row["License"].ToString(),

                }).ToList();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = modelList;

                return result;

            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }


                string query = @"
SELECT 
    ISNULL(H.CompanyID, 0) AS Id,
    ISNULL(H.CompanyName, '') AS CompanyName,
    ISNULL(H.CompanyLegalName, '') AS CompanyLegalName,
    ISNULL(H.Address, '') AS Address,
    ISNULL(H.City, '') AS City,
    ISNULL(H.ZipCode, '') AS ZipCode,
    ISNULL(H.TelephoneNo, '') AS TelephoneNo,
    ISNULL(H.FaxNo, '') AS FaxNo,
    ISNULL(H.Email, '') AS Email,
    ISNULL(H.ContactPerson, '') AS ContactPerson,
    ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation,
    ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone,
    ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail,
    ISNULL(H.TINNo, '') AS TINNo,
    ISNULL(H.VatRegistrationNo, '') AS VatRegistrationNo,
    ISNULL(H.Comments, '') AS Comments,
    ISNULL(H.CreatedBy, '') AS CreatedBy,
    ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS CreatedOn,
    ISNULL(H.LastModifiedBy, '') AS LastModifiedBy,
    ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS LastModifiedOn,
    ISNULL(H.BIN, '') AS BIN,
    ISNULL(FORMAT(H.FYearStart, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS FYearStart,
    ISNULL(FORMAT(H.FYearEnd, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS FYearEnd

FROM 
    CompanyInfo AS H
WHERE 
    1 = 1

";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND H.CompanyID = @CompanyID ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@CompanyID", vm.Id);
                }

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new CompanyProfileVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    CompanyName = row["CompanyName"].ToString(),
                    CompanyLegalName = row["CompanyLegalName"].ToString(),
                    Address1 = row["Address"].ToString(),
                    City = row["City"].ToString(),
                    ZipCode = row["ZipCode"].ToString(),
                    TelephoneNo = row["TelephoneNo"].ToString(),
                    FaxNo = row["FaxNo"].ToString(),
                    Email = row["Email"].ToString(),
                    ContactPerson = row["ContactPerson"].ToString(),
                    ContactPersonDesignation = row["ContactPersonDesignation"].ToString(),
                    ContactPersonTelephone = row["ContactPersonTelephone"].ToString(),
                    ContactPersonEmail = row["ContactPersonEmail"].ToString(),
                    TINNo = row["TINNo"].ToString(),
                    VatRegistrationNo = row["VatRegistrationNo"].ToString(),
                    Comments = row["Comments"].ToString(),
                    CreatedBy = row["CreatedBy"].ToString(),
                    CreatedOn = row["CreatedOn"].ToString(),
                    LastModifiedBy = row["LastModifiedBy"].ToString(),
                    LastModifiedOn = row["LastModifiedOn"].ToString(),
                    BIN = row["BIN"].ToString(),
                    FYearStart = row["FYearStart"].ToString(),
                    FYearEnd = row["FYearEnd"].ToString(),

                }).ToList();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = modelList;

                return result;

            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
SELECT 
    ISNULL(H.Id, 0) AS Id,
    ISNULL(H.Code, '') AS Code,
    ISNULL(H.CompanyName, '') AS CompanyName,
    ISNULL(H.CompanyBanglaName, '') AS CompanyBanglaName,
    ISNULL(H.CompanyLegalName, '') AS CompanyLegalName,
    ISNULL(H.Address1, '') AS Address1,
    ISNULL(H.Address2, '') AS Address2,
    ISNULL(H.Address3, '') AS Address3,
    ISNULL(H.City, '') AS City,
    ISNULL(H.ZipCode, '') AS ZipCode,
    ISNULL(H.TelephoneNo, '') AS TelephoneNo,
    ISNULL(H.FaxNo, '') AS FaxNo,
    ISNULL(H.Email, '') AS Email,
    ISNULL(H.ContactPerson, '') AS ContactPerson,
    ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation,
    ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone,
    ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail,
    ISNULL(H.TINNo, '') AS TINNo,
    ISNULL(H.VatRegistrationNo, '') AS VatRegistrationNo,
    ISNULL(H.Comments, '') AS Comments,
    ISNULL(H.IsArchive, 0) AS IsArchive,
    ISNULL(H.IsActive, 0) AS IsActive,
    ISNULL(H.CreatedBy, '') AS CreatedBy,
    ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS CreatedOn,
    ISNULL(H.LastModifiedBy, '') AS LastModifiedBy,
    ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS LastModifiedOn,
    ISNULL(H.CreatedFrom, '') AS CreatedFrom,
    ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom,
    ISNULL(FORMAT(H.FYearStart, 'yyyy-MM-dd'), '1900-01-01') AS FYearStart,
    ISNULL(FORMAT(H.FYearEnd, 'yyyy-MM-dd'), '1900-01-01') AS FYearEnd,
    ISNULL(H.BusinessNature, '') AS BusinessNature,
    ISNULL(H.AccountingNature, '') AS AccountingNature,
    ISNULL(H.CompanyTypeId, 0) AS CompanyTypeId,
    ISNULL(H.Section, '') AS Section,
    ISNULL(H.BIN, '') AS BIN,
    ISNULL(H.IsVDSWithHolder, 0) AS IsVDSWithHolder,
    ISNULL(H.AppVersion, '') AS AppVersion,
    ISNULL(H.License, '') AS License

FROM CompanyProfiles AS H
WHERE 1 = 1

";

                DataTable dataTable = new DataTable();

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
                SELECT Id CompanyId, CompanyName
                FROM CompanyProfiles
                WHERE IsActive = 1
                ORDER BY CompanyName ";

                DataTable dropdownData = new DataTable();
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                    {
                        adapter.SelectCommand.Transaction = transaction;
                    }
                    adapter.Fill(dropdownData);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dropdownData;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<CompanyProfileVM>();

                // Define your SQL query string
                string sqlQuery = @"
            -- Count query
                    SELECT COUNT(DISTINCT H.Id) AS totalcount
                   FROM CompanyProfiles H 
                    WHERE H.IsArchive != 1
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<CompanyProfileVM>.FilterCondition(options.filter) + ")" : "") + @"

                    -- Data query with pagination and sorting
                    SELECT * 
                    FROM (
                        SELECT 
                         ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "H.Id DESC ") + @") AS rowindex,
                        ISNULL(H.Id, 0) AS Id,
                        ISNULL(H.Code, '') AS Code,
                        ISNULL(H.CompanyName, '') AS CompanyName,
                        ISNULL(H.CompanyBanglaName, '') AS CompanyBanglaName,
                        ISNULL(H.CompanyLegalName, '') AS CompanyLegalName,
                        ISNULL(H.Address1, '') AS Address1,
                        ISNULL(H.Address2, '') AS Address2,
                        ISNULL(H.Address3, '') AS Address3,
                        ISNULL(H.City, '') AS City,
                        ISNULL(H.ZipCode, '') AS ZipCode,
                        ISNULL(H.TelephoneNo, '') AS TelephoneNo,
                        ISNULL(H.FaxNo, '') AS FaxNo,
                        ISNULL(H.Email, '') AS Email,
                        ISNULL(H.ContactPerson, '') AS ContactPerson,
                        ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation,
                        ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone,
                        ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail,
                        ISNULL(H.TINNo, '') AS TINNo,
                        ISNULL(H.VatRegistrationNo, '') AS VatRegistrationNo,
                        ISNULL(H.Comments, '') AS Comments,
                        ISNULL(H.IsArchive, 0) AS IsArchive,
                        ISNULL(H.IsActive, 0) AS IsActive,
                        CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive' END AS Status,
                        ISNULL(H.CreatedBy, '') AS CreatedBy,
                        ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS CreatedOn,
                        ISNULL(H.LastModifiedBy, '') AS LastModifiedBy,
                        ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS LastModifiedOn,
                        ISNULL(H.CreatedFrom, '') AS CreatedFrom,
                        ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom,
                        ISNULL(FORMAT(H.FYearStart, 'yyyy-MM-dd'), '1900-01-01') AS FYearStart,
                        ISNULL(FORMAT(H.FYearEnd, 'yyyy-MM-dd'), '1900-01-01') AS FYearEnd,
                        ISNULL(H.BusinessNature, '') AS BusinessNature,
                        ISNULL(H.AccountingNature, '') AS AccountingNature,
                        ISNULL(H.CompanyTypeId, 0) AS CompanyTypeId,
                        ISNULL(H.Section, '') AS Section,
                        ISNULL(H.BIN, '') AS BIN,
                        ISNULL(H.IsVDSWithHolder, 0) AS IsVDSWithHolder,
                        ISNULL(H.AppVersion, '') AS AppVersion,
                        ISNULL(H.License, '') AS License
                    
                        FROM CompanyProfiles AS H

                        WHERE H.IsArchive != 1
                  
            -- Add the filter condition
            " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<CompanyProfileVM>.FilterCondition(options.filter) + ")" : "") + @"

            ) AS a
            WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
        ";

                data = KendoGrid<CompanyProfileVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = data;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }



        public async Task<ResultVM> AuthCompanyInsert(CompanyProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
                INSERT INTO CompanyProfiles 
                    (
                        Code, CompanyName, CompanyBanglaName, CompanyLegalName, Address1, Address2, Address3, City, ZipCode, 
                        TelephoneNo, FaxNo, Email, ContactPerson, ContactPersonDesignation, ContactPersonTelephone, ContactPersonEmail,TINNo, VatRegistrationNo, Comments, IsArchive, IsActive, CreatedBy, CreatedOn,CreatedFrom,FYearStart, FYearEnd, BusinessNature, AccountingNature, CompanyTypeId, Section, BIN, 
                        IsVDSWithHolder, AppVersion, License
                    )
                    VALUES 
                    (
                      @Code, @CompanyName, @CompanyBanglaName, @CompanyLegalName, @Address1, @Address2, @Address3, @City, @ZipCode,
                      @TelephoneNo, @FaxNo, @Email, @ContactPerson, @ContactPersonDesignation, @ContactPersonTelephone, @ContactPersonEmail, @TINNo, @VatRegistrationNo, @Comments, 
                        @IsArchive, @IsActive, @CreatedBy, GETDATE(), @CreatedFrom, @FYearStart, @FYearEnd, @BusinessNature, @AccountingNature, @CompanyTypeId, @Section, @BIN, @IsVDSWithHolder, @AppVersion, @License
                    );
                    SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyName", vm.CompanyName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyBanglaName", vm.CompanyBanglaName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyLegalName", vm.CompanyLegalName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address1", vm.Address1 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address2", vm.Address2 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address3", vm.Address3 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@City", vm.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ZipCode", vm.ZipCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TelephoneNo", vm.TelephoneNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FaxNo", vm.FaxNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Email", vm.Email ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPerson", vm.ContactPerson ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonDesignation", vm.ContactPersonDesignation ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonTelephone", vm.ContactPersonTelephone ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonEmail", vm.ContactPersonEmail ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TINNo", vm.TINNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@VatRegistrationNo", vm.VatRegistrationNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Comments", vm.Comments ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearStart", vm.FYearStart ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearEnd", vm.FYearEnd ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BusinessNature", vm.BusinessNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@AccountingNature", vm.AccountingNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyTypeId", vm.CompanyTypeId);
                    cmd.Parameters.AddWithValue("@Section", vm.Section ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BIN", vm.BIN ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsVDSWithHolder", vm.IsVDSWithHolder);
                    cmd.Parameters.AddWithValue("@AppVersion", vm.AppVersion ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@License", vm.License ?? (object)DBNull.Value);


                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
            throw new Exception("Database connection fail!");
        }

        // Update Method
        public async Task<ResultVM> AuthCompanyUpdate(CompanyProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
UPDATE CompanyProfiles
SET
    CompanyName = @CompanyName,
    CompanyBanglaName = @CompanyBanglaName,
    CompanyLegalName = @CompanyLegalName,
    Address1 = @Address1,
    Address2 = @Address2,
    Address3 = @Address3,
    City = @City,
    ZipCode = @ZipCode,
    TelephoneNo = @TelephoneNo,
    FaxNo = @FaxNo,
    Email = @Email,
    ContactPerson = @ContactPerson,
    ContactPersonDesignation = @ContactPersonDesignation,
    ContactPersonTelephone = @ContactPersonTelephone,
    ContactPersonEmail = @ContactPersonEmail,
    TINNo = @TINNo,
    VatRegistrationNo = @VatRegistrationNo,
    Comments = @Comments,
    LastModifiedOn = GETDATE(),
    FYearStart = @FYearStart,
    FYearEnd = @FYearEnd,
    BusinessNature = @BusinessNature,
    AccountingNature = @AccountingNature,
    CompanyTypeId = @CompanyTypeId,
    Section = @Section,
    BIN = @BIN,
    IsVDSWithHolder = @IsVDSWithHolder,
    LastModifiedBy = @LastModifiedBy,    
    LastUpdateFrom = @LastUpdateFrom,
    AppVersion = @AppVersion,
    License = @License

WHERE Id = @Id ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@CompanyName", vm.CompanyName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyBanglaName", vm.CompanyBanglaName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyLegalName", vm.CompanyLegalName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address1", vm.Address1 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address2", vm.Address2 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address3", vm.Address3 ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@City", vm.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ZipCode", vm.ZipCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TelephoneNo", vm.TelephoneNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FaxNo", vm.FaxNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Email", vm.Email ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPerson", vm.ContactPerson ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonDesignation", vm.ContactPersonDesignation ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonTelephone", vm.ContactPersonTelephone ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonEmail", vm.ContactPersonEmail ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TINNo", vm.TINNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@VatRegistrationNo", vm.VatRegistrationNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Comments", vm.Comments ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearStart", vm.FYearStart ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FYearEnd", vm.FYearEnd ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BusinessNature", vm.BusinessNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@AccountingNature", vm.AccountingNature ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CompanyTypeId", vm.CompanyTypeId);
                    cmd.Parameters.AddWithValue("@Section", vm.Section ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@BIN", vm.BIN ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsVDSWithHolder", vm.IsVDSWithHolder);
                    cmd.Parameters.AddWithValue("@AppVersion", vm.AppVersion ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@License", vm.License ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.LastModifiedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.UpdateSuccess;
                    }
                    else
                    {
                        result.Message = "No rows were updated.";
                    }
                }


                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Delete Method
        public async Task<ResultVM> AuthCompanyDelete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE CompanyProfiles SET IsArchive = 1, IsActive = 0,LastModifiedBy = @LastModifiedBy,LastUpdateFrom = @LastUpdateFrom ,LastModifiedOn =GETDATE() WHERE Id IN ({inClause})";
                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.ModifyFrom);

                    int rowsAffected = cmd.ExecuteNonQuery();


                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.DeleteSuccess;
                    }
                    else
                    {
                        throw new Exception("No rows were deleted.");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }


        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
                                SELECT 
                                    ISNULL(H.Id, 0) AS Id,
                                    ISNULL(H.Code, '') AS Code,
                                    ISNULL(H.CompanyName, '') AS CompanyName,
                                    ISNULL(H.CompanyBanglaName, '') AS CompanyBanglaName,
                                    ISNULL(H.CompanyLegalName, '') AS CompanyLegalName,
                                    ISNULL(H.Address1, '') AS Address1,
                                    ISNULL(H.Address2, '') AS Address2,
                                    ISNULL(H.Address3, '') AS Address3,
                                    ISNULL(H.City, '') AS City,
                                    ISNULL(H.ZipCode, '') AS ZipCode,
                                    ISNULL(H.TelephoneNo, '') AS TelephoneNo,
                                    ISNULL(H.FaxNo, '') AS FaxNo,
                                    ISNULL(H.Email, '') AS Email,
                                    ISNULL(H.ContactPerson, '') AS ContactPerson,
                                    ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation,
                                    ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone,
                                    ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail,
                                    ISNULL(H.TINNo, '') AS TINNo,
                                    ISNULL(H.VatRegistrationNo, '') AS VatRegistrationNo,
                                    ISNULL(H.Comments, '') AS Comments,
                                    ISNULL(H.IsArchive, 0) AS IsArchive,
                                    ISNULL(H.IsActive, 0) AS IsActive,
                                    ISNULL(H.CreatedBy, '') AS CreatedBy,
                                    ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS CreatedOn,
                                    ISNULL(H.LastModifiedBy, '') AS LastModifiedBy,
                                    ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm:ss'), '1900-01-01') AS LastModifiedOn,
                                    ISNULL(H.CreatedFrom, '') AS CreatedFrom,
                                    ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom,
                                    ISNULL(FORMAT(H.FYearStart, 'yyyy-MM-dd'), '1900-01-01') AS FYearStart,
                                    ISNULL(FORMAT(H.FYearEnd, 'yyyy-MM-dd'), '1900-01-01') AS FYearEnd,
                                    ISNULL(H.BusinessNature, '') AS BusinessNature,
                                    ISNULL(H.AccountingNature, '') AS AccountingNature,
                                    ISNULL(H.CompanyTypeId, 0) AS CompanyTypeId,
                                    ISNULL(H.Section, '') AS Section,
                                    ISNULL(H.BIN, '') AS BIN,
                                    ISNULL(H.IsVDSWithHolder, 0) AS IsVDSWithHolder,
                                    ISNULL(H.AppVersion, '') AS AppVersion,
                                    ISNULL(H.License, '') AS License

                                FROM 
                                    CompanyProfiles AS H
                                WHERE 
                                    1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND H.Id = @Id ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValue, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValue);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;
                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }



    }


}
