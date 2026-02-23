using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class BranchProfileRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(BranchProfileVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
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
         INSERT INTO BranchProfiles 
                     (Code, Name, LegalName, Address, ActiveStatus, IsArchive, City, ZipCode, TelephoneNo, FaxNo, Email, ContactPerson, ContactPersonDesignation, ContactPersonTelephone, ContactPersonEmail, Comments, CreatedBy, CreatedOn)
                     VALUES 
                     (@Code, @Name, @LegalName, @Address, @ActiveStatus, @IsArchive, @City, @ZipCode, @TelephoneNo, @FaxNo, @Email, @ContactPerson, @ContactPersonDesignation, @ContactPersonTelephone, @ContactPersonEmail, @Comments, @CreatedBy, GETDATE());
                     SELECT SCOPE_IDENTITY();";



                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {

                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name);
                    cmd.Parameters.AddWithValue("@LegalName", vm.LegalName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address", vm.Address ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ActiveStatus", vm.ActiveStatus);
                    cmd.Parameters.AddWithValue("@City", vm.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ZipCode", vm.ZipCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TelephoneNo", vm.TelephoneNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FaxNo", vm.FaxNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Email", vm.Email ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPerson", vm.ContactPerson ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonDesignation", vm.ContactPersonDesignation ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonTelephone", vm.ContactPersonTelephone ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonEmail", vm.ContactPersonEmail ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@VATRegistrationNo", vm.VATRegistrationNo ?? (object)DBNull.Value);
                   // cmd.Parameters.AddWithValue("@BIN", vm.BIN ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@TINNO", vm.TINNO ?? (object)DBNull.Value);
                
                    cmd.Parameters.AddWithValue("@Comments", vm.Comments ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                   
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy);

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
                result.Status = MessageModel.Fail;
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Update Method
        public async Task<ResultVM> Update(BranchProfileVM branchProfile, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = branchProfile.Id.ToString(), DataVM = branchProfile };

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
                UPDATE BranchProfiles 
                SET 
                    Name = @Name,LegalName=@LegalName,Address=@Address,City=@City,ZipCode=@ZipCode,TelephoneNo = @TelephoneNo,FaxNo=@FaxNo,Email = @Email,ContactPerson=@ContactPerson, ContactPersonDesignation = @ContactPersonDesignation, ContactPersonTelephone = @ContactPersonTelephone,ContactPersonEmail = @ContactPersonEmail, Comments = @Comments,ActiveStatus = @ActiveStatus, 
                    LastModifiedBy = @LastModifiedBy, LastModifiedOn = GETDATE()
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", branchProfile.Id);
                    cmd.Parameters.AddWithValue("@Name", branchProfile.Name);
                    cmd.Parameters.AddWithValue("@LegalName", branchProfile.LegalName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Address", branchProfile.Address ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@City", branchProfile.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ZipCode", branchProfile.ZipCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TelephoneNo", branchProfile.TelephoneNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@FaxNo", branchProfile.FaxNo ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Email", branchProfile.Email ?? (object)DBNull.Value);

                    //cmd.Parameters.AddWithValue("@VATRegistrationNo", branchProfile.VATRegistrationNo ?? (object)DBNull.Value);
                    // cmd.Parameters.AddWithValue("@BIN", branchProfile.BIN ?? (object)DBNull.Value);
                    // cmd.Parameters.AddWithValue("@TINNO", branchProfile.TINNO ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPerson", branchProfile.ContactPerson ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonDesignation", branchProfile.ContactPersonDesignation ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonTelephone", branchProfile.ContactPersonTelephone ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ContactPersonEmail", branchProfile.ContactPersonEmail ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Comments", branchProfile.Comments ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ActiveStatus", branchProfile.ActiveStatus);
                    cmd.Parameters.AddWithValue("@LastModifiedBy", branchProfile.LastModifiedBy ?? (object)DBNull.Value);

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

                string query = $"UPDATE BranchProfiles SET IsArchive = 1, IsActive = 0,LastModifiedBy = @LastModifiedBy,LastUpdateFrom = @LastUpdateFrom ,LastModifiedOn =GETDATE() WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    //string ids = string.Join(",", IDs);
                    //cmd.Parameters.AddWithValue("@Ids", ids);
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
    ISNULL(H.Id, 0) AS Id
";
                if (vm != null && !string.IsNullOrEmpty(vm.UserLogInId))
                {
                    query += @"
    ,@UserId AS UserId";
                }
                else
                {
                    query += @"
    ,'' AS UserId";
                }

                query += @"
    
     ,ISNULL(H.Code, '') AS Code
    ,ISNULL(H.Name, '') AS Name
	,ISNULL(H.LegalName, '') AS LegalName
	,ISNULL(H.Address, '') AS Address
	,ISNULL(H.City, '') AS City
	,ISNULL(H.ZipCode, '') AS ZipCode
    ,ISNULL(H.TelephoneNo, '') AS TelephoneNo 
	,ISNULL(H.FaxNo, '') AS FaxNo	
    ,ISNULL(H.Email, '') AS Email
	,ISNULL(H.ContactPerson, '') AS ContactPerson
	,ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation
	,ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone
	,ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail
    ,ISNULL(H.Comments,'') Comments
    ,ISNULL(H.IsArchive,0)	IsArchive
    ,ISNULL(H.ActiveStatus,0) ActiveStatus
    --,CASE WHEN ISNULL(H.ActiveStatus,0) = 1 THEN 'Active' ELSE 'Inactive'	END Status
    ,ISNULL(H.CreatedBy,'') CreatedBy
    ,ISNULL(H.LastModifiedBy,'') LastModifiedBy
    ,ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm'),'1900-01-01') CreatedOn
FROM 
    BranchProfiles H
	
	WHERE 1 = 1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND Id = @Id ";
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
                if (vm != null && !string.IsNullOrEmpty(vm.UserLogInId))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@UserId", vm.UserLogInId);
                }
                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new BranchProfileVM
                {

                    Id = Convert.ToInt32(row["Id"]),
                    UserId = row["UserId"].ToString(),

                    Code = row["Code"].ToString(),
                    Name = row["Name"].ToString(),
                    LegalName = row["LegalName"].ToString(),
                    Address = row["Address"].ToString(),
                    City = row["City"].ToString(),
                    ZipCode = row["ZipCode"].ToString(),
                    TelephoneNo = row["TelephoneNo"].ToString(),
                    FaxNo = row["FaxNo"].ToString(),
                    Email = row["Email"].ToString(),
                    ContactPerson = row["ContactPerson"].ToString(),
                    ContactPersonDesignation = row["ContactPersonDesignation"].ToString(),
                    ContactPersonTelephone = row["ContactPersonTelephone"].ToString(),
                    ContactPersonEmail = row["ContactPersonEmail"].ToString(),
                    Comments = row["Comments"].ToString(),
                    IsArchive = Convert.ToBoolean(row["IsArchive"]),
                    ActiveStatus = Convert.ToBoolean(row["ActiveStatus"]),
                    CreatedBy = row["CreatedBy"].ToString(),
                    LastModifiedBy = row["LastModifiedBy"].ToString(),
                    CreatedOn = row["CreatedOn"].ToString(),
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
     ISNULL(H.Code, '') AS Code
    ,ISNULL(H.Name, '') AS Name
    ,ISNULL(H.TelephoneNo, '') AS TelephoneNo
    ,ISNULL(H.Address, '') AS Address
    ,ISNULL(H.Email, '') AS Email
    ,ISNULL(H.VATRegistrationNo, '') AS VATRegistrationNo
    ,ISNULL(H.BIN, '') AS BIN
    ,ISNULL(H.TINNO, '') AS TINNO
    ,ISNULL(H.Comments, '') AS Comments
    ,ISNULL(H.IsArchive, 0) AS IsArchive
    ,ISNULL(H.IsActive, 0) AS IsActive
    ,ISNULL(H.CreatedBy, '') AS CreatedBy
    ,ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') AS CreatedOn
    ,ISNULL(H.LastModifiedBy, '') AS LastModifiedBy
    ,ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') AS LastModifiedOn
    ,ISNULL(H.CreatedFrom, '') AS CreatedFrom
    ,ISNULL(H.LastUpdateFrom, '') AS LastUpdateFrom
FROM 
    BranchProfiles H
	
	WHERE 1 = 1 ";

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
                SELECT Id, Name
                FROM BranchAdvances
                WHERE IsActive = 1
                ORDER BY Name";

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

                var data = new GridEntity<BranchProfileVM>();

                // Define your SQL query string
                string sqlQuery = @"
            -- Count query
                    SELECT COUNT(DISTINCT H.Id) AS totalcount
                   FROM BranchProfiles H 
                   WHERE H.IsArchive != 1
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BranchProfileVM>.FilterCondition(options.filter) + ")" : "") + @"

                    -- Data query with pagination and sorting
                    SELECT * 
                    FROM (
                        SELECT 
                         ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "H.Id DESC ") + @") AS rowindex
   
                        ,ISNULL(H.Id, 0) AS Id
                        ,ISNULL(H.Code, '') AS Code
                        ,ISNULL(H.Name, '') AS Name
	                    ,ISNULL(H.LegalName, '') AS LegalName
	                    ,ISNULL(H.Address, '') AS Address
	                    ,ISNULL(H.City, '') AS City
	                    ,ISNULL(H.ZipCode, '') AS ZipCode
                        ,ISNULL(H.TelephoneNo, '') AS TelephoneNo 
	                    ,ISNULL(H.FaxNo, '') AS FaxNo	
                        ,ISNULL(H.Email, '') AS Email
	                    ,ISNULL(H.ContactPerson, '') AS ContactPerson
	                    ,ISNULL(H.ContactPersonDesignation, '') AS ContactPersonDesignation
	                    ,ISNULL(H.ContactPersonTelephone, '') AS ContactPersonTelephone
	                    ,ISNULL(H.ContactPersonEmail, '') AS ContactPersonEmail
                        ,ISNULL(H.Comments,'') Comments
                        ,ISNULL(H.IsArchive,0)	IsArchive
                        ,ISNULL(H.ActiveStatus,0)	ActiveStatus
                        ,CASE WHEN ISNULL(H.ActiveStatus,0) = 1 THEN 'Active' ELSE 'Inactive'	END Status
                        ,ISNULL(H.CreatedBy,'') CreatedBy
                        ,ISNULL(H.LastModifiedBy,'') LastModifiedBy
                        ,ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm'),'1900-01-01') CreatedOn
				        

                    FROM BranchProfiles H
                    WHERE H.IsArchive != 1
                  
            -- Add the filter condition
            " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<BranchProfileVM>.FilterCondition(options.filter) + ")" : "") + @"

            ) AS a
            WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
        ";

                data = KendoGrid<BranchProfileVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

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

                        ISNULL(H.Id, 0) AS Id
                        ,ISNULL(H.Code, '') AS Code
                        ,ISNULL(H.Name, '') AS Name
				        ,ISNULL(H.Email,'') Email
                        ,ISNULL(H.TelephoneNo, '') AS TelephoneNo                        
                        ,ISNULL(H.Address, '') AS Address
				        ,ISNULL(H.VATRegistrationNo,'''') VATRegistrationNo
				        ,ISNULL(H.BIN,'') BIN
                        ,ISNULL(H.TINNO, '') AS TINNO
				        ,ISNULL(H.CreatedBy,'') CreatedBy
				        ,ISNULL(H.LastModifiedBy,'') LastModifiedBy
				        ,ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm'),'1900-01-01') CreatedOn
				        ,ISNULL(FORMAT(H.LastModifiedOn,'yyyy-MM-dd HH:mm'),'1900-01-01') LastModifiedOn

                       FROM BranchProfiles H
                       WHERE  1 = 1 ";

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
