using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class SettingsRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(SettingVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
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

INSERT INTO Settings 
(
 SettingGroup
,SettingName
,SettingValue
,SettingType
,Remarks
,IsActive
,IsArchive
,CreatedBy
,CreatedOn
,CreatedFrom
)
VALUES 
(
 @SettingGroup
,@SettingName
,@SettingValue
,@SettingType
,@Remarks
,@IsActive
,@IsArchive
,@CreatedBy
,GETDATE()
,@CreatedFrom

);
 SELECT SCOPE_IDENTITY(); ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@SettingGroup", vm.SettingGroup ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@SettingName", vm.SettingName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@SettingValue", vm.SettingValue ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@SettingType", vm.SettingType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message =MessageModel.InsertSuccess;
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }


                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Update Method
        public async Task<ResultVM> Update(SettingVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
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
 UPDATE Settings 
 SET 
 SettingValue=@SettingValue
,Remarks=@Remarks
,LastModifiedBy=@LastModifiedBy
,LastUpdateFrom=@LastUpdateFrom
,LastModifiedOn=GETDATE()
 WHERE Id = @Id ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@SettingValue", vm.SettingValue ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.LastModifiedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.RetrievedSuccess;
                    }
                    else
                    {
                        throw new Exception(result.Message);
                    }
                }


                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Delete Method
        public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, IDs = vm.IDs, DataVM = null };

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

                string query = $" UPDATE Settings SET IsArchive = 1, IsActive = 0,LastModifiedBy = @LastModifiedBy,LastUpdateFrom = @LastUpdateFrom ,LastModifiedOn =GETDATE() WHERE Id IN ({inClause})";

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
               
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // List Method
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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

SELECT DISTINCT
 ISNULL(M.Id,0)	Id
,ISNULL(M.SettingGroup,'')	SettingGroup
,ISNULL(M.SettingName,'')	SettingName
,ISNULL(M.SettingType,'')	SettingType
,ISNULL(M.SettingValue,'')	SettingValue
,ISNULL(M.Remarks,'')	Remarks
,ISNULL(M.IsArchive,0)	IsArchive
,ISNULL(M.IsActive,0)	IsActive
,ISNULL(M.CreatedBy,'')	CreatedBy
,ISNULL(M.LastModifiedBy,'')	LastModifiedBy
,ISNULL(FORMAT(M.CreatedOn,'yyyy-MM-dd HH:mm'),'1900-01-01')	CreatedOn
,ISNULL(FORMAT(M.LastModifiedOn,'yyyy-MM-dd HH:mm'),'1900-01-01')	LastModifiedOn

FROM Settings M 
WHERE 1=1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND M.Id = @Id ";
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

                var model = new List<SettingVM>();
                foreach (DataRow row in dataTable.Rows)
                {
                    model.Add(new SettingVM
                    {
                        Id = Convert.ToInt32(row["Id"]),

                        SettingGroup = row["SettingGroup"].ToString(),
                        SettingName = row["SettingName"].ToString(),
                        SettingType = row["SettingType"].ToString(),
                        SettingValue = row["SettingValue"].ToString(),
                        Remarks = row["Remarks"].ToString(),
                        CreatedBy = row["CreatedBy"].ToString(),
                        LastModifiedBy = row["LastModifiedBy"].ToString(),
                        IsArchive = Convert.ToBoolean(row["IsArchive"]),
                        IsActive = Convert.ToBoolean(row["IsActive"]),
                        CreatedOn = row["CreatedOn"].ToString(),
                        LastModifiedOn = row["LastModifiedOn"].ToString(),
                    });
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = model;

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"

SELECT DISTINCT
 ISNULL(M.Id,0)	Id
,ISNULL(M.SettingGroup,'')	SettingGroup
,ISNULL(M.SettingName,'')	SettingName
,ISNULL(M.SettingType,'')	SettingType
,ISNULL(M.SettingValue,'')	SettingValue
,ISNULL(M.Remarks,'')	Remarks
,ISNULL(M.IsArchive,0)	IsArchive
,ISNULL(M.IsActive,0)	IsActive
,ISNULL(M.CreatedBy,'')	CreatedBy
,ISNULL(M.LastModifiedBy,'')	LastModifiedBy
,ISNULL(FORMAT(M.CreatedOn,'yyyy-MM-dd HH:mm'),'1900-01-01')	CreatedOn
,ISNULL(FORMAT(M.LastModifiedOn,'yyyy-MM-dd HH:mm'),'1900-01-01')	LastModifiedOn

FROM Settings M 
WHERE 1=1 ";

                DataTable dataTable = new DataTable();

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValue, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValue);

                objComm.Fill(dataTable);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;

                return result;
            }
            catch (Exception ex)
            {
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
                SELECT Id, SettingValue Name
                FROM Settings
                WHERE IsActive = 1
                ORDER BY Id";

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
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }


        // CodesDataInsert Method
        public async Task<ResultVM> CodesDataInsert(CommonVM vm, string CodeGroup, string CodeName, string prefix, string Lenth, string ActiveStatus, SqlConnection conn = null, SqlTransaction transaction = null)
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

                string selectQuery = @" SELECT COUNT(CodeId) FROM Codes WHERE CodeGroup = @CodeGroup AND CodeName = @CodeName AND prefix = @prefix ";
                var checkCommand = CreateCommand(selectQuery, conn, transaction);
                checkCommand.Parameters.Add("@CodeGroup", SqlDbType.NChar).Value = CodeGroup.Trim();
                checkCommand.Parameters.Add("@CodeName", SqlDbType.NChar).Value = CodeName.Trim();
                checkCommand.Parameters.Add("@prefix", SqlDbType.NChar).Value = prefix.Trim();

                int count = Convert.ToInt32(checkCommand.ExecuteScalar());
                if (count > 0)
                {
                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.AlreadyExists;
                    return result;
                }

                string query = @"

INSERT INTO Codes 
(
 CodeGroup
,CodeName
,prefix
,Lenth
,ActiveStatus
,LastId
,CreatedBy
,CreatedOn
,CreatedFrom
,LastModifiedBy
,LastModifiedOn
,LastUpdateFrom
)
VALUES 
(
 @CodeGroup
,@CodeName
,@prefix
,@Lenth
,@ActiveStatus
,@LastId
,@CreatedBy
,GETDATE()
,@CreatedFrom
,@CreatedBy
,GETDATE()
,@CreatedFrom
);
 SELECT SCOPE_IDENTITY(); ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@CodeGroup", CodeGroup ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CodeName", CodeName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@prefix", prefix ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Lenth", Lenth ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ActiveStatus", ActiveStatus ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastId", 0);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.ModifyBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.ModifyFrom ?? (object)DBNull.Value);

                    var newId = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.UpdateSuccess;
                    result.Id = newId.ToString();
                    result.DataVM = vm;
                }


                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // SettingsDataInsert Method
        public async Task<ResultVM> SettingsDataInsert(CommonVM vm,string SettingGroup, string SettingName, string SettingValue, string SettingType, bool IsActive, SqlConnection conn = null, SqlTransaction transaction = null)
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

                string selectQuery = @" SELECT COUNT(Id) FROM Settings WHERE SettingGroup = @SettingGroup AND SettingName = @SettingName ";
                var checkCommand = CreateCommand(selectQuery, conn, transaction);
                checkCommand.Parameters.Add("@SettingGroup", SqlDbType.NChar).Value = SettingGroup.Trim();
                checkCommand.Parameters.Add("@SettingName", SqlDbType.NChar).Value = SettingName.Trim();

                int count = Convert.ToInt32(checkCommand.ExecuteScalar());
                if (count > 0)
                {
                    result.Status = MessageModel.Success;
                    result.Message =MessageModel.AlreadyExists;
                    return result;
                }

                string query = @"

INSERT INTO Settings 
(
 SettingGroup
,SettingName
,SettingValue
,SettingType
,Remarks
,IsActive
,IsArchive
,CreatedBy
,CreatedOn
,CreatedFrom
,LastModifiedBy
,LastUpdateFrom
,LastModifiedOn
)
VALUES 
(
 @SettingGroup
,@SettingName
,@SettingValue
,@SettingType
,@Remarks
,@IsActive
,@IsArchive
,@CreatedBy
,GETDATE()
,@CreatedFrom
,@CreatedBy
,@CreatedFrom
,GETDATE()
);
 SELECT SCOPE_IDENTITY(); ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@SettingGroup", SettingGroup ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@SettingName", SettingName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@SettingValue", SettingValue ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@SettingType", SettingType ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", "-");
                    cmd.Parameters.AddWithValue("@IsArchive", false);
                    cmd.Parameters.AddWithValue("@IsActive", IsActive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.ModifyBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.ModifyFrom ?? (object)DBNull.Value);

                    var newId = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.UpdateSuccess;
                    result.Id = newId.ToString();
                    result.DataVM = vm;
                }


                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // NewTableAdd Method
        public async Task<ResultVM> NewTableAdd(string tableName, string query, SqlConnection conn = null, SqlTransaction transaction = null)
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

                string sqlText = "";
                int count = 0;
                sqlText = "";

                sqlText += " IF  NOT EXISTS (SELECT * FROM sys.objects ";
                sqlText += " WHERE object_id = OBJECT_ID(N'" + tableName + "') AND type in (N'U'))";

                sqlText += " BEGIN";
                sqlText += " " + query;
                sqlText += " END";

                SqlCommand command = CreateCommand(sqlText, conn, transaction);

                command.ExecuteNonQuery();               

                result.Status = MessageModel.Success;
                result.Message = MessageModel.UpdateSuccess ;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;                
            }

            return result;
        }

        // DBTableFieldAdd Method
        public async Task<ResultVM> DBTableFieldAdd(string tableName, string fieldName, string dataType, bool allowNull, SqlConnection conn = null, SqlTransaction transaction = null)
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

                string sqlText = "";
                sqlText = "";
                sqlText += " IF  NOT EXISTS (SELECT * FROM sys.columns ";
                sqlText += " WHERE Name = N'" + fieldName + "' and Object_ID = Object_ID(N'" + tableName + "'))   ";
                sqlText += " BEGIN ";
                if (allowNull == true)
                {
                    sqlText += " ALTER TABLE " + tableName + " ADD " + fieldName + " " + dataType + " NULL DEFAULT 0 ;";
                }
                else
                {
                    sqlText += " ALTER TABLE " + tableName + " ADD " + fieldName + " " + dataType + " NOT NULL DEFAULT 0 ;";
                }
                sqlText += " END ";

                SqlCommand command = CreateCommand(sqlText, conn, transaction);

                command.ExecuteNonQuery();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.UpdateSuccess;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
            }

            return result;
        }

        // DBTableFieldAlter Method
        public async Task<ResultVM> DBTableFieldAlter(string tableName, string fieldName, string dataType, SqlConnection conn = null, SqlTransaction transaction = null)
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

                string sqlText = "";
                sqlText = "";
                sqlText += " ALTER TABLE " + tableName + " ALTER COLUMN " + fieldName + "   " + dataType + "";

                SqlCommand command = CreateCommand(sqlText, conn, transaction);

                command.ExecuteNonQuery();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.UpdateSuccess;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
            }

            return result;
        }



    }
}
