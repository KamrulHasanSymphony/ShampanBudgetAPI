using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{  
    public class MenuAuthorizationRepository : CommonRepository
    {
        public async Task<ResultVM> Insert(UserRoleVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

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


                string query = @" INSERT INTO [dbo].[Role] (



 Name
,CreatedBy
,CreatedOn
,CreatedFrom
) VALUES (

 @Name
,@CreatedBy
,GETDATE()
,@CreatedFrom

); 
SELECT SCOPE_IDENTITY()";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = "Success";
                    result.Message = "Data inserted successfully.";
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in Insert.";
                return result;
            }
        }

        public async Task<ResultVM> Update(UserRoleVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

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

                string query = @"  UPDATE [dbo].[Role] SET  

 Name=@Name
,LastModifiedBy=@LastModifiedBy
,LastModifiedOn=GETDATE()
,LastUpdateFrom=@LastUpdateFrom
                       
WHERE  Id = @Id ";
                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.LastModifiedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Data updated successfully.";
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
                
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetRoleIndexData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<UserRoleVM>();

                // Define your SQL query string
                string sqlQuery = @"
                                 -- Count query
                  SELECT COUNT(DISTINCT H.Id) AS totalcount
					FROM Role H
					WHERE 1 = 1

                  -- Add the filter condition
                  " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<UserRoleVM>.FilterCondition(options.filter) + ")" : "") + @"

                  -- Data query with pagination and sorting
                   SELECT * 
                  FROM (
                      SELECT 
                     ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "H.Id DESC ") + @") AS rowindex
			    ,ISNULL(H.Id,0)	Id
				,ISNULL(H.Name,'') Name
				,ISNULL(H.CreatedBy,'') CreatedBy
				,ISNULL(H.LastModifiedBy,'') LastModifiedBy
				,ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm'),'1900-01-01') CreatedOn
				,ISNULL(FORMAT(H.LastModifiedOn,'yyyy-MM-dd HH:mm'),'1900-01-01') LastModifiedOn

				FROM Role H 
				WHERE 1 = 1

                  -- Add the filter condition
                  " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<UserRoleVM>.FilterCondition(options.filter) + ")" : "") + @"

                  ) AS a
                  WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            ";

                data = KendoGrid<UserRoleVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = data;

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> RoleMenuDelete(RoleMenuVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

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

                string query = @"DELETE FROM RoleMenu WHERE RoleId = @RolesId";
                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.Add("@RolesId", SqlDbType.NChar).Value = vm.RoleId;
                    int res = cmd.ExecuteNonQuery();

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Data updated successfully.";
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
                throw ex;
            }

        }

        public async Task<ResultVM> RoleMenuInsert(RoleMenuVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

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


                string query = @" INSERT INTO RoleMenu (



 RoleId
,MenuId
,List
,[Insert]
,[Delete]
,Post
,CreatedBy
,CreatedOn
,CreatedFrom
,LastModifiedBy
,LastModifiedOn
,LastUpdateFrom

) VALUES (

 @RoleId
,@MenuId
,@List
,@Insert
,@Delete
,@Post
,@CreatedBy
,@CreatedOn
,@CreatedFrom
,@LastModifiedBy
,@LastModifiedOn
,@LastUpdateFrom



); 
SELECT SCOPE_IDENTITY()";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@RoleId", SqlDbType.NChar).Value = vm.RoleId;
                    cmd.Parameters.AddWithValue("@MenuId", SqlDbType.NChar).Value = vm.MenuId;
                    cmd.Parameters.AddWithValue("@List", SqlDbType.NChar).Value = vm.List;
                    cmd.Parameters.AddWithValue("@Insert", SqlDbType.NChar).Value = vm.Insert;
                    cmd.Parameters.AddWithValue("@Delete", SqlDbType.NChar).Value = vm.Delete;
                    cmd.Parameters.AddWithValue("@Post", SqlDbType.NChar).Value = vm.Post;


                    cmd.Parameters.AddWithValue("@CreatedBy", SqlDbType.NChar).Value = string.IsNullOrEmpty(vm.CreatedBy) ? DBNull.Value : vm.CreatedBy.Trim();
                    cmd.Parameters.AddWithValue("@CreatedOn", SqlDbType.NChar).Value = string.IsNullOrEmpty(vm.CreatedOn.ToString()) ? DBNull.Value : vm.CreatedOn.ToString();
                    cmd.Parameters.AddWithValue("@CreatedFrom", SqlDbType.NChar).Value = string.IsNullOrEmpty(vm.CreatedFrom) ? DBNull.Value : vm.CreatedFrom.Trim();

                    cmd.Parameters.AddWithValue("@LastModifiedBy", SqlDbType.NChar).Value = string.IsNullOrEmpty(vm.LastModifiedBy) ? DBNull.Value : vm.LastModifiedBy.Trim();
                    cmd.Parameters.AddWithValue("@LastModifiedOn", SqlDbType.VarChar).Value = string.IsNullOrEmpty(vm.LastModifiedOn) ? DBNull.Value : vm.LastModifiedOn;
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", SqlDbType.NChar).Value = string.IsNullOrEmpty(vm.LastUpdateFrom) ? DBNull.Value : vm.LastUpdateFrom.Trim();

                

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = "Success";
                    result.Message = "Data inserted successfully.";
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }


                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in Insert.";
                return result;
            }
        }


    }

}
