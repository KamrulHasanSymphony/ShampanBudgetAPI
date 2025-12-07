using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using Newtonsoft.Json;

namespace ShampanBFRS.Repository.SetUp
{
    public class StructureRepository : CommonRepository
    {
        public async Task<ResultVM> Insert(StructureVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"

                INSERT INTO Structures

                (Code, Name, Remarks, IsActive, IsArchive, CreatedBy, CreatedOn, CreatedFrom)

                VALUES

               (@Code, @Name, @Remarks, @IsActive, @IsArchive, @CreatedBy, GETDATE(), @CreatedFrom)

                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {

                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    object newId = await cmd.ExecuteScalarAsync();

                    vm.Id = Convert.ToInt32(newId);

                    result.Status = "Success";
                    result.Message = "Data Save Successfully";
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }

                //if (isNewConnection)
                //{
                //    transaction.Commit();
                //}

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.Message;
                result.Message = "Failed to insert requisition.";
                return result;
            }
            //finally
            //{
            //    if (isNewConnection && conn != null)
            //    {
            //        //conn.Close();
            //    }
            //}
        }

        public async Task<ResultVM> InsertDetails(StructureDetailsVM detail, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"

                INSERT INTO StructureDetails

                (StructureId, SegmentId, Remarks)

                VALUES

                (@StructureId, @SegmentId, @Remarks);

                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@StructureId", detail.StructureId);
                    cmd.Parameters.AddWithValue("@SegmentId", detail.SegmentId);
                    cmd.Parameters.AddWithValue("@Remarks", detail.Remarks ?? (object)DBNull.Value);
                    object newId = await cmd.ExecuteScalarAsync();

                    detail.Id = Convert.ToInt32(newId);

                    result.Status = "Success";
                    result.Message = "Detail inserted successfully.";
                    result.Id = detail.Id.ToString();
                    result.DataVM = detail;
                }

                if (isNewConnection)
                {
                    transaction.Commit();
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.Message;
                result.Message = "Failed to insert requisition detail.";
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

        public async Task<ResultVM> Update(StructureVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
                UPDATE Structures SET
                    Name = @Name,
                    Remarks = @Remarks,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateOn = GETDATE(),
                    LastUpdateFrom = @LastUpdateFrom
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@Name", vm.Name);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();

                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Updated successfully";
                    }
                    else
                    {
                        result.Message = "No rows were updated.";
                        throw new Exception("No rows were updated.");
                    }
                }

                if (isNewConnection)
                {
                    transaction.Commit();
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
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
        public async Task<ResultVM> Delete(string?[] IDs, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, IDs = IDs, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string inClause = string.Join(", ", IDs.Select((id, index) => $"@Id{index}"));

                string query = $"DELETE FROM Structures WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", IDs[i]);
                    }

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Data deleted successfully.";
                    }
                    else
                    {
                        result.Message = "No rows were deleted.";
                        throw new Exception("No rows were deleted.");
                    }
                }
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
            }

            return result;
        }
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
            SELECT

                    ISNULL(M.Id, 0) AS Id,
                    ISNULL(M.Code, '') AS Code,
                    ISNULL(M.Name, '') AS Name,
                    ISNULL(M.Remarks, '') AS Remarks,
                    ISNULL(M.IsActive, 0) AS IsActive,
                    ISNULL(M.IsArchive, 0) AS IsArchive,
                    ISNULL(M.CreatedBy, '') AS CreatedBy,
                    ISNULL(FORMAT(M.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreaCreatedOntedAt,
                    ISNULL(M.LastUpdateBy,'') AS LastUpdateBy,
                    ISNULL(FORMAT(M.LastUpdateOn,'yyyy-MM-dd HH:mm'),'') AS LastUpdateOn


              FROM Structures M

               WHERE 1 = 1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND M.Id = @Id ";
                }

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                var model = new List<StructureVM>();

                foreach (DataRow row in dataTable.Rows)
                {
                    model.Add(new StructureVM
                    {
                        Id = Convert.ToInt32(row["Id"]),
                        Code = row["Code"].ToString(),
                        Name = row["Name"].ToString(),
                        Remarks = row["Remarks"].ToString(),
                        IsActive = row.Field<bool>("IsActive"),
                        IsArchive = row.Field<bool>("IsArchive"),
                        CreatedBy = row.Field<string>("CreatedBy"),
                        LastUpdateBy = row.Field<string>("LastUpdateBy"),
                        LastUpdateAt = row.Field<string>("LastUpdateOn")

                    });
                }

                // Get the MaintenanceRequisition details

                var detailsDataList = DetailsList(new[] { "D.StructureId" }, conditionalValues, vm, conn, transaction);

                if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable dt)
                {

                    string json = JsonConvert.SerializeObject(dt);
                    var details = JsonConvert.DeserializeObject<List<StructureDetailsVM>>(json);
                    foreach (var req in model)
                    {
                        req.StructureDetails = details.Where(d => d.StructureId == req.Id).ToList();
                    }

                }

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = model;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = "Error in List.";
                return result;
            }
        }
        public ResultVM DetailsList(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
        SELECT 

             ISNULL(D.Id, 0) AS Id,
            ISNULL(D.StructureId, 0) AS StructureId,
            ISNULL(D.SegmentId, 0) AS SegmentId,
			ISNULL(SG.Name, '') AS SegmentName,
            ISNULL(D.Remarks, '') AS Remarks
            FROM StructureDetails D
			LEFT JOIN Segments SG ON D.SegmentId = SG.Id

            WHERE 1 = 1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.StructureId = @Id ";
                }

                query = ApplyConditions(query, conditionalFields, conditionalValue, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValue);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Details Data retrieved successfully.";
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
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }

                string query = "SELECT Id, Code AS Name FROM Structures ORDER BY Id DESC";

                DataTable dt = new DataTable();
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                    {
                        adapter.SelectCommand.Transaction = transaction;
                    }
                    adapter.Fill(dt);
                }

                result.Status = "Success";
                result.Message = "Dropdown data retrieved successfully.";
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
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
        public async Task<ResultVM> ListAsDataTable(string[] fields, string[] values, PeramModel vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };
            try
            {
                string query = "SELECT * FROM Structures WHERE 1=1";
                query = ApplyConditions(query, fields, values, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, fields, values);

                DataTable dt = new DataTable();
                adapter.Fill(dt);

                result.Status = "Success";
                result.Message = "Loaded successfully";
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }
        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $@"
UPDATE Structures
SET IsPost     = 1,
    PostedBy    = @PostedBy,
    PostedOn   = GETDATE(),
    PostedFrom = @PostedFrom
WHERE Id IN ({inClause});";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@PostedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@PostedFrom", vm.ModifyFrom);

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    if (rowsAffected > 0)
                    {
                        result.Status = "Success";
                        result.Message = "Posted successfully.";
                    }
                    else
                    {
                        throw new Exception("No rows were posted.");
                    }
                }

                if (isNewConnection)
                {
                    transaction.Commit();
                }

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }

                result.Message = ex.Message;
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



        // GetGridData Method
        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn, SqlTransaction transaction)
        {


            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }

                var data = new GridEntity<StructureVM>();

                // Define your SQL query string
                string sqlQuery = @"
                -- Count query
             SELECT COUNT(DISTINCT M.ID) AS totalcount
            FROM Structures M


            WHERE 1 = 1
                -- Add the filter condition
    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<StructureVM>.FilterCondition(options.filter) + ")" : ""); /*+ @"*/
                // Apply additional conditions
                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    -- Data query with pagination and sorting
    SELECT * 
    FROM (
        SELECT 
        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "M.Id DESC") + @") AS rowindex,
        
              ISNULL(M.Id, 0) AS Id,
             ISNULL(M.Code, '') AS Code,
             ISNULL(M.Name, '') AS Name,
             ISNULL(M.Remarks, '') AS Remarks
           FROM Structures M
  


            WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<StructureVM>.FilterCondition(options.filter) + ")" : ""); /*+ @"*/
                // Apply additional conditions
                //sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"

                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            ";

                //data = KendoGrid<SaleOrderVM>.GetGridData_CMD(options, sqlQuery, "H.Id");
                data = KendoGrid<StructureVM>.GetGridData_CMD(options, sqlQuery, "M.Id");

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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }

        }


        public async Task<ResultVM> GetStructureDetailDataById(GridOptions options, int masterId, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<StructureDetailsVM>();

                // Define your SQL query string
                string sqlQuery = @"
    -- Count query

            SELECT COUNT(DISTINCT D.Id) AS totalcount
             FROM StructureDetails D  
            LEFT OUTER JOIN Segments T ON D.StructureId = T.Id       

    -- Add the filter condition
    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<StructureDetailsVM>.FilterCondition(options.filter) + ")" : "") + @"

    -- Data query with pagination and sorting
    SELECT * 
    FROM (
        SELECT 
        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "D.ID DESC") + @") AS rowindex,
        
            ISNULL(D.Id, 0) AS Id,
            ISNULL(T.Name, '') AS SegmentName,
            ISNULL(T.Code, '') AS SegmentCode,
            ISNULL(D.StructureId, 0) AS StructureId,
            ISNULL(D.SegmentId, 0) AS SegmentId,
            ISNULL(D.Remarks, '') AS Remarks
            FROM StructureDetails D

            LEFT OUTER JOIN Segments T ON D.StructureId = T.Id
            WHERE D.StructureId = @masterId

    -- Add the filter condition
    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<StructureDetailsVM>.FilterCondition(options.filter) + ")" : "") + @"

    ) AS a
    WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
";
                sqlQuery = sqlQuery.Replace("@masterId", "" + masterId + "");

                data = KendoGrid<StructureDetailsVM>.GetGridData_CMD(options, sqlQuery, "H.ID");

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




        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }

                string query = @"
                    SELECT
                    MR.Id, MR.Code, MR.VehicleID, MR.DriverID, MR.RequisitionDate, MR.Remarks,
                    MR.IsPost, MR.PostedBy, MR.PostedAt, MR.PostedFrom,
                    MR.CreatedBy, MR.CreatedAt, MR.CreatedFrom,
                    MR.LastUpdateBy, MR.LastUpdateAt, MR.LastUpdateFrom,
                    MR.BranchId, MR.CompanyId,
                    MRD.Id AS DetailId, MRD.TaskID, MRD.Remarks AS DetailRemarks
                    FROM MM_MaintenanceRequisitions MR
                    LEFT JOIN MM_MaintenanceRequisitionDetails MRD ON MR.Id = MRD.MaintenanceRequisitionID
                    WHERE 1 = 1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND MR.Id = @Id";
                }

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);
                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }
                adapter.Fill(dt);

                result.Status = "Success";
                result.Message = "Report data retrieved successfully.";
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }
    }
}
