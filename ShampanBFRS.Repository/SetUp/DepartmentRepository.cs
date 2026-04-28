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
    public class DepartmentRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(DepartmentVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                INSERT INTO Departments
                (
                   Code, Name,DepartmentName,Description,StartDate,EndDate,DepartmentType,ApprovalStatus,FundingSource,OwnFund,OwnFundAmt,GovernmentGrant,GovernmentAmt,GovernmentLoan,GovernmentLoanAmt,ForeignGrant,ForeignGrantAmt,ForeignLoan,ForeignLoanAmt,ShareCapital,ShareCapitalAmt,
            others,ProjectName,TotalValue,Reference,Remarks, IsActive, IsArchive, CreatedBy, CreatedFrom,CreatedOn
                )
                VALUES
                (
                   @Code, @Name,@DepartmentName, @Description,@StartDate,@EndDate,@DepartmentType,@ApprovalStatus,@FundingSource,@OwnFund, @OwnFundAmt,
            @GovernmentGrant, @GovernmentAmt,@GovernmentLoan, @GovernmentLoanAmt,@ForeignGrant, @ForeignGrantAmt,@ForeignLoan, @ForeignLoanAmt,@ShareCapital, @ShareCapitalAmt,@Others,@ProjectName,
            @TotalValue,@Reference,@Remarks, @IsActive, @IsArchive, @CreatedBy, @CreatedFrom,GETDate()
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@DepartmentName", (object?)vm.DepartmentName ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@Description", vm.Description ?? (object)DBNull.Value);

                    cmd.Parameters.AddWithValue("@StartDate", (object?)vm.StartDate ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@EndDate", (object?)vm.EndDate ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@DepartmentType", (object?)vm.DepartmentType ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ApprovalStatus", (object?)vm.ApprovalStatus ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@FundingSource", (object?)vm.FundingSource ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@OwnFund", (object?)vm.OwnFund ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@OwnFundAmt", (object?)vm.OwnFundAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@GovernmentGrant", (object?)vm.GovernmentGrant ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@GovernmentAmt", (object?)vm.GovernmentAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@GovernmentLoan", (object?)vm.GovernmentLoan ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@GovernmentLoanAmt", (object?)vm.GovernmentLoanAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@ForeignGrant", (object?)vm.ForeignGrant ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ForeignGrantAmt", (object?)vm.ForeignGrantAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@ForeignLoan", (object?)vm.ForeignLoan ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ForeignLoanAmt", (object?)vm.ForeignLoanAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@ShareCapital", (object?)vm.ShareCapital ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ShareCapitalAmt", (object?)vm.ShareCapitalAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@Others", (object?)vm.Others ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ProjectName", (object?)vm.ProjectName ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@TotalValue", (object?)vm.TotalValue ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@Reference", vm.Reference ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    vm.Id =Convert.ToInt32(cmd.ExecuteScalar());

                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.InsertSuccess;
                result.Id = vm.Id.ToString();
                result.DataVM = vm;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // Update Method
        //public async Task<ResultVM> Update(DepartmentVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        //{
        //    ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", Id = vm.Id.ToString(), DataVM = vm };

        //    try
        //    {
        //        if (conn == null) throw new Exception("Database connection failed!");
        //        if (transaction == null) transaction = conn.BeginTransaction();

        //        string query = @"
        //        UPDATE Departments
        //        SET 
        //            Code = @Code,
        //            Name = @Name,
        //            Description = @Description,
        //            Reference = @Reference,
        //            Remarks = @Remarks,
        //            IsActive = @IsActive,
        //            LastUpdateBy = @LastUpdateBy,
        //            LastUpdateFrom = @LastUpdateFrom,
        //            LastUpdateOn = GETDATE()
        //        WHERE Id = @Id";

        //        using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
        //        {
        //            cmd.Parameters.AddWithValue("@Id", vm.Id);
        //            cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
        //            cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
        //            cmd.Parameters.AddWithValue("@Description", vm.Description ?? (object)DBNull.Value);
        //            cmd.Parameters.AddWithValue("@Reference", vm.Reference ?? (object)DBNull.Value);
        //            cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);
        //            cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
        //            cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
        //            cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

        //            int rows = cmd.ExecuteNonQuery();
        //            if (rows > 0)
        //            {
        //                result.Status = MessageModel.Success;
        //                result.Message = MessageModel.UpdateSuccess;
        //            }
        //            else
        //            {
        //                throw new Exception("No rows were updated.");
        //            }
        //        }

        //        return result;
        //    }
        //    catch (Exception ex)
        //    {
        //        result.Message = ex.Message;
        //        result.ExMessage = ex.ToString();
        //        return result;
        //    }
        //}

        // Delete (Archive) Method

        public async Task<ResultVM> Update(DepartmentVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
        UPDATE Departments SET
            Code = @Code,
            Name = @Name,
            DepartmentName = @DepartmentName,
            Description = @Description,
            StartDate = @StartDate,
            EndDate = @EndDate,
            DepartmentType = @DepartmentType,
            ApprovalStatus = @ApprovalStatus,
            FundingSource = @FundingSource,

            OwnFund = @OwnFund,
            OwnFundAmt = @OwnFundAmt,

            GovernmentGrant = @GovernmentGrant,
            GovernmentAmt = @GovernmentAmt,

            GovernmentLoan = @GovernmentLoan,
            GovernmentLoanAmt = @GovernmentLoanAmt,

            ForeignGrant = @ForeignGrant,
            ForeignGrantAmt = @ForeignGrantAmt,

            ForeignLoan = @ForeignLoan,
            ForeignLoanAmt = @ForeignLoanAmt,

            ShareCapital = @ShareCapital,
            ShareCapitalAmt = @ShareCapitalAmt,

            Others = @Others,
            ProjectName = @ProjectName,
            TotalValue = @TotalValue,

            Reference = @Reference,
            Remarks = @Remarks,
            IsActive = @IsActive,
            IsArchive = @IsArchive,

            LastUpdateBy = @LastUpdateBy,
            LastUpdateFrom = @LastUpdateFrom,
            LastUpdateOn = GETDATE()

        WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);

                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@DepartmentName", (object?)vm.DepartmentName ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@Description", vm.Description ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@StartDate",vm.StartDate ?? (object)DBNull.Value);

                    cmd.Parameters.AddWithValue("@EndDate",vm.EndDate ?? (object)DBNull.Value);

                    cmd.Parameters.AddWithValue("@DepartmentType", (object?)vm.DepartmentType ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ApprovalStatus", (object?)vm.ApprovalStatus ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@FundingSource", (object?)vm.FundingSource ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@OwnFund", (object?)vm.OwnFund ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@OwnFundAmt", (object?)vm.OwnFundAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@GovernmentGrant", (object?)vm.GovernmentGrant ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@GovernmentAmt", (object?)vm.GovernmentAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@GovernmentLoan", (object?)vm.GovernmentLoan ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@GovernmentLoanAmt", (object?)vm.GovernmentLoanAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@ForeignGrant", (object?)vm.ForeignGrant ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ForeignGrantAmt", (object?)vm.ForeignGrantAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@ForeignLoan", (object?)vm.ForeignLoan ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ForeignLoanAmt", (object?)vm.ForeignLoanAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@ShareCapital", (object?)vm.ShareCapital ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ShareCapitalAmt", (object?)vm.ShareCapitalAmt ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@Others", (object?)vm.Others ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@ProjectName", (object?)vm.ProjectName ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@TotalValue", (object?)vm.TotalValue ?? DBNull.Value);

                    cmd.Parameters.AddWithValue("@Reference", vm.Reference ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Remarks", vm.Remarks ?? (object)DBNull.Value);

                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@IsArchive", vm.IsArchive);

                    // Audit fields
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    cmd.ExecuteNonQuery();
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.UpdateSuccess;
                result.Id = vm.Id.ToString();
                result.DataVM = vm;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }
        public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", Id = string.Join(",", vm.IDs) };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $@"
                UPDATE Examinees
                SET IsArchive = 1, IsActive = 0,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateFrom = @LastUpdateFrom
     
                WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.ModifyBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.ModifyFrom ?? (object)DBNull.Value);

                    int rows = cmd.ExecuteNonQuery();
                    if (rows > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message =MessageModel.DeleteSuccess;
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
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> InsertDetails(DepartmentSabreVM detail, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

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

                INSERT INTO DepartmentSabres
                (
                    DepartmentId, SabreId
                )
                VALUES
                (
                    @DepartmentId, @SabreId
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@DepartmentId", detail.DepartmentId);
                    cmd.Parameters.AddWithValue("@SabreId", detail.SabreId);
                    object newId = await cmd.ExecuteScalarAsync();

                    detail.Id = Convert.ToInt32(newId);

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
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
                result.Message = MessageModel.InsertFail;
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

        // List Method
        //public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
        //    SqlConnection conn = null, SqlTransaction transaction = null)
        //{
        //    DataTable dt = new DataTable();
        //    ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

        //    try
        //    {
        //        if (conn == null) throw new Exception("Database connection failed!");

        //        string query = @"
        //        SELECT 
        //            ISNULL(M.Id,0) AS Id,
        //            ISNULL(M.Code, '') AS Code,
        //            ISNULL(M.Name, '') AS Name,
        //            ISNULL(M.Description, '') AS Description,
        //            ISNULL(M.Reference, '') AS Reference,
        //            ISNULL(M.Remarks, '') AS Remarks,                   
        //            ISNULL(M.IsActive, 0) AS IsActive,
        //            ISNULL(M.IsArchive, 0) AS IsArchive,
        //            ISNULL(M.CreatedBy, '') AS CreatedBy,
        //            ISNULL(FORMAT(M.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn,
        //            ISNULL(M.LastUpdateBy, '') AS LastUpdateBy
        //        FROM Departments M
        //        WHERE 1=1";

        //        if (vm != null && !string.IsNullOrEmpty(vm.Id))
        //            query += " AND M.Id=@Id ";

        //        query = ApplyConditions(query, conditionalFields, conditionalValues, false);

        //        SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
        //        adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

        //        if (vm != null && !string.IsNullOrEmpty(vm.Id))
        //            adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

        //        adapter.Fill(dt);

        //        var list = dt.AsEnumerable().Select(row => new DepartmentVM
        //        {
        //            Id = row.Field<int>("Id"),
        //            Code = row.Field<string>("Code"),
        //            Name = row.Field<string>("Name"),
        //            Description = row.Field<string>("Description"),
        //            Reference = row.Field<string>("Reference"),
        //            Remarks = row.Field<string>("Remarks"),
        //            IsActive = row.Field<bool>("IsActive"),
        //            IsArchive = row.Field<bool>("IsArchive"),
        //            CreatedBy = row.Field<string>("CreatedBy"),
        //            CreatedOn = row.Field<string>("CreatedOn"),
        //            LastUpdateBy = row.Field<string>("LastUpdateBy"),
        //           // LastUpdateAt = row.Field<string>("LastUpdateOn")
        //        }).ToList();

        //        // get details list and bind it DepartmentVM.SabreList

        //        var detailsDataList = DetailsList(new[] { "D.DepartmentId" }, conditionalValues, vm, conn, transaction);

        //        if (detailsDataList.Status == "Success" && detailsDataList.DataVM is DataTable ddt)
        //        {

        //            string json = JsonConvert.SerializeObject(ddt);
        //            var details = JsonConvert.DeserializeObject<List<DepartmentSabreVM>>(json);
        //            foreach (var req in list)
        //            {
        //                req.SabreList = details.Where(d => d.DepartmentId == req.Id).ToList();
        //            }

        //        }

        //        result.Status = MessageModel.Success;
        //        result.Message = MessageModel.RetrievedSuccess;
        //        result.DataVM = list;

        //        return result;
        //    }
        //    catch (Exception ex)
        //    {
        //        result.Message = ex.Message;
        //        result.ExMessage = ex.ToString();
        //        return result;
        //    }
        //}

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
    SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
        SELECT 
            ISNULL(M.Id,0) AS Id,
            ISNULL(M.Code, '') AS Code,
            ISNULL(M.Name, '') AS Name,
            ISNULL(M.DepartmentName, '') AS DepartmentName,
            ISNULL(M.Description, '') AS Description,

            ISNULL(FORMAT(M.StartDate, 'yyyy-MM-dd'), '') AS StartDate,
            ISNULL(FORMAT(M.EndDate, 'yyyy-MM-dd'), '') AS EndDate,
            ISNULL(M.DepartmentType,'') AS DepartmentType,
            ISNULL(M.ApprovalStatus,'') AS ApprovalStatus,
            ISNULL(M.FundingSource,'') AS FundingSource,

            ISNULL(M.OwnFund,0) AS OwnFund,
            ISNULL(M.OwnFundAmt,0) AS OwnFundAmt,

            ISNULL(M.GovernmentGrant,0) AS GovernmentGrant,
            ISNULL(M.GovernmentAmt,0) AS GovernmentAmt,

            ISNULL(M.GovernmentLoan,0) AS GovernmentLoan,
            ISNULL(M.GovernmentLoanAmt,0) AS GovernmentLoanAmt,

            ISNULL(M.ForeignGrant,0) AS ForeignGrant,
            ISNULL(M.ForeignGrantAmt,0) AS ForeignGrantAmt,

            ISNULL(M.ForeignLoan,0) AS ForeignLoan,
            ISNULL(M.ForeignLoanAmt,0) AS ForeignLoanAmt,

            ISNULL(M.ShareCapital,0) AS ShareCapital,
            ISNULL(M.ShareCapitalAmt,0) AS ShareCapitalAmt,

            ISNULL(M.Others,'') AS Others,
            ISNULL(M.ProjectName,'') AS ProjectName,
            ISNULL(M.TotalValue,0) AS TotalValue,

            ISNULL(M.Reference, '') AS Reference,
            ISNULL(M.Remarks, '') AS Remarks,                   
            ISNULL(M.IsActive, 0) AS IsActive,
            ISNULL(M.IsArchive, 0) AS IsArchive,

            ISNULL(M.CreatedBy, '') AS CreatedBy,
            ISNULL(FORMAT(M.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn,

            ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
            ISNULL(FORMAT(M.LastUpdateOn,'yyyy-MM-dd HH:mm'),'') AS LastUpdateOn

        FROM Departments M
        WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND M.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new DepartmentVM
                {
                    Id = row.Field<int>("Id"),
                    Code = row.Field<string>("Code"),
                    Name = row.Field<string>("Name"),
                    DepartmentName = row.Field<string>("DepartmentName"),
                    Description = row.Field<string>("Description"),

                    StartDate = row.Field<string?>("StartDate"),
                    EndDate = row.Field<string?>("EndDate"),
                    DepartmentType = row.Field<string>("DepartmentType"),
                    ApprovalStatus = row.Field<string>("ApprovalStatus"),
                    FundingSource = row.Field<bool>("FundingSource"),

                    OwnFund = row.Field<bool>("OwnFund"),
                    OwnFundAmt = row.Field<decimal?>("OwnFundAmt"),

                    GovernmentGrant = row.Field<bool>("GovernmentGrant"),
                    GovernmentAmt = row.Field<decimal?>("GovernmentAmt"),

                    GovernmentLoan = row.Field<bool>("GovernmentLoan"),
                    GovernmentLoanAmt = row.Field<decimal?>("GovernmentLoanAmt"),

                    ForeignGrant = row.Field<bool>("ForeignGrant"),
                    ForeignGrantAmt = row.Field<decimal?>("ForeignGrantAmt"),

                    ForeignLoan = row.Field<bool>("ForeignLoan"),
                    ForeignLoanAmt = row.Field<decimal?>("ForeignLoanAmt"),

                    ShareCapital = row.Field<bool>("ShareCapital"),
                    ShareCapitalAmt = row.Field<decimal?>("ShareCapitalAmt"),

                    Others = row.Field<bool>("Others"),
                    ProjectName = row.Field<bool>("ProjectName"),
                    TotalValue = row.Field<decimal?>("TotalValue"),

                    Reference = row.Field<string>("Reference"),
                    Remarks = row.Field<string>("Remarks"),

                    IsActive = row.Field<bool>("IsActive"),
                    IsArchive = row.Field<bool>("IsArchive"),

                    CreatedBy = row.Field<string>("CreatedBy"),
                    CreatedOn = row.Field<string>("CreatedOn"),

                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateOn = row.Field<string>("LastUpdateOn")

                }).ToList();

                // 🔥 Details binding (unchanged but fixed condition)
                var detailsDataList = DetailsList(new[] { "D.DepartmentId" }, conditionalValues, vm, conn, transaction);

                if (detailsDataList.Status == MessageModel.Success && detailsDataList.DataVM is DataTable ddt)
                {
                    string json = JsonConvert.SerializeObject(ddt);
                    var details = JsonConvert.DeserializeObject<List<DepartmentSabreVM>>(json);

                    foreach (var req in list)
                    {
                        req.SabreList = details.Where(d => d.DepartmentId == req.Id).ToList();
                    }
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = list;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public ResultVM DetailsList(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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

             ISNULL(D.Id, 0) AS Id,
            ISNULL(D.DepartmentId, 0) AS DepartmentId,
            ISNULL(D.SabreId, 0) AS SabreId,

            ISNULL(C.Code, '') AS iBASCode,
            ISNULL(C.Name, '') AS iBASName,
            ISNULL(sb.Name, '') AS Name,
            ISNULL(sb.Code, '') AS Code
            FROM DepartmentSabres D
			LEFT JOIN Departments SG ON D.DepartmentId = SG.Id
			LEFT OUTER JOIN Sabres sb ON sb.Id = D.SabreId
           LEFT OUTER JOIN COAs C on C.Id=D.SabreId
            

            WHERE 1 = 1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.DepartmentId = @Id ";
                }

                query = ApplyConditions(query, conditionalFields, conditionalValue, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

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
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }


        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT Id,Code,Name, Description, Reference, Remarks, IsActive, IsArchive, CreatedBy, CreatedAt, LastUpdateBy, LastUpdateAt
                FROM Departments
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND Id=@Id";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                result.Status = MessageModel.Success;
                result.Message =MessageModel.RetrievedSuccess;
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };
            DataTable dt = new DataTable();

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT Id, Name
                FROM Departments
                WHERE IsActive = 1 AND IsArchive = 0
                ORDER BY Name";

                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                        adapter.SelectCommand.Transaction = transaction;
                    adapter.Fill(dt);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dt;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        // GetGridData Method
        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                var data = new GridEntity<DepartmentVM>();

                string sqlQuery = @"
                -- Count
                SELECT COUNT(DISTINCT H.Id) AS totalcount
                FROM Departments H
                WHERE H.IsArchive != 1
                " + (options.filter.Filters.Count > 0
                        ? " AND (" + GridQueryBuilder<DepartmentVM>.FilterCondition(options.filter) + ")"
                        : "") + @"

                -- Data
                SELECT *
                FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY " +
                        (options.sort.Count > 0
                            ? " " + options.sort[0].field + " " + options.sort[0].dir
                            : "H.Id DESC") + @") AS rowindex,
                           ISNULL(H.Id,0) AS Id,
                           ISNULL(H.Code,'') AS Code,
                           ISNULL(H.Name,'') AS Name,
                           ISNULL(H.DepartmentName,'') AS DepartmentName,
                           ISNULL(H.Description,'') AS Description,
                           ISNULL(H.Reference,'') AS Reference,
                           ISNULL(H.Remarks,'') AS Remarks,
                           ISNULL(H.IsActive,0) AS IsActive,
                           CASE WHEN ISNULL(H.IsActive,0)=1 THEN 'Active' ELSE 'Inactive' END AS Status,
                           ISNULL(H.CreatedBy,'') AS CreatedBy,
                           ISNULL(FORMAT(H.CreatedOn,'yyyy-MM-dd HH:mm'),'') AS CreatedOn,
                           ISNULL(H.LastUpdateBy,'') AS LastUpdateBy,
                           ISNULL(FORMAT(H.LastUpdateOn,'yyyy-MM-dd HH:mm'),'') AS LastUpdateOn
                    FROM Departments H
                    WHERE H.IsArchive != 1
                    " + (options.filter.Filters.Count > 0
                            ? " AND (" + GridQueryBuilder<DepartmentVM>.FilterCondition(options.filter) + ")"
                            : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take=0 OR rowindex <= @take)";

                data = KendoGrid<DepartmentVM>.GetGridDataQuestions_CMD(options, sqlQuery, "H.Id");

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = data;

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }
    }
}
