using Newtonsoft.Json;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.QuestionVM;
using System;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.Question
{
    public class QuestionSetDetailRepository : CommonRepository
    {
        #region Insert
        public async Task<ResultVM> Insert(QuestionSetDetailVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };
            try
            {
                if (conn == null) throw new Exception("Database connection failed!");
                if (transaction == null) transaction = conn.BeginTransaction();

                string query = @"
                INSERT INTO QuestionSetDetails
                (
                    QuestionSetHeaderId, QuestionHeaderId, QuestionMark
                )
                VALUES
                (
                    @QuestionSetHeaderId, @QuestionHeaderId, @QuestionMark
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@QuestionSetHeaderId", vm.QuestionSetHeaderId);
                    cmd.Parameters.AddWithValue("@QuestionHeaderId", vm.QuestionHeaderId);
                    cmd.Parameters.AddWithValue("@QuestionMark", vm.QuestionMark);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());
                }

                result.Status = "Success";
                result.Message = "QuestionSetDetail inserted successfully.";
                result.Id = vm.Id.ToString();
                result.DataVM = vm;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }
            return result;
        }
        #endregion

        #region List
        public ResultVM List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            try
            {
                if (conn == null) throw new Exception("Database connection failed!");

                string query = @"
                SELECT 
                    ISNULL(M.Id,0) AS Id, 
                    ISNULL(M.QuestionSetHeaderId,0) AS QuestionSetHeaderId,
                    ISNULL(M.QuestionHeaderId,0) AS QuestionHeaderId,
                    ISNULL(M.QuestionMark,0) AS QuestionMark
                FROM QuestionSetDetails M
                WHERE 1=1";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND M.Id=@Id ";
                }

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

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
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }
        #endregion

    }
}
