using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.SetUp;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data.SqlClient;

namespace ShampanBFRS.Service.SetUp
{
    public class FiscalYearService
    {
        
        public async Task<ResultVM> Insert(FiscalYearVM fiscalYear)
        {
            string CodeGroup = "FiscalYear";
            string CodeName = "FiscalYear";
            CommonRepository _commonRepo = new CommonRepository();
            FiscalYearRepository _repo = new FiscalYearRepository();

            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;
            try
            {
                // Establish a new connection
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                // Begin a new transaction
                transaction = conn.BeginTransaction();

                // Check for duplicate fiscal year before inserting
                bool duplicateFiscal = _repo.DuplicateFiscal(fiscalYear.Year, conn, transaction);
                if (duplicateFiscal)
                {
                    // Rollback the transaction if fiscal year already exists
                    transaction.Rollback();

                    // Set result as failure, indicating fiscal year exists
                    result.Status = "Fail";
                    result.Message = "Fiscal Year already exists.";
                    result.Id = fiscalYear.Id.ToString();
                    return result;
                }

                // Proceed with the insert if no duplicate was found
                result = await _repo.Insert(fiscalYear, conn, transaction);

                // Commit the transaction if the insert was successful
                if (result.Status == "Success" && isNewConnection)
                {
                    transaction.Commit();
                }
                else
                {
                    // Rollback the transaction if insert failed
                    transaction.Rollback();
                }

                return result;
            }
            catch (Exception ex)
            {
                // Rollback in case of an exception
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }

                // Capture exception details in the result
                result.ExMessage = ex.ToString();
                result.Message = "Error in inserting fiscal year.";
                return result;
            }
            finally
            {
                // Ensure the connection is closed after the operation
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> Update(FiscalYearVM fiscalYear)
        {
            FiscalYearRepository _repo = new FiscalYearRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                // Open a new database connection
                conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                conn.Open();
                isNewConnection = true;

                // Start a new transaction
                transaction = conn.BeginTransaction();

                // Check if fiscal year is a duplicate
                //bool duplicateFiscal =  _repo.DuplicateFiscal(fiscalYear.Year, conn, transaction);
                //if (duplicateFiscal)
                //{
                //    // Rollback the transaction if the fiscal year is a duplicate
                //    transaction.Rollback();
                //    result.Status = "Fail";
                //    result.Message = "Fiscal Year already exists.";
                //    result.Id = fiscalYear.Id.ToString();
                //    return result;
                //}

                // Proceed with the update if it's not a duplicate
                result = await _repo.Update(fiscalYear, conn, transaction);

                // Commit the transaction if the update was successful
                if (result.Status == "Success" && isNewConnection)
                {
                    transaction.Commit();
                }
                else
                {
                    transaction.Rollback();
                }

                return result;
            }
            catch (Exception ex)
            {
                // Rollback transaction in case of an error
                if (transaction != null && isNewConnection)
                {
                    transaction.Rollback();
                }

                // Set the exception message in the result and return it
                result.ExMessage = ex.ToString();
                result.Message = "Error in updating fiscal year.";
                return result;
            }
            finally
            {
                // Close the connection if it was opened in this method
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }

        public async Task<ResultVM> Delete(CommonVM vm)
        {
            FiscalYearRepository _repo = new FiscalYearRepository();
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
            FiscalYearRepository _repo = new FiscalYearRepository();
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
            FiscalYearRepository _repo = new FiscalYearRepository();
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

                result = await _repo.ListAsDataTable(conditionalFields, conditionalValues, vm,conn, transaction);

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
            FiscalYearRepository _repo = new FiscalYearRepository();
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

        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            FiscalYearRepository _repo = new FiscalYearRepository();
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

                result = await _repo.GetGridData(options, conn, transaction);

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
                result.Message = ex.ToString();
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
