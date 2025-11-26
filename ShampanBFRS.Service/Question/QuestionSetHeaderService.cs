using ShampanBFRS.Repository.Common;
using ShampanBFRS.Repository.Question;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using ShampanBFRS.ViewModel.Utility;
using System.Data.SqlClient;

namespace ShampanBFRS.Service.Question
{
    public class QuestionSetHeaderService
    {
        CommonRepository _commonRepo = new CommonRepository();

        // Insert
        public async Task<ResultVM> Insert(QuestionSetHeaderVM questionSetHeader)
        {
            QuestionSetHeaderRepository _repo = new QuestionSetHeaderRepository();
            QuestionSetDetailRepository questionSetDetailsRepository = new QuestionSetDetailRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                if (questionSetHeader.questionSetDetailList == null || !questionSetHeader.questionSetDetailList.Any())
                {
                    throw new Exception("QuestionSetDetails must have at least one detail!");
                }

                #region Check Exist Data
                string[] conditionField = { "Name" };
                string[] conditionValue = { questionSetHeader.Name.Trim() };

                bool exist = _commonRepo.CheckExists("QuestionSetHeaders", conditionField, conditionValue, conn, transaction);

                if (exist)
                {
                    result.Message = "Data Already Exist!";
                    throw new Exception("Data Already Exist!");
                }
                #endregion

                //string code = _commonRepo.CodeGenerationNo("QuestionSet", "QuestionSet", conn, transaction);
                //if (!string.IsNullOrEmpty(code))
                //{
                //    questionSetHeader.Code = code;

                result = await _repo.Insert(questionSetHeader, conn, transaction);

                questionSetHeader.Id = Convert.ToInt32(result.Id);

                if (result.Status.ToLower() == "success")
                {
                    foreach (var questionSetDetail in questionSetHeader.questionSetDetailList)
                    {
                        questionSetDetail.QuestionSetHeaderId = questionSetHeader.Id;

                        var resultDetail = await questionSetDetailsRepository.Insert(questionSetDetail, conn, transaction);
                        if (resultDetail.Status.ToLower() != "success")
                        {
                            throw new Exception(resultDetail.Message);
                        }
                    }
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
            //    }
            //    else
            //    {
            //        throw new Exception("Code Generation Failed!");
            //    }
            //}
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection) transaction.Rollback();
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }

        // Update
        public async Task<ResultVM> Update(QuestionSetHeaderVM questionSetHeader)
        {
            QuestionSetHeaderRepository _repo = new QuestionSetHeaderRepository();
            QuestionSetDetailRepository questionSetDetailsRepository = new QuestionSetDetailRepository();
            _commonRepo = new CommonRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                if (questionSetHeader.questionSetDetailList == null || !questionSetHeader.questionSetDetailList.Any())
                {
                    throw new Exception("QuestionSetDetails must have at least one detail!");
                }

                var record = _commonRepo.DetailsDelete("QuestionSetDetails", new[] { "QuestionSetHeaderId" }, new[] { questionSetHeader.Id.ToString() }, conn, transaction);
                if (record.Status == "Fail")
                {
                    throw new Exception("Error in Delete for QuestionSetDetails Data.");
                }

                result = await _repo.Update(questionSetHeader, conn, transaction);

                if (result.Status.ToLower() == "success")
                {
                    foreach (var questionSetDetail in questionSetHeader.questionSetDetailList)
                    {
                        questionSetDetail.QuestionSetHeaderId = questionSetHeader.Id;

                        var resultDetail = await questionSetDetailsRepository.Insert(questionSetDetail, conn, transaction);
                        if (resultDetail.Status.ToLower() != "success")
                            throw new Exception(resultDetail.Message);
                    }
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
                if (transaction != null && isNewConnection) transaction.Rollback();
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }

        // List
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null)
        {
            QuestionSetHeaderRepository _repo = new QuestionSetHeaderRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.List(conditionalFields, conditionalValues, vm, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                    transaction.Commit();
                else
                    throw new Exception(result.Message);

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection) transaction.Rollback();
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
            finally
            {
                if (isNewConnection && conn != null) conn.Close();
            }
        }

        // GetGridData
        public async Task<ResultVM> GetGridData(GridOptions options)
        {
            QuestionSetHeaderRepository _repo = new QuestionSetHeaderRepository();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error" };

            bool isNewConnection = false;
            SqlConnection conn = null;
            SqlTransaction transaction = null;

            try
            {
                conn = new SqlConnection(DatabaseHelper.GetConnectionStringQuestion());
                conn.Open();
                isNewConnection = true;
                transaction = conn.BeginTransaction();

                result = await _repo.GetGridData(options, conn, transaction);

                if (isNewConnection && result.Status == "Success")
                    transaction.Commit();
                else
                    throw new Exception(result.Message);

                return result;
            }
            catch (Exception ex)
            {
                if (transaction != null && isNewConnection) transaction.Rollback();
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
