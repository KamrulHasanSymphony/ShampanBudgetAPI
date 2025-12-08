using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace ShampanBFRS.Repository.Common
{

    public class CommonRepository
    {
        protected SqlConnection _context;
        protected SqlTransaction _transaction;

        #region
        public async Task<ResultVM> NextPrevious(string id, string status, string tableName, string type, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                int getId = 0;
                int branchId = 0;
                string sqlText = "";

                if (type.ToLower() == "transactional")
                {
                    sqlText = $@" SELECT ISNULL(BranchId,0) BranchId  FROM {tableName} WHERE Id=@Id ";

                    SqlDataAdapter command = CreateAdapter(sqlText, conn, transaction);
                    command.SelectCommand.Parameters.AddWithValue("@Id", id);

                    DataTable dt = new DataTable();
                    command.Fill(dt);

                    if (dt.Rows.Count > 0)
                    {
                        branchId = Convert.ToInt16(dt.Rows[0]["BranchId"]);
                    }
                }

                string getSqlText = "";
                if (status.ToLower() == "previous")
                {
                    getSqlText = $@" SELECT TOP 1 Id  FROM {tableName} WHERE 1=1 AND  Id<@Id ";

                    if (type.ToLower() == "transactional")
                    {
                        getSqlText += " AND BranchId=@BranchId ";
                    }
                }
                else if (status.ToLower() == "next")
                {
                    getSqlText = $@" SELECT TOP 1 Id  FROM {tableName} WHERE 1=1 AND  Id>@Id ";

                    if (type.ToLower() == "transactional")
                    {
                        getSqlText += " AND BranchId=@BranchId ";
                    }
                }
                if (status.ToLower() == "previous")
                {
                    getSqlText += " ORDER BY Id DESC ";
                }
                else if (status.ToLower() == "next")
                {
                    getSqlText += " ORDER BY Id ASC ";
                }

                SqlDataAdapter preCommand = CreateAdapter(getSqlText, conn, transaction);

                preCommand.SelectCommand.Parameters.AddWithValue("@Id", id);
                preCommand.SelectCommand.Parameters.AddWithValue("@BranchId", branchId);

                DataTable table = new DataTable();
                preCommand.Fill(table);

                if (table.Rows.Count > 0)
                {
                    getId = Convert.ToInt16(table.Rows[0]["Id"]);
                }

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Id = getId.ToString();
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public string GenerateCode(string CodeGroup, string CodeName, string EntryDate, int? branchId = 0, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            try
            {
                _context = conn;
                _transaction = transaction;

                string sqlText = "";

                string NewCode = "";
                string CodePreFix = "";
                string CodeGenerationFormat = "Branch/Number/Year";
                string CodeGenerationMonthYearFormat = "MMYY";
                string BranchCode = "001";
                string CurrentYear = "2020";
                string BranchNumber = "1";
                int CodeLength = 0;
                int nextNumber = 0;

                DataTable dt1 = new DataTable();
                DataTable dt2 = new DataTable();
                DataSet ds = new DataSet();

                DateTime TransactionDate = Convert.ToDateTime(EntryDate);
                string year = Convert.ToDateTime(DateTime.Now).ToString("yyyy");

                int BranchId = Convert.ToInt32(branchId);


                sqlText += @" SELECT top 1  SettingName FROM Settings ";
                sqlText += @" 
                            WHERE (SettingGroup ='" + CodeGenerationFormat + "') and   (SettingValue ='Y')  ";

                sqlText += @" 
                            SELECT top 1  SettingName FROM Settings";
                sqlText += @" 
                            WHERE  (SettingGroup ='" + CodeGenerationFormat + "') and   (SettingValue ='Y')  ";


                sqlText += @" 
                            SELECT  top 1 Code FROM BranchProfiles ";
                sqlText += @" 
                            WHERE  (Id ='" + BranchId + "')   ";

                sqlText += @" 
                            SELECT   count(Code) BranchNumber FROM BranchProfiles where IsArchive=0 and IsActive=1 ";

                sqlText += @"  
                        SELECT * from  CodeGenerations where CurrentYear<='2020' ";

                sqlText += @"  
                        SELECT YEAR from FiscalYears where '" + Convert.ToDateTime(TransactionDate).ToString("yyyy/MM/dd") + "' between YearStart and YearEnd ";

                SqlCommand command = CreateCommand(sqlText);

                SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(ds);


                if (ds.Tables[0] != null && ds.Tables[0].Rows.Count > 0)
                    CodeGenerationFormat = ds.Tables[0].Rows[0][0].ToString();

                if (ds.Tables[1] != null && ds.Tables[1].Rows.Count > 0)
                    CodeGenerationMonthYearFormat = ds.Tables[1].Rows[0][0].ToString();
                if (ds.Tables[2] != null && ds.Tables[2].Rows.Count > 0)
                    BranchCode = ds.Tables[2].Rows[0][0].ToString();

                BranchNumber = BranchId.ToString();


                sqlText = "  ";
                sqlText += "  update CodeGenerations set CurrentYear ='2020'  where CurrentYear <='2020'   ";

                command = CreateCommand(sqlText);
                command.ExecuteNonQuery();

                if (ds.Tables[5] != null && ds.Tables[5].Rows.Count > 0)
                {
                    CurrentYear = ds.Tables[5].Rows[0][0].ToString();
                }
                else
                {
                    throw new ArgumentNullException("Fiscal year not set yet!");
                }

                sqlText = "  ";

                sqlText += " SELECT * FROM Codes";
                sqlText += " WHERE ( CodeGroup = @CodeGroup ) AND (CodeName = @CodeName) ";

                command.CommandText = sqlText;


                command.Parameters.AddWithValue("@CodeGroup", CodeGroup);
                command.Parameters.AddWithValue("@CodeName", CodeName);

                dataAdapter = new SqlDataAdapter(command);


                dataAdapter.Fill(dt1);
                if (dt1 == null || dt1.Rows.Count <= 0)
                {
                    throw new ArgumentNullException();
                }
                else
                {
                    CodePreFix = dt1.Rows[0]["prefix"].ToString();
                    CodeLength = Convert.ToInt32(dt1.Rows[0]["Lenth"]);
                }

                sqlText = "  ";

                sqlText += @" 
SELECT top 1 
Id
,CurrentYear
,BranchId
,Prefix
,ISNULL(LastId,0) LastId
FROM CodeGenerations 
WHERE CurrentYear=@CurrentYear AND BranchId=@BranchId AND Prefix=@Prefix order by LastId Desc
";


                command.CommandText = sqlText;


                command.Parameters.AddWithValue("@BranchId", BranchId);
                command.Parameters.AddWithValue("@CurrentYear", CurrentYear);
                command.Parameters.AddWithValue("@Prefix", CodePreFix);


                dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(dt2);


                if (dt2 == null || dt2.Rows.Count <= 0)
                {
                    sqlText = "  ";
                    sqlText +=
                        " INSERT INTO CodeGenerations(	CurrentYear,BranchId,Prefix,LastId)";
                    sqlText += " VALUES(";
                    sqlText += " @CurrentYear,";
                    sqlText += " @BranchId,";
                    sqlText += " @Prefix,";
                    sqlText += " 1";
                    sqlText += " )";

                    command.CommandText = sqlText;

                    object objfoundId1 = command.ExecuteNonQuery();

                    nextNumber = 1;
                }
                else
                {
                    if (nextNumber != 1)
                    {
                        nextNumber = dt2.Rows[0]["LastId"] == null ? 1 : Convert.ToInt32(dt2.Rows[0]["LastId"]) + 1;
                    }

                    sqlText = "  ";
                    sqlText += " update  CodeGenerations set LastId='" + nextNumber + "'";
                    sqlText += " WHERE CurrentYear=@CurrentYear AND BranchId=@BranchId AND Prefix=@Prefix";


                    command.CommandText = sqlText;
                    command.ExecuteNonQuery();
                }

                NewCode = NextId(CodeGenerationMonthYearFormat, BranchNumber, CodeGenerationFormat, BranchCode.Trim(), CodeLength, nextNumber, CodePreFix, TransactionDate.ToString());
                return NewCode;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private string NextId(string CodeGenerationMonthYearFormat, string BranchNumber, string CodeGenerationFormat, string BranchCode, int CodeLength
           , int nextNumber, string CodePreFix, string TransactionDate)
        {
            string NewCode = "";
            #region try
            try
            {
                CodeGenerationMonthYearFormat = CodeGenerationMonthYearFormat.Replace("Y", "y");
                if (Convert.ToInt32(BranchNumber) < 1)
                {
                    CodeGenerationFormat = CodeGenerationFormat.Replace("B/", "");
                }

                var my = Convert.ToDateTime(TransactionDate).ToString(CodeGenerationMonthYearFormat);
                var nextNumb = nextNumber.ToString().PadLeft(CodeLength, '0');
                CodeGenerationFormat = CodeGenerationFormat.Replace("Branch", BranchCode);
                CodeGenerationFormat = CodeGenerationFormat.Replace("Number", nextNumb);
                CodeGenerationFormat = CodeGenerationFormat.Replace("Year", my);

                NewCode = CodePreFix + "-" + CodeGenerationFormat;
            }

            #endregion
            catch (Exception ex)
            {
                throw ex;
            }

            return NewCode;
        }

        public string CodeGenerationNo(string codeGroup, string codeName, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            try
            {
                _context = conn;
                _transaction = transaction;

                string sqlText = "";

                string NewCode = "";
                string CodePreFix = "";
                int CodeLength = 0;
                int nextNumber = 0;

                DataTable dt1 = new DataTable();
                DataTable dt2 = new DataTable();
                DataSet ds = new DataSet();

                SqlCommand command = CreateCommand(sqlText);

                sqlText = "  ";

                sqlText += " SELECT * FROM Codes ";
                sqlText += " WHERE ( CodeGroup = @CodeGroup ) AND (CodeName = @CodeName) ";

                command.CommandText = sqlText;

                command.Parameters.AddWithValue("@CodeGroup", codeGroup);
                command.Parameters.AddWithValue("@CodeName", codeName);

                SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(dt1);

                if (dt1 == null || dt1.Rows.Count <= 0)
                {
                    throw new ArgumentNullException();
                }
                else
                {
                    CodePreFix = dt1.Rows[0]["prefix"].ToString();
                    CodeLength = Convert.ToInt32(dt1.Rows[0]["Lenth"]);
                }

                sqlText = "  ";
                sqlText += @" 
SELECT top 1 
Id
,CurrentYear
,BranchId
,Prefix
,ISNULL(LastId,0) LastId
FROM CodeGenerations 
WHERE Prefix=@Prefix 
order by LastId Desc ";


                command.CommandText = sqlText;

                command.Parameters.AddWithValue("@Prefix", CodePreFix);

                dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(dt2);


                if (dt2 == null || dt2.Rows.Count <= 0)
                {
                    sqlText = "  ";
                    sqlText +=
                        " INSERT INTO CodeGenerations (CurrentYear,BranchId,Prefix,LastId)";
                    sqlText += " VALUES(";
                    sqlText += " FORMAT(GETDATE(),'yyyy'),";
                    sqlText += " 0,";
                    sqlText += " @Prefix,";
                    sqlText += " 1 ";
                    sqlText += " )";

                    command.CommandText = sqlText;

                    object objfoundId1 = command.ExecuteNonQuery();

                    nextNumber = 1;
                }
                else
                {
                    if (nextNumber != 1)
                    {
                        nextNumber = dt2.Rows[0]["LastId"] == null ? 1 : Convert.ToInt32(dt2.Rows[0]["LastId"]) + 1;
                    }

                    sqlText = "  ";
                    sqlText += " update  CodeGenerations set LastId='" + nextNumber + "'";
                    sqlText += " WHERE Prefix=@Prefix";


                    command.CommandText = sqlText;
                    command.ExecuteNonQuery();
                }

                NewCode = NextCodeGenerationNo(CodeLength, nextNumber, CodePreFix);
                return NewCode;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private string NextCodeGenerationNo(int CodeLength, int nextNumber, string CodePreFix)
        {
            string NewCode = "0";

            try
            {
                string CodeGenerationFormat = string.Empty;
                var nextNumb = nextNumber.ToString().PadLeft(CodeLength, '0');
                NewCode = CodePreFix + "-" + nextNumb;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return NewCode;

        }

        protected SqlCommand CreateCommand(string query)
        {
            return new SqlCommand(query, _context, _transaction);
        }

        protected SqlCommand CreateCommand(string query, SqlConnection context, SqlTransaction transaction)
        {
            return new SqlCommand(query, context, transaction);
        }

        protected SqlDataAdapter CreateAdapter(string query)
        {
            var cmd = new SqlCommand(query, _context, _transaction);
            return new SqlDataAdapter(cmd);
        }

        protected SqlDataAdapter CreateAdapter(string query, SqlConnection context, SqlTransaction transaction)
        {
            var cmd = new SqlCommand(query, context, transaction);
            return new SqlDataAdapter(cmd);
        }

        protected SqlDataAdapter CreateAdapter(SqlCommand cmd)
        {
            return new SqlDataAdapter(cmd);
        }

        public string GetSettingsValue(string[] conditionalFields, string[] conditionalValue)
        {
            try
            {
                string sqlText = @" SELECT SettingValue FROM Settings WHERE 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand objComm = CreateCommand(sqlText);

                objComm = ApplyParameters(objComm, conditionalFields, conditionalValue);

                SqlDataAdapter adapter = new SqlDataAdapter(objComm);
                DataTable dt = new DataTable();

                adapter.Fill(dt);

                string settingValue = "2";

                if (dt.Rows.Count > 0)
                {
                    DataRow row = dt.Rows[0];
                    settingValue = row["SettingValue"].ToString();
                }
                return settingValue;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string StringReplacing(string stringToReplace)
        {
            try
            {
                string newString = stringToReplace;
                if (stringToReplace.Contains("."))
                {
                    newString = Regex.Replace(stringToReplace, @"^[^.]*.", "", RegexOptions.IgnorePatternWhitespace);
                }
                newString = newString.Replace(">", "From");
                newString = newString.Replace("<", "To");
                newString = newString.Replace("!", "");
                newString = newString.Replace("[", "");
                newString = newString.Replace("]", "");
                return newString;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string ApplyConditions(string sqlText, string[] conditionalFields, string[] conditionalValue, bool orOperator = false)
        {
            try
            {
                string cField = "";
                string field = "";
                bool conditionFlag = true;
                var checkValueExist = conditionalValue == null ? false : conditionalValue.ToList().Any(x => !string.IsNullOrEmpty(x));
                var checkConditioanlValue = conditionalValue == null ? false : conditionalValue.ToList().Any(x => !string.IsNullOrEmpty(x));

                if (checkValueExist && orOperator && checkConditioanlValue)
                {
                    sqlText += " and (";
                }

                if (conditionalFields != null && conditionalValue != null && conditionalFields.Length == conditionalValue.Length)
                {
                    for (int i = 0; i < conditionalFields.Length; i++)
                    {
                        if (string.IsNullOrWhiteSpace(conditionalFields[i]) || string.IsNullOrWhiteSpace(conditionalValue[i]))
                        {
                            continue;
                        }
                        cField = conditionalFields[i].ToString();
                        field = StringReplacing(cField);
                        cField = cField.Replace(".", "");
                        string operand = " AND ";

                        if (orOperator)
                        {
                            operand = " OR ";

                            if (conditionFlag)
                            {
                                operand = "  ";
                                conditionFlag = false;
                            }
                        }


                        if (conditionalFields[i].ToLower().Contains("like"))
                        {
                            sqlText += operand + conditionalFields[i] + " '%'+ " + " @" + cField.Replace("like", "").Trim() + " +'%'";
                        }
                        //else if (conditionalFields[i].Contains(">=") || conditionalFields[i].Contains("<="))
                        //{
                        //    sqlText += operand + conditionalFields[i] + " @" + cField;
                        //}
                        else if (conditionalFields[i].Contains(">") || conditionalFields[i].Contains("<"))
                        {
                            sqlText += operand + conditionalFields[i] + " @" + cField;
                        }

                        else if (conditionalFields[i].ToLower().Contains("between"))
                        {
                            cField = cField.Replace(" between", "");
                            field = field.Replace(" between", "");
                            string param = conditionalFields[i].Replace(" between", "");
                            sqlText += operand + param + " BETWEEN  @" + cField + " AND @" + field;
                        }
                        else if (conditionalFields[i].ToLower().Contains("not"))
                        {
                            cField = cField.Replace(" not", "");
                            string param = conditionalFields[i].Replace(" not", "");
                            sqlText += operand + param + " != @" + cField;
                        }
                        else if (conditionalFields[i].Contains("in", StringComparison.OrdinalIgnoreCase))
                        {
                            var test = conditionalFields[i].Split(" in");

                            if (test.Length > 1)
                            {
                                sqlText += operand + conditionalFields[i] + "(" + conditionalValue[i] + ")";
                            }
                            else
                            {
                                sqlText += operand + conditionalFields[i] + "= '" + Convert.ToString(conditionalValue[i]) + "'";
                            }
                        }
                        else
                        {
                            sqlText += operand + conditionalFields[i] + "= @" + cField;
                        }
                    }
                }

                if (checkValueExist && orOperator && checkConditioanlValue)
                {
                    sqlText += " )";
                }

                return sqlText;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string ApplyCondition2(string sqlText, string[]? NewconditionalFields, string[]? NewconditionalValues, bool orOperator = false)
        {
            try
            {
                string cField = "";
                string field = "";
                bool conditionFlag = true;
                var checkValueExist = NewconditionalValues == null ? false : NewconditionalValues.ToList().Any(x => !string.IsNullOrEmpty(x));
                var checkConditioanlValue = NewconditionalValues == null ? false : NewconditionalValues.ToList().Any(x => !string.IsNullOrEmpty(x));

                if (checkValueExist && orOperator && checkConditioanlValue)
                {
                    sqlText += " and (";
                }

                if (NewconditionalFields != null && NewconditionalValues != null && NewconditionalFields.Length == NewconditionalValues.Length)
                {
                    for (int i = 0; i < NewconditionalFields.Length; i++)
                    {
                        if (string.IsNullOrWhiteSpace(NewconditionalFields[i]) || string.IsNullOrWhiteSpace(NewconditionalValues[i]))
                        {
                            continue;
                        }
                        cField = NewconditionalFields[i].ToString();
                        field = StringReplacing(cField);
                        cField = cField.Replace(".", "");
                        string operand = " AND ";

                        if (orOperator)
                        {
                            operand = " OR ";

                            if (conditionFlag)
                            {
                                operand = "  ";
                                conditionFlag = false;
                            }
                        }


                        if (NewconditionalFields[i].ToLower().Contains("like"))
                        {
                            sqlText += operand + NewconditionalFields[i] + " '%'+ " + " @" + cField.Replace("like", "").Trim() + " +'%'";
                        }
                        //else if (conditionalFields[i].Contains(">=") || conditionalFields[i].Contains("<="))
                        //{
                        //    sqlText += operand + conditionalFields[i] + " @" + cField;
                        //}
                        else if (NewconditionalFields[i].Contains(">") || NewconditionalFields[i].Contains("<"))
                        {
                            sqlText += operand + NewconditionalFields[i] + " @" + cField;
                        }

                        else if (NewconditionalFields[i].ToLower().Contains("between"))
                        {
                            cField = cField.Replace(" between", "");
                            field = field.Replace(" between", "");
                            string param = NewconditionalFields[i].Replace(" between", "");
                            sqlText += operand + param + " BETWEEN  @" + cField + " AND @" + field;
                        }
                        else if (NewconditionalFields[i].ToLower().Contains("not"))
                        {
                            cField = cField.Replace(" not", "");
                            string param = NewconditionalFields[i].Replace(" not", "");
                            sqlText += operand + param + " != @" + cField;
                        }
                        else if (NewconditionalFields[i].Contains("in", StringComparison.OrdinalIgnoreCase))
                        {
                            var test = NewconditionalFields[i].Split(" in");

                            if (test.Length > 1)
                            {
                                sqlText += operand + NewconditionalFields[i] + "(" + NewconditionalValues[i] + ")";
                            }
                            else
                            {
                                sqlText += operand + NewconditionalFields[i] + "= '" + Convert.ToString(NewconditionalValues[i]) + "'";
                            }
                        }
                        else
                        {
                            sqlText += operand + NewconditionalFields[i] + "= @" + cField;
                        }
                    }
                }

                if (checkValueExist && orOperator && checkConditioanlValue)
                {
                    sqlText += " )";
                }

                return sqlText;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public SqlCommand ApplyParameters(SqlCommand objComm, string[] conditionalFields, string[] conditionalValue)
        {
            try
            {
                string cField = "";
                if (conditionalFields != null && conditionalValue != null && conditionalFields.Length == conditionalValue.Length)
                {
                    for (int j = 0; j < conditionalFields.Length; j++)
                    {
                        if (string.IsNullOrWhiteSpace(conditionalFields[j]) || string.IsNullOrWhiteSpace(conditionalValue[j]))
                        {
                            continue;
                        }
                        cField = conditionalFields[j].ToString();
                        //cField = StringReplacing(cField);
                        cField = cField.Replace(".", "");

                        if (conditionalFields[j].ToLower().Contains("like"))
                        {
                            objComm.Parameters.AddWithValue("@" + cField.Replace("like", "").Trim(), conditionalValue[j]);
                        }
                        else if (conditionalFields[j].ToLower().Contains("not"))
                        {
                            cField = cField.Replace(" not", "");
                            objComm.Parameters.AddWithValue("@" + cField, conditionalValue[j]);
                        }
                        else
                        {
                            objComm.Parameters.AddWithValue("@" + cField, conditionalValue[j]);
                        }
                    }
                }

                return objComm;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public int GetCount(string tableName, string fieldName, string[] conditionalFields, string[] conditionalValue)
        {
            try
            {
                string sqlText = "SELECT COUNT (" + fieldName + ") TotalRecords FROM " + tableName + " WHERE 1=1  ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue, false);

                SqlCommand command = CreateCommand(sqlText);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                int totalRecords = Convert.ToInt32(command.ExecuteScalar());

                return totalRecords;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public int Delete(string tableName, string[] conditionalFields, string[] conditionalValue, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            try
            {
                string sqlText = " UPDATE   " + tableName + " SET IsArchive = 1 AND IsActive = 0 WHERE 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand command = CreateCommand(sqlText, conn, transaction);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                int totalRecords = Convert.ToInt32(command.ExecuteNonQuery());

                return totalRecords;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public ResultVM DetailsDelete(string tableName, string[] conditionalFields, string[] conditionalValue, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                string sqlText = " DELETE FROM " + tableName + "  WHERE 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand command = CreateCommand(sqlText, conn, transaction);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                int totalRecords = Convert.ToInt32(command.ExecuteNonQuery());

                result.Status = "Success";
                result.Message = "Details data deleted.";
                result.Id = totalRecords.ToString();
                result.DataVM = null;

            }
            catch (Exception ex)
            {
                result.Id = "-1";
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
            }

            return result;
        }

        public bool CheckExists(string tableName, string[] conditionalFields, string[] conditionalValue, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            try
            {
                string sqlText = " SELECT COUNT(*)  FROM " + tableName + " WHERE 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand command = CreateCommand(sqlText, conn, transaction);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                int totalRecords = Convert.ToInt32(command.ExecuteScalar());

                return totalRecords > 0;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public bool CheckPostStatus(string tableName, string[] conditionalFields, string[] conditionalValue)
        {
            try
            {
                bool ÌsPost = false;
                string Post = "";

                DataTable dt = new DataTable();

                string sqlText = " SELECT IsPost  FROM " + tableName + " WHERE 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand command = CreateCommand(sqlText);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    Post = dt.Rows[0]["IsPost"].ToString();
                    if (!string.IsNullOrEmpty(Post) && Post.Trim().ToLower() == "y")
                    {
                        ÌsPost = true;
                    }
                }

                return ÌsPost;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public bool CheckCompletedStatus(string tableName, string[] conditionalFields, string[] conditionalValue)
        {
            try
            {
                bool IsCompleted = false;
                string Completed = "";

                DataTable dt = new DataTable();

                string sqlText = " SELECT IsCompleted  FROM " + tableName + " WHERE 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand command = CreateCommand(sqlText);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    Completed = dt.Rows[0]["IsCompleted"].ToString();
                    if (!string.IsNullOrEmpty(Completed) && Completed == "1")
                    {
                        IsCompleted = true;
                    }
                }

                return IsCompleted;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public bool CheckPushStatus(string tableName, string[] conditionalFields, string[] conditionalValue)
        {
            try
            {
                bool ÌsPush = false;
                string Push = "";

                DataTable dt = new DataTable();

                string sqlText = "select IsPush  from " + tableName + " where 1=1 ";

                sqlText = ApplyConditions(sqlText, conditionalFields, conditionalValue);

                SqlCommand command = CreateCommand(sqlText);

                command = ApplyParameters(command, conditionalFields, conditionalValue);

                SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
                dataAdapter.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    Push = dt.Rows[0]["IsPush"].ToString();
                    if (!string.IsNullOrEmpty(Push) && Push.Trim().ToLower() == "y")
                    {
                        ÌsPush = true;
                    }
                }

                return ÌsPush;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public List<Dictionary<string, object>> ConvertDataTableToList(DataTable dt)
        {
            var list = new List<Dictionary<string, object>>();

            foreach (DataRow row in dt.Rows)
            {
                var dict = new Dictionary<string, object>();

                foreach (DataColumn col in dt.Columns)
                {
                    dict[col.ColumnName] = row[col] == DBNull.Value ? null : row[col];
                }

                list.Add(dict);
            }

            return list;
        }

        #endregion
        public async Task<ResultVM> SettingsValue(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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
SELECT ISNULL(SettingValue,'') SettingValue FROM [Settings]
WHERE 1 = 1 ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Value = dataTable.Rows.Count > 0 ? dataTable.Rows[0]["SettingValue"].ToString() : "0";
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

        public async Task<ResultVM> EnumList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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
SELECT [Id]
      ,[Name]
 FROM [EnumTypes]
WHERE 
    1 = 1";


                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new EnumTypeVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Name = row["Name"]?.ToString(),


                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> BulkInsert(string tableName, DataTable dt, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                #region Connection Management
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }
                #endregion

                #region Bulk Insert
                using (var sqlBulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, transaction))
                {
                    sqlBulkCopy.BulkCopyTimeout = 0;
                    sqlBulkCopy.DestinationTableName = tableName;

                    // Configure notification if needed
                    SqlRowsCopiedEventHandler rowsCopiedCallBack = null;
                    if (rowsCopiedCallBack != null)
                    {
                        sqlBulkCopy.NotifyAfter = 500;
                        sqlBulkCopy.SqlRowsCopied += rowsCopiedCallBack;
                    }

                    // Set column mappings
                    foreach (DataColumn column in dt.Columns)
                    {
                        sqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    // Perform bulk insert
                    await sqlBulkCopy.WriteToServerAsync(dt);
                }
                #endregion
                result.Status = "Success";
                result.Message = "Data Imported Successfully";
                result.DataVM = dt;
            }
            catch (Exception ex)
            {

                result.Status = "Fail";
                result.ExMessage = ex.Message;
                result.Message = ex.Message.ToString();
                return result;
            }

            return result;
        }

        // GetProductModalData Method
        public async Task<ResultVM> GetProductModalData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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
ISNULL(P.Id,0) ProductId , 
ISNULL(P.Name,'') ProductName,
ISNULL(P.Code,'') ProductCode, 
ISNULL(P.Description,'') Description, 
CASE WHEN P.IsActive = 1 THEN 'Active' ELSE 'Inactive' END Status

FROM Products P

WHERE P.IsActive = 1 ";

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);
                query += @"  ORDER BY " + vm.OrderName + "  " + vm.orderDir;
                query += @" OFFSET  " + vm.startRec + @" ROWS FETCH NEXT " + vm.pageSize + " ROWS ONLY";

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new ProductVM
                {
                    Id = row.Field<int>("ProductId"),
                    Name = row.Field<string>("ProductName"),
                    Code = row.Field<string>("ProductCode"),
                    //Description = row.Field<string>("Description"),
                    Status = row.Field<string>("Status")

                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }// GetProductModalData Method
        public async Task<ResultVM> GetSegmentModalData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
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
ISNULL(P.Id,0) Id , 
ISNULL(P.Name,'') Name,
ISNULL(P.Code,'') Code, 
ISNULL(P.Remarks,'') Remarks, 
CASE WHEN P.IsActive = 1 THEN 'Active' ELSE 'Inactive' END Status

FROM Segments P

WHERE P.IsActive = 1 ";

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);
                query += @"  ORDER BY " + vm.OrderName + "  " + vm.orderDir;
                query += @" OFFSET  " + vm.startRec + @" ROWS FETCH NEXT " + vm.pageSize + " ROWS ONLY";

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new SegmentVM
                {
                    Id = row.Field<int>("Id"),
                    Name = row.Field<string>("Name"),
                    Code = row.Field<string>("Code"),
                    Remarks = row.Field<string>("Remarks"),
                    Status = row.Field<string>("Status")

                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // GetProductModalCountData Method
        public async Task<ResultVM> GetProductModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM Products P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }// GetProductModalCountData Method
        public async Task<ResultVM> GetSegmentModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM Segments P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // GetProductModalCountData Method
        public async Task<ResultVM> GetMesurementModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM Measurements P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }
        public async Task<ResultVM> GetViewMesurementForOrderCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM OrderMeasurements P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }
        // GetMesurementForOrderCountData Method
        public async Task<ResultVM> GetMesurementForOrderCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM ItemMeasurements P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }
        // GetItemDesignCountData Method
        public async Task<ResultVM> GetItemDesignCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM ItemDesignMapings P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // GetItemModalCountData Method
        public async Task<ResultVM> GetItemModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM Items P
LEFT OUTER JOIN UOMs u ON P.UomId = u.Id

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }
        
        // GetProductModalCountData Method
        public async Task<ResultVM> GetdesignCategoryModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM ItemDesignCategories P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }
        public async Task<ResultVM> GetUomModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM UOMs P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // GetItemModalCountData Method
        public async Task<ResultVM> GetItemModalCountDataForMakingCharge(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM Items P
LEFT OUTER JOIN UOMs u ON P.UomId = u.Id
LEFT OUTER JOIN ItemCategories IC ON P.CategoryId = IC.Id

WHERE P.IsActive = 1 AND IC.Type = 'Finish'";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // GetItemModalCountData Method
        public async Task<ResultVM> GetItemModalCountDataForFabric(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM Items P
LEFT OUTER JOIN UOMs u ON P.UomId = u.Id
LEFT OUTER JOIN ItemCategories IC ON P.CategoryId = IC.Id

WHERE P.IsActive = 1 AND IC.Type = 'Raw'";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetFabricSourceModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM FabricSources P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetDesignTypeModalCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM DesignTypes P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetDesignFollowedModelCountData(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

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
SELECT

ISNULL(COUNT(P.Id), 0) AS FilteredCount

FROM DesignFollows P

WHERE P.IsActive = 1 ";


                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, true);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.Count = Convert.ToInt32(dataTable.Rows[0][0]);
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> GetFiscalYearComboBox(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
SELECT 
[Id] as Id
,[YearName] as [Name]
 FROM FiscalYears
WHERE 
    1 = 1";


                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                query += @" order by Year";

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new EnumTypeVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Name = row["Name"]?.ToString(),


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
        public async Task<ResultVM> COAList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string sqlQuery = @"
	         SELECT DISTINCT top (20)

          ISNULL(H.Id, 0) Id
         ,ISNULL(H.Code, '') Code
         ,ISNULL(H.Name, '') Name     
         ,ISNULL(H.IsActive, 0) IsActive
         ,ISNULL(H.IsArchive, 0) IsArchive
         ,CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive'   END Status

         FROM COAs H

         WHERE H.IsActive = 1

                      ";


                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(sqlQuery, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new COAVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Code = row["Code"]?.ToString(),
                    Name = row["Name"]?.ToString(),
                    IsActive = Convert.ToBoolean(row["IsActive"]),
                    IsArchive = Convert.ToBoolean(row["IsArchive"]),
                 
                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> DepartmentList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string sqlQuery = @"
	         SELECT DISTINCT 

          ISNULL(H.Id, 0) Id
         ,ISNULL(H.Name, '') Name 
         ,ISNULL(H.Description, '') Description
         ,ISNULL(H.Reference, '') Reference
         ,ISNULL(H.Remarks, '') Remarks
         ,ISNULL(H.IsActive, 0) IsActive
         ,ISNULL(H.IsArchive, 0) IsArchive
         ,CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive'   END Status
         FROM Departments H
         WHERE H.IsActive = 1

                      ";


                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(sqlQuery, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new DepartmentVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Name = row["Name"]?.ToString(),
                    Description = row["Description"]?.ToString(),
                    Reference = row["Reference"]?.ToString(),
                    Remarks = row["Remarks"]?.ToString(),
                    IsActive = Convert.ToBoolean(row["IsActive"]),
                    IsArchive = Convert.ToBoolean(row["IsArchive"]),

                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> SabreList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string sqlQuery = @"
	         SELECT DISTINCT 

          ISNULL(H.Id, 0) Id
         ,ISNULL(H.Code, '') Code 
         ,ISNULL(H.Name, '') Name 
         ,ISNULL(H.Remarks, '') Remarks
         ,ISNULL(H.IsActive, 0) IsActive
         ,ISNULL(H.IsArchive, 0) IsArchive
         ,CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive'   END Status
         FROM Sabres H
         WHERE H.IsActive = 1

                      ";


                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(sqlQuery, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new SabresVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Code = row["Code"]?.ToString(),
                    Name = row["Name"]?.ToString(),
                    Remarks = row["Remarks"]?.ToString(),
                    IsActive = Convert.ToBoolean(row["IsActive"]),
                    IsArchive = Convert.ToBoolean(row["IsArchive"]),

                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> COAGroupList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string sqlQuery = @"
	         SELECT DISTINCT

          ISNULL(H.Id, 0) Id
         ,ISNULL(H.Code, '') Code
         ,ISNULL(H.Category, '') Category
         ,ISNULL(H.Name, '') Name
         ,ISNULL(H.Remarks, '') Remarks 
         ,ISNULL(H.IsActive, 0) IsActive
         ,ISNULL(H.IsArchive, 0) IsArchive
         ,CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive'   END Status
         FROM COAGroups H
         WHERE H.IsActive = 1

                      ";


                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(sqlQuery, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new COAGroupVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Code = row["Code"]?.ToString(),
                    Category = row["Category"]?.ToString(),
                    Name = row["Name"]?.ToString(),
                    Remarks = row["Remarks"]?.ToString(),
                    IsActive = Convert.ToBoolean(row["IsActive"]),
                    IsArchive = Convert.ToBoolean(row["IsArchive"])

                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> StructureList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string sqlQuery = @"
	         SELECT DISTINCT
          ISNULL(H.Id, 0) Id
         ,ISNULL(H.Code, '') Code
         ,ISNULL(H.Name, '') Name
         ,ISNULL(H.Remarks, '') Remarks 
         ,ISNULL(H.IsActive, 0) IsActive
         ,ISNULL(H.IsArchive, 0) IsArchive
         ,CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive'   END Status
         FROM Structures H
         WHERE H.IsActive = 1
        ";

                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(sqlQuery, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new StructureVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Code = row["Code"]?.ToString(),
                    Name = row["Name"]?.ToString(),                
                    Remarks = row["Remarks"]?.ToString(),
                    IsActive = Convert.ToBoolean(row["IsActive"]),
                    IsArchive = Convert.ToBoolean(row["IsArchive"]),

                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> ProductGroupList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }
                string sqlQuery = @"
	            SELECT DISTINCT

                    ISNULL(P.Id, 0) Id
                    ,ISNULL(P.Name, '') Name 
                    ,ISNULL(P.Remarks, '') Remarks 
                     FROM ProductGroups P
                  WHERE 1 = 1

                      ";


                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(sqlQuery, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);



                objComm.Fill(dataTable);

                var modelList = dataTable.AsEnumerable().Select(row => new ProductGroupVM
                {
                    Id = Convert.ToInt32(row["Id"]),
                    Name = row["Name"]?.ToString(),
                    Remarks = row["Remarks"]?.ToString()
                }).ToList();


                result.Status = "Success";
                result.Message = "Data retrieved successfully.";
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }
    }

}
