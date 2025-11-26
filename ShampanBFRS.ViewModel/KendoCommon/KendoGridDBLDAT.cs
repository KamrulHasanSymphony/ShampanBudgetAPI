using ShampanBFRS.ViewModel.Utility;
using System.Data;
using System.Data.SqlClient;


namespace ShampanBFRS.ViewModel.KendoCommon
{
    public class KendoGridDBLDAT<T>
    {
        private static SqlDataAdapter da;
        private static SqlConnection dbConn;
        private static SqlCommand cmd;
        private static DataSet ds;
        private static DataTable dt;
        private static int totalCount = 0;
       
        public static GridEntity<T> GetGridData(GridOptions gridOption, string ProcName, string CallType, string orderby, string param1 = "", string param2 = "", string param3 = "", string param4 = "", string param5 = "", string param6 = "")
        {
            try
            {
                gridOption.take = gridOption.skip + gridOption.take;
                var filterby = "";
                if (gridOption.filter != null)
                {
                    filterby = gridOption != null ? GridQueryBuilder<T>.FilterCondition(gridOption.filter) : "";
                }
                if (gridOption.sort != null)
                {
                    orderby = gridOption.sort[0].field + " " + gridOption.sort[0].dir;
                }

                dbConn = new SqlConnection(DatabaseHelper.GetConnectionString());
                dbConn.Open();
                cmd = new SqlCommand(ProcName, dbConn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("@CallType", CallType));
                cmd.Parameters.Add(new SqlParameter("@skip", gridOption.skip));
                cmd.Parameters.Add(new SqlParameter("@take ", gridOption.take));
                cmd.Parameters.Add(new SqlParameter("@filter", filterby));
                cmd.Parameters.Add(new SqlParameter("@orderby", orderby.Trim()));
                cmd.Parameters.Add(new SqlParameter("@param1", param1));
                cmd.Parameters.Add(new SqlParameter("@param2", param2));
                cmd.Parameters.Add(new SqlParameter("@param3", param3));
                cmd.Parameters.Add(new SqlParameter("@param4", param4));
                cmd.Parameters.Add(new SqlParameter("@param5", param5));
                cmd.Parameters.Add(new SqlParameter("@param6", param6));
                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);
                dbConn.Close();
                dbConn.Dispose();
                cmd.Dispose();

                dt = ds.Tables[1];
                totalCount = Convert.ToInt32(ds.Tables[0].Rows[0]["TotalCount"]);
                var dataList = (List<T>)ListConversion.ConvertTo<T>(dt).ToList();
                var result = new GridResult<T>().Data(dataList, totalCount);

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }



    }
}
