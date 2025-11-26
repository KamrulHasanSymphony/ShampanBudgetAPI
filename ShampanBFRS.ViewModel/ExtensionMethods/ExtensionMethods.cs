using Newtonsoft.Json;
using System.Data;
using System.Reflection;

namespace ShampanBFRS.ViewModel.ExtensionMethods
{
    public static class ExtensionMethods
    {

        public static DataTable DtColumnNameChangeList(DataTable table, List<string> oldColumnNames, List<string> newColumnNames)
        {
            DataTable resultDt = new DataTable();
            resultDt = table;

            // Iterate through each old column name
            for (int i = 0; i < oldColumnNames.Count; i++)
            {
                // Get the corresponding column index
                int columnIndex = resultDt.Columns.IndexOf(oldColumnNames[i]);

                // If the column exists, change its name to the new column name
                if (columnIndex >= 0)
                {
                    resultDt.Columns[columnIndex].ColumnName = newColumnNames[i];
                }
            }
            return resultDt;
        }
       
        
        public static string DataTableToJson(DataTable dataTable)
        {
            try
            {
                string json = JsonConvert.SerializeObject(dataTable, Formatting.Indented);
                return json;
            }
            catch (Exception)
            {
                return "";
            }

        }

        public static T DeserializeJson<T>(string jsonData)
        {
            if (string.IsNullOrEmpty(jsonData))
            {
                throw new ArgumentException("JSON data cannot be null or empty", nameof(jsonData));
            }

            return JsonConvert.DeserializeObject<T>(jsonData);
        }
        public static DataTable ToDataTable<T>(this List<T> items)
        {
            var dt = new DataTable(typeof(T).Name);
            var props = typeof(T).GetProperties();

            foreach (var prop in props)
                dt.Columns.Add(prop.Name, prop.PropertyType);

            foreach (var item in items)
            {
                var values = props.Select(p => p.GetValue(item, null)).ToArray();
                dt.Rows.Add(values);
            }

            return dt;
        }

        public static DataTable ToDataTable<T>(this IEnumerable<T> list)
        {
            DataTable table = new DataTable();

            // Get properties of the type
            PropertyInfo[] props = typeof(T).GetProperties();

            // Create columns based on properties
            foreach (PropertyInfo prop in props)
            {
                table.Columns.Add(prop.Name, prop.PropertyType);
            }

            // Add rows to the table
            foreach (T item in list)
            {
                var values = props.Select(p => p.GetValue(item, null)).ToArray();
                table.Rows.Add(values);
            }

            return table;
        }

        public static List<Dictionary<string, object>> ConvertDataTableToList(DataTable dt)
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


    }


}
