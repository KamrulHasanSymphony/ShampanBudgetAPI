using Microsoft.Extensions.Configuration;
using System.Configuration;
using System.Data.SqlClient;

namespace ShampanBFRS.ViewModel.Utility
{
    public class DatabaseHelper
    {
        public static string GetConnectionStringQuestion()
        {
			try
			{
                // Set up the Configuration to read from appsettings.json
                var configuration = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory())  // Set the base directory to current folder
                 .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)  // Load appsettings.json
                 .Build();

                // Get the connection string from the configuration
                string connectionString = configuration.GetConnectionString("DefaultConnectionQuestion");
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                return connectionString;
            }
			catch (Exception ex)
			{
				throw ex;
			}
        }
        public static string GetConnectionString()
        {
            try
            {
                // Set up the Configuration to read from appsettings.json
                var configuration = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory())  // Set the base directory to current folder
                 .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)  // Load appsettings.json
                 .Build();

                // Get the connection string from the configuration
                string connectionString = configuration.GetConnectionString("DefaultConnection");
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                return connectionString;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string GetAuthConnectionString()
        {
            try
            {
                // Set up the Configuration to read from appsettings.json
                var configuration = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory())  // Set the base directory to current folder
                 .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)  // Load appsettings.json
                 .Build();

                // Get the connection string from the configuration
                string connectionString = configuration.GetConnectionString("AuthContext");
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                return connectionString;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string AuthDbName()
        {
            // Set up the Configuration to read from appsettings.json
            var configuration = new ConfigurationBuilder()
             .SetBasePath(Directory.GetCurrentDirectory())  // Set the base directory to current folder
             .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)  // Load appsettings.json
             .Build();

            string connectionString = configuration.GetConnectionString("AuthContext");
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
            string authDB = builder.InitialCatalog;
            return authDB;
        }

        public static string DBName()
        {
            // Set up the Configuration to read from appsettings.json
            var configuration = new ConfigurationBuilder()
             .SetBasePath(Directory.GetCurrentDirectory())  // Set the base directory to current folder
             .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)  // Load appsettings.json
             .Build();

            string connectionString = configuration.GetConnectionString("DefaultConnection");
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
            string dnName = builder.InitialCatalog;
            return dnName;
        }

        public static string GetKey()
        {
            try
            {
                var configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                    .Build();

                string key = configuration["AppSettings:key"];
                return key;
            }
            catch (Exception ex)
            {
                throw;
            }
        }


    }

}
