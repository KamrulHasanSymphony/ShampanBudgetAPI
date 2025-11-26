using Newtonsoft.Json;
using System.Net;
using System.Text;

namespace ShampanBFRSAPI.Configuration
{
    public class HttpRequestHelper
    {
        string BaseURL = "";
        string BaseURLReport = "";

        public HttpRequestHelper()
        {
            BaseURL = "http://localhost:11910/";
            BaseURLReport = "http://localhost:11910/";
        }

        public AuthModel GetAuthentication(CredentialModel credentialModel)
        {
            try
            {
                var keyValues = new Dictionary<string, string>();

                keyValues.Add("UserName", credentialModel.UserName);
                keyValues.Add("Password", credentialModel.Password);
                keyValues.Add("ApiKey", credentialModel.ApiKey);
                keyValues.Add("Grant_type", credentialModel.Grant_type);

                var result = PostFormUrlEncoded(credentialModel.PathName+"/token", new FormUrlEncodedContent(keyValues));

                return JsonConvert.DeserializeObject<AuthModel>(result);
            }
            catch (Exception e)
            {

                throw e;
            }
        }

        public string PostData(string url, AuthModel auth, string payLoad)
        {
            try
            {
                WebRequest request = (HttpWebRequest)WebRequest.Create(BaseURL + url);
                request.Method = "POST";
                request.Headers.Add("Authorization", "Bearer " + auth.Access_token);
                byte[] byteArray = Encoding.UTF8.GetBytes(payLoad);
                request.ContentLength = byteArray.Length;
                request.ContentType = "application/json";

                Stream datastream = request.GetRequestStream();
                datastream.Write(byteArray, 0, byteArray.Length);
                datastream.Close();

                WebResponse response = request.GetResponse();
                datastream = response.GetResponseStream();

                StreamReader reader = new StreamReader(datastream);
                string responseMessage = reader.ReadToEnd();

                reader.Close();

                return responseMessage;

            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public Stream PostDataReport(string url, AuthModel auth, string payLoad)
        {
            try
            {
                WebRequest request = (HttpWebRequest)WebRequest.Create( url);
                request.Method = "POST";

                byte[] byteArray = Encoding.UTF8.GetBytes(payLoad);
                request.ContentLength = byteArray.Length;
                request.ContentType = "application/json";

                Stream datastream = request.GetRequestStream();
                datastream.Write(byteArray, 0, byteArray.Length);
                datastream.Close();

                WebResponse response = request.GetResponse();
                datastream = response.GetResponseStream();
                return datastream;

            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public string PostFormUrlEncoded(string url, FormUrlEncodedContent formUrlEncodedContent)
        {
            try
            {
                WebRequest request = (HttpWebRequest)WebRequest.Create( url);
                request.Method = "POST";

                byte[] byteArray = Encoding.UTF8.GetBytes(formUrlEncodedContent.ReadAsStringAsync().Result);
                request.ContentLength = byteArray.Length;

                request.ContentType = "application/x-www-form-urlencoded";

                Stream datastream = request.GetRequestStream();
                datastream.Write(byteArray, 0, byteArray.Length);
                datastream.Close();

                WebResponse response = request.GetResponse();
                datastream = response.GetResponseStream();

                StreamReader reader = new StreamReader(datastream);

                string responseMessage = reader.ReadToEnd();

                reader.Close();

                return responseMessage;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public string GetData(string url, AuthModel auth)
        {
            try
            {
                WebRequest request = (HttpWebRequest)WebRequest.Create(BaseURL + url);
                request.Method = "GET";

                request.Headers.Add("Authorization", "Bearer " + auth.Access_token);

                WebResponse response = request.GetResponse();
                Stream datastream = response.GetResponseStream();

                StreamReader reader = new StreamReader(datastream);
                string responseMessage = reader.ReadToEnd();

                reader.Close();

                return responseMessage;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public string GetIntegrationData(string url)
        {
            try
            {
                WebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.Method = "GET";


                WebResponse response = request.GetResponse();
                Stream datastream = response.GetResponseStream();

                StreamReader reader = new StreamReader(datastream);
                string responseMessage = reader.ReadToEnd();

                reader.Close();

                return responseMessage;
            }
            catch (Exception e)
            {
                throw e;
            }
        }


        private NetworkCredential GetCredentials()
        {

            return new NetworkCredential("vatuser", "123456");

        }
        public class CredentialModel
        {
            public string UserName { get; set; }

            public string Password { get; set; }

            public string ApiKey { get; set; }

            public string Grant_type { get; set; } = "password";
            public string PathName { get; set; }
        }


        public class AuthModel
        {
            public string Access_token { get; set; }

            public string Token_type { get; set; }

            public string Expires_in { get; set; }
        }
    }

}
