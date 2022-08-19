using NLog;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Speech_Analytics.Dados
{
    public static class SSH
    {
        private static NLog.Logger NLog = LogManager.GetCurrentClassLogger();
        public static string Upload(string workingdirectory_, string uploadfile_)
        {
            /*
            string host = "104.41.42.182";//"domainna.me";
            const string username = "chucknorris";
            const string password = "norrischuck";
              string workingdirectory = workingdirectory_;//"/highway/hell";
            */
            string host = ConfigurationManager.AppSettings["ftpHost"].ToString();
            int port = Convert.ToInt16(ConfigurationManager.AppSettings["ftpPort"]);
            string username = ConfigurationManager.AppSettings["ftpUser"].ToString();
            string password = ConfigurationManager.AppSettings["ftpPassword"].ToString();

            string workingdirectory = ConfigurationManager.AppSettings["ftpDirectory"].ToString(); ;//"/highway/hell";

            string uploadfile = uploadfile_;//@"c:\yourfilegoeshere.txt";
            string status = string.Empty;
            Console.WriteLine("Creating client and connecting");
            using (var client = new SftpClient(host, port, username, password))
            {
                client.Connect();
                NLog.Info("Connected to {0}", host);

                client.ChangeDirectory(workingdirectory);
                NLog.Info("Changed directory to {0}", workingdirectory);

                //var listDirectory = client.ListDirectory(workingdirectory);
                //NLog.Info("Listing directory:");
                //foreach (var fi in listDirectory)
                //{
                //    Console.WriteLine(" - " + fi.Name);
                //    NLog.Info(" - " + fi.Name);
                //}

                using (var fileStream = new FileStream(uploadfile, FileMode.Open))
                {
                    Console.WriteLine("Uploading {0} ({1:N0} bytes)", uploadfile, fileStream.Length);
                   NLog.Info("Uploading {0} ({1:N0} bytes)", uploadfile, fileStream.Length);
                    try
                    {
                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                        client.UploadFile(fileStream, Path.GetFileName(uploadfile));
                        status = "UPLOAD OK";
                    }
                    catch (Exception UP)
                    {
                        NLog.Info("Erro" + UP.Message);
                        status = "Erro";
                    }
                  
                }

                client.Dispose();
            }
            return status;
        }
    }
}
