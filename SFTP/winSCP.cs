using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WinSCP;

namespace SFTP
{
    public static class winSCP
    {

        private static NLog.Logger NLog = LogManager.GetCurrentClassLogger();
        public static bool Upload(string diretorio_, string arquivo_)
        {
            bool retorno = false;
            string dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            // Set up session options
            //SessionOptions sessionOptions = new SessionOptions
            //{
            //    Protocol = Protocol.Sftp,
            //    HostName = "104.41.42.182",
            //    PortNumber = 2222,
            //    UserName = "_SFTPPORTINHO",
            //    SshHostKeyFingerprint = "ssh-ed25519 256 CeYb/mdP7AzncYT+D/VfL92ckRmHq8B4BBY7DCu9m4I=",
            //    SshPrivateKeyPath = dir+@"\KEYS\private.key.ppk",
            //};

            // Set up session options
            SessionOptions sessionOptions = new SessionOptions
            {
                Protocol = Protocol.Sftp,
                HostName = "104.41.42.182",
                PortNumber = 2222,
                UserName = "_SFTPCRC",
                SshHostKeyFingerprint = "ssh-ed25519 256 CeYb/mdP7AzncYT+D/VfL92ckRmHq8B4BBY7DCu9m4I=",
                SshPrivateKeyPath = @"C:\crc\id_rsa.ppk",
            };

            using (Session session = new Session())
            {
                // Connect
                session.Open(sessionOptions);

                // Your code
            }


            try
            {
                //string novo_sftp = Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString()).Replace("\\","/") + "/";
                //NLog.Info("Novo diretorio:" + novo_sftp);

                using (Session session = new Session())
                {
                    // Connect
                    session.Open(sessionOptions);
                    //var teste = session.ListDirectory("/home/_sftpportinho/");

                   //if (!session.FileExists(novo_sftp))
                   // {
                   //     NLog.Info("Criando diretorio:" + DateTime.Today.Year.ToString());
                   //     session.CreateDirectory(DateTime.Today.Year.ToString());
                        
                        
                   //     if (!session.FileExists(Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString()).Replace("\\","/")))
                   //     {
                   //         NLog.Info("Criando diretorio:" + Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString()));
                   //         session.CreateDirectory(Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString()).Replace("\\", "/"));

                   //         if (!session.FileExists(Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString()).Replace("\\", "/")))
                   //         {
                   //             NLog.Info("Criando diretorio:" + Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString()));
                   //             session.CreateDirectory(Path.Combine(DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString()).Replace("\\", "/"));
                   //         }
                   //     }
                   // }
                    
                    // Upload files
                    TransferOptions transferOptions = new TransferOptions();
                    transferOptions.TransferMode = TransferMode.Binary;
                    //transferOptions.FilePermissions = new FilePermissions { Octal = "7777" };
                   
                    TransferOperationResult transferResult;
                    transferResult = session.PutFiles(@""+arquivo_, "/home/_sftpcrc/", false, transferOptions);
                    NLog.Info("Local: {0} - Remoto: {1}", arquivo_, "/home/_sftpcrc/");
                    // Throw on any error
                    transferResult.Check();

                    // Print results
                    foreach (TransferEventArgs transfer in transferResult.Transfers)
                    {
                       NLog.Info("Upload de {0} com sucesso!", transfer.FileName);
                    }

                    retorno = true;
                }

            }
            catch (Exception se)
            {
                NLog.Error("erro WINSCP: " + se.Message);
                NLog.Error(se);
                retorno = false;
            }

            return retorno;

        }
    }
}
