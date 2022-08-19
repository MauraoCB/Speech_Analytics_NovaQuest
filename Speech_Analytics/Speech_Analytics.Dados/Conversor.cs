using NLog;
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;

namespace Speech_Analytics.Dados
{
    public class Conversor
    {
        static NLog.Logger NLog = LogManager.GetCurrentClassLogger();


        private static object loccker = new object();
        // private ILog Log;
        public string To { get; private set; }
        public string From { get; private set; }
        public string DestFolder { get; private set; }
        private bool error = false;
        public string ExecuteDate { get; private set; }


        public Conversor(string from, string to, string executeDate, string destFolder)
        {
            this.To = to;
            this.From = from;
            this.ExecuteDate = executeDate;
            this.DestFolder = destFolder;
        }

        public bool Execute()
        {

            //DateTime myDate = DateTime.ParseExact(ExecuteDate, "yyyyMMdd",
            //                           System.Globalization.CultureInfo.InvariantCulture);
            //DateTime executeDate = (DateTime)myDate;

            SendArchiveToDestinationFolder_AQM(this.DestFolder, this.From, this.To, ExecuteDate);
            return !error;
        }

        private void SendArchiveToDestinationFolder_UIP(string destinFolderName, string archive, string archiveRenamed, string executeDate)
        {
            //Log.Debug("Teste " +destinFolderName + " " + archive + " " + archiveRenamed + " " + executeDate);
            CreateDirectoryIfNotExists(destinFolderName);
            try
            {
                //Log.Debug($"Initializing procces to copy archive");
                string fromFileName = Path.Combine(archive.Replace(".wav", ".vox"));
                string destinFileName = Path.Combine(destinFolderName, archiveRenamed);

                NLog.Debug($"{fromFileName} ========> {destinFileName}");

                try
                {
                    ConvertByFFMPEG(fromFileName, destinFileName);
                }
                catch (InvalidOperationException e)
                {
                    FallbackMethodNotConvertArchive(fromFileName, destinFileName, e);
                }
                catch (Win32Exception e)
                {
                    FallbackMethodNotConvertArchive(fromFileName, destinFileName, e);
                }
                catch (FileNotFoundException e)
                {
                    error = true;
                    NLog.Error("Cannot Find File, generate in NotSent Folder Application", e);
                    try
                    {
                        destinFolderName = "NotSent";
                        destinFileName = Path.Combine(destinFolderName, archiveRenamed);
                        ConvertByFFMPEG(fromFileName, destinFileName);
                    }
                    catch (Exception)
                    {
                        FallbackMethodNotConvertArchive(fromFileName, destinFileName, e);
                    }
                }
            }
            catch (Exception e)
            {
                NLog.Error(e, "Unspected Error occours");
                error = true;
            }
        }
        private void SendArchiveToDestinationFolder_AQM(string destinFolderName, string archive, string archiveRenamed, string executeDate)
        {
            //Log.Debug("Teste " +destinFolderName + " " + archive + " " + archiveRenamed + " " + executeDate);
            CreateDirectoryIfNotExists(destinFolderName);
            try
            {
                //Log.Debug($"Initializing procces to copy archive");
                string fromFileName = Path.Combine(archive);
                string destinFileName = Path.Combine(destinFolderName, archiveRenamed);

                NLog.Debug($"{fromFileName} ========> {destinFileName}");

                try
                {
                    ConvertByFFMPEG(fromFileName, destinFileName);
                }
                catch (InvalidOperationException e)
                {
                    FallbackMethodNotConvertArchive(fromFileName, destinFileName, e);
                }
                catch (Win32Exception e)
                {
                    FallbackMethodNotConvertArchive(fromFileName, destinFileName, e);
                }
                catch (FileNotFoundException e)
                {
                    error = true;
                    NLog.Error("Cannot Find File, generate in NotSent Folder Application", e);
                    try
                    {
                        destinFolderName = "NotSent";
                        destinFileName = Path.Combine(destinFolderName, archiveRenamed);
                        ConvertByFFMPEG(fromFileName, destinFileName);
                    }
                    catch (Exception)
                    {
                        FallbackMethodNotConvertArchive(fromFileName, destinFileName, e);
                    }
                }
            }
            catch (Exception e)
            {
                NLog.Error(e, "Unspected Error occours");
                error = true;
            }
        }

        private void CreateDirectoryIfNotExists(string destinFolderName)
        {
            if (!Directory.Exists(destinFolderName))
            {
                NLog.Debug("{0} does not exists, creating ...", destinFolderName);
                Directory.CreateDirectory(destinFolderName);
            }
        }

        private void ConvertByFFMPEG(string fromFileName, string destinFileName)
        {
            lock (loccker)
            {
                //Environment.SetEnvironmentVariable("Path", @"C:\Program Files (x86)\sox-14-4-2", EnvironmentVariableTarget.User);
                NLog.Debug(@"Convertendo {0} {1}", fromFileName, destinFileName);
                Process process = Process.Start("ffmpeg", string.Format(" -i {0} -ar 8000 -acodec pcm_u8 -ac 2 {1} -y", (object)fromFileName, (object)destinFileName));
                process.StartInfo.CreateNoWindow = false;
                process.StartInfo.UseShellExecute = false;
                process.WaitForExit();

                if (File.Exists(destinFileName))
                {
                    long length = new FileInfo(destinFileName).Length;
                    if (length < 90)
                    {
                        NLog.Error("Verificar arquivo ====> " + destinFileName);
                    }

                }

            }
            NLog.Debug($"SUCCESS, {destinFileName} in folder");
        }
        private void ConvertBySOX(string fromFileName, string destinFileName)
        {
            lock (loccker)
            {
                //Environment.SetEnvironmentVariable("Path", @"C:\Program Files (x86)\sox-14-4-2", EnvironmentVariableTarget.User);
                NLog.Debug(@"Convertendo {0} {1}", fromFileName, destinFileName);
                var p = Process.Start(@"" + System.Configuration.ConfigurationManager.AppSettings["Sox"], $@" {fromFileName} {destinFileName}");
                p.StartInfo.CreateNoWindow = true;
                p.StartInfo.UseShellExecute = false;
                //p.StartInfo.CreateNoWindow = false;
                p.WaitForExit();
                NLog.Debug(@"Convertido {0} {1}", fromFileName, destinFileName);
                //NLog.Debug(@"sox -r 8k -e signed -b 8 " + "\"" + fromFileName + "\" \"" + destinFileName + "\"");

                //var startInfo = new ProcessStartInfo();
                //startInfo.FileName = @"sox";
                //startInfo.Arguments = @"-r 8k -e signed -b 8 "+"\""+fromFileName+"\" \"123"+destinFileName+"\"";
                //startInfo.WindowStyle = ProcessWindowStyle.Hidden;
                //startInfo.UseShellExecute = false;
                //startInfo.CreateNoWindow = false;
                //startInfo.WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory;
                //NLog.Debug(startInfo.WorkingDirectory);
                //using (Process soxProc = Process.Start(startInfo))
                //{
                //    soxProc.WaitForExit();
                //}
            }
            NLog.Debug($"SUCCESS, {destinFileName} in folder");
        }

        private void FallbackMethodNotConvertArchive(string fromFileName, string destinFileName, Exception e)
        {
            NLog.Error("Cannot use FFMPEG in {0}", fromFileName);
            NLog.Warn($"using Fallback Method");
            try
            {
                File.Copy(fromFileName, $"{destinFileName}_UNCONVERTED", true);
                error = false;
            }
            catch (Exception ex)
            {
                NLog.Error("Unspected Error occours {0}", ex.Message);
                NLog.Error(ex, "Unspected Error occours {0}");
                error = true;
            }
        }
    }
}
