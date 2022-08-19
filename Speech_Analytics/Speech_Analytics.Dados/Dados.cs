
// Decompiled with JetBrains decompiler
// Type: Speech_Analytics.Dados.Dados
// Assembly: Speech_Analytics.Dados, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 717B1495-6BAB-40DB-B94D-C6D74F869A93
// Assembly location: C:\Users\roger\Desktop\temp\Speech_Analytics.Dados.dll

using NLog;
using System;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Xml;

namespace Speech_Analytics.Dados
{
    public static class Dados
    {
        private static Logger NLog = LogManager.GetCurrentClassLogger();
        private static DateTime DataInicioRotina;
        private static DateTime DataFinalRotina;

        public static void Consulta_retroativa(DateTime I, DateTime F)
        {
            string connectionString1 = ConfigurationManager.ConnectionStrings["UIP1"].ToString();
            string connectionString2 = ConfigurationManager.ConnectionStrings["UIP2"].ToString();
            string str1 = string.Empty;
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString1))
                {
                    Speech_Analytics.Dados.Dados.NLog.Info("Iniciando Consulta em Banco");
                    connection.Open();
                    string appSetting1 = ConfigurationManager.AppSettings["Application"];
                   /* string str2 = string.Empty;
                    string str3 = "\\Speech_Analytics_AQM.sql";
                    using (StreamReader streamReader = new StreamReader(appSetting1 + str3, Encoding.Default, true))
                        str2 = streamReader.ReadToEnd();
                    string str4 = str2.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + I.ToString("yyyy-MM-dd HH:mm:ss") + "'").Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + F.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                    Speech_Analytics.Dados.Dados.NLog.Info(str4);*/
                    string newValue = "#TEMP_SPEECH" + I.ToString("HHmmss");
                    //SqlCommand sqlCommand = new SqlCommand(str4.Replace("##TEMP_SPEECH", newValue), connection);

                    SqlCommand sqlCommand = new SqlCommand("SP_Speech_Analytics_AQM", connection);
                    sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
                    sqlCommand.Parameters.AddWithValue("@DT_INICIO", I.ToString("yyyy-MM-dd HH:mm:ss"));
                    sqlCommand.Parameters.AddWithValue("@DT_FINAL", F.ToString("yyyy-MM-dd HH:mm:ss"));

                    sqlCommand.CommandTimeout = 99999;
                    SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();

                    str1 = "ok";
                    long int32_1 = (long)Convert.ToInt32(new SqlCommand("SELECT COUNT(*) FROM " + newValue, connection).ExecuteScalar());
                    Console.WriteLine("Starting row count = {0}", (object)int32_1);
                    Logger nlog1 = Speech_Analytics.Dados.Dados.NLog;
                    long num1 = int32_1;
                    DateTime dateTime = I.AddHours(-1.0);
                    string str5 = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                    string str6 = F.ToString("yyyy-MM-dd HH:mm:ss");
                    nlog1.Debug<long, string, string>("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", num1, str5, str6);
                    Speech_Analytics.Dados.Dados.NLog.Info("Executo consulta");
                    string appSetting2 = ConfigurationManager.AppSettings["Destino"];
                    dateTime = DateTime.Today;
                    string path2_1 = dateTime.Year.ToString();
                    dateTime = DateTime.Today;
                    string path3 = dateTime.Month.ToString();
                    dateTime = DateTime.Today;
                    string path4 = dateTime.Day.ToString();
                    string str7 = Path.Combine(appSetting2, path2_1, path3, path4);
                    if (!Directory.Exists(str7))
                    {
                        Directory.CreateDirectory(str7);
                        Speech_Analytics.Dados.Dados.NLog.Debug("Criando Diretorio : " + str7);
                    }
                    DateTime now1 = DateTime.Now;
                    Speech_Analytics.Dados.Dados.DataInicioRotina = DateTime.Now;
                    Speech_Analytics.Dados.Dados.NLog.Info("Inicio:" + now1.ToString());
                    int num2 = 1;
                    try
                    {
                        while (sqlDataReader.Read())
                        {
                            try
                            {
                                string path1 = Path.Combine(ConfigurationManager.AppSettings["Audio1"], sqlDataReader[14].ToString().Split('-')[0], sqlDataReader[14].ToString());
                                string path2 = Path.Combine(ConfigurationManager.AppSettings["Audio2"], sqlDataReader[14].ToString().Split('-')[0], sqlDataReader[14].ToString());
                                string path5 = Path.Combine(ConfigurationManager.AppSettings["Audio3"], sqlDataReader[14].ToString().Split('-')[0], sqlDataReader[14].ToString());
                                string str8 = string.Empty;
                                if (File.Exists(path1))
                                    str8 = path1;
                                else if (File.Exists(path2))
                                    str8 = path2;
                                else if (File.Exists(path5))
                                    str8 = path5;
                                if (!File.Exists(Path.Combine(str7, sqlDataReader[14].ToString())))
                                {
                                    dateTime = DateTime.Now;
                                    string str9 = "_" + dateTime.ToString("yyyy_MM_dd_HHMMsss");
                                    string str10 = str7 ?? "";
                                    string from = str8;
                                    string to = sqlDataReader[14].ToString().Replace(".wav", "") + str9 + ".wav";
                                    dateTime = Speech_Analytics.Dados.Dados.DataInicioRotina.Date;
                                    string executeDate = dateTime.ToString("ddMMyyyy");
                                    string destFolder = str10;
                                    Conversor conversor = new Conversor(from, to, executeDate, destFolder);
                                    if (File.Exists(conversor.From))
                                    {
                                        if (conversor.Execute())
                                            Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> OK!", conversor.From);
                                        else
                                            Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> Erro!", conversor.From);
                                    }
                                    else
                                        Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> não existe origem!", conversor.From);
                                    string size1 = Speech_Analytics.Dados.Dados.GetSize(new FileInfo(str7 + "\\" + sqlDataReader[14].ToString().Replace(".wav", "") + str9 + ".wav").Length);
                                    string path2_2 = Speech_Analytics.Dados.Dados.EscreverMetadata(sqlDataReader[0], sqlDataReader[1], sqlDataReader[2], sqlDataReader[3], sqlDataReader[4], sqlDataReader[5], sqlDataReader[6], sqlDataReader[7], sqlDataReader[8], sqlDataReader[9], sqlDataReader[10], sqlDataReader[11], sqlDataReader[12], sqlDataReader[13], sqlDataReader[14], (object)size1, sqlDataReader[16], sqlDataReader[17], sqlDataReader[18], sqlDataReader[14].ToString().Replace(".wav", "") + str9);
                                    string size2 = Speech_Analytics.Dados.Dados.GetSize(new FileInfo(str7 + "\\" + path2_2).Length);
                                    string inputFilePath1 = Path.Combine(str7, sqlDataReader[14].ToString().Replace(".wav", "") + str9 + ".wav");
                                    string inputFilePath2 = Path.Combine(str7, path2_2);
                                    try
                                    {
                                        //18/08/2022 - Início
                                        //O nome do novo arquivo será o ID da gravação + _1.wav
                                        FileInfo fi = new FileInfo(inputFilePath1);
                                        string inputFilePath1_1wav = fi.FullName.Replace(fi.Name, sqlDataReader[22].ToString() + "_1");
                                        string fileCsv = inputFilePath1_1wav.Replace("wav", "csv");

                                        Speech_Analytics.Dados.Dados.Copy(inputFilePath1, inputFilePath1_1wav);

                                        FileStream streamCsv = fi.Open(FileMode.Append);

                                        using (StreamWriter writerCsv = new StreamWriter(streamCsv))
                                        {
                                            string csvLine = "";
                                            for (int i = 0; i < sqlDataReader.FieldCount -1; i++)
                                            {
                                                csvLine += sqlDataReader[0] + ",";
                                            }

                                            csvLine = csvLine.Remove(csvLine.Length - 1);

                                            writerCsv.WriteLine(csvLine);

                                        }
                                        streamCsv.Close();

                                        SSH.Upload(fi.DirectoryName, inputFilePath1_1wav);
                                        SSH.Upload(fi.DirectoryName, fileCsv);
                                        //18/08/2022 - Fim

                                        Speech_Analytics.Dados.Dados.Copy(inputFilePath1, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], conversor.To));
                                        string str11 = sqlDataReader[14].ToString().Replace(".wav", "") + str9 + ".wav";
                                        dateTime = DateTime.Now;
                                        string str12 = dateTime.ToString("dd/MM/yyyy HH:mm:ss");
                                        string str13 = size1;
                                        Speech_Analytics.Dados.Dados.EscreverBastao(string.Format("{0},{1},{2}", (object)str11, (object)str12, (object)str13));
                                        Speech_Analytics.Dados.Dados.Copy(inputFilePath2, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], path2_2));
                                        string str14 = path2_2;
                                        dateTime = DateTime.Now;
                                        string str15 = dateTime.ToString("dd/MM/yyyy HH:mm:ss");
                                        string str16 = size2;
                                        Speech_Analytics.Dados.Dados.EscreverBastao(string.Format("{0},{1},{2}", (object)str14, (object)str15, (object)str16));
                                        ++num2;
                                        Speech_Analytics.Dados.Dados.NLog.Info("Cliente Transferiu audio e xml OK!");
                                    }
                                    catch (Exception ex)
                                    {
                                        Speech_Analytics.Dados.Dados.NLog.Info("ERRO:" + ex.Message);
                                    }
                                }
                                else
                                    Speech_Analytics.Dados.Dados.NLog.Error("ERRO existe:" + File.Exists(Path.Combine(str7, sqlDataReader[14].ToString())).ToString());
                            }
                            catch (Exception ex)
                            {
                                Speech_Analytics.Dados.Dados.NLog.Error<Exception>(ex);
                            }
                        }
                        Speech_Analytics.Dados.Dados.NLog.Info<int, long>("Quantidade Enviados {0} arquivos de {1}", num2, int32_1);
                        int num3;
                        if (File.Exists(str7 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"))
                        {
                            dateTime = DateTime.Now;
                            num3 = dateTime.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]) ? 1 : 0;
                        }
                        else
                            num3 = 0;
                        if (num3 != 0)
                        {
                            Speech_Analytics.Dados.Dados.Copy(str7 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], "bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"));
                            Speech_Analytics.Dados.Dados.NLog.Info("arquivo bastao - {0}", str7 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
                        }
                        DateTime now2 = DateTime.Now;
                        Speech_Analytics.Dados.Dados.DataFinalRotina = DateTime.Now;
                        TimeSpan timeSpan = now2 - now1;
                        Logger nlog2 = Speech_Analytics.Dados.Dados.NLog;
                        dateTime = DateTime.Now;
                        string str17 = "Fim:" + dateTime.ToString();
                        nlog2.Info(str17);
                        Speech_Analytics.Dados.Dados.NLog.Info("Time:" + timeSpan.ToString());
                    }
                    catch (Exception ex)
                    {
                        Speech_Analytics.Dados.Dados.NLog.Info(ex.Message);
                        using (XmlWriter xmlWriter = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + now1.ToString("ddMMyyyy_HH") + ".xml"))
                        {
                            xmlWriter.WriteStartDocument();
                            xmlWriter.WriteStartElement("ALM");
                            while (sqlDataReader.Read())
                            {
                                xmlWriter.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));
                                for (int ordinal = 0; ordinal < sqlDataReader.FieldCount; ++ordinal)
                                {
                                    xmlWriter.WriteStartElement(sqlDataReader.GetName(ordinal));
                                    xmlWriter.WriteValue(sqlDataReader[ordinal].ToString());
                                    xmlWriter.WriteEndElement();
                                }
                                xmlWriter.WriteEndElement();
                            }
                            xmlWriter.WriteEndElement();
                            xmlWriter.WriteEndDocument();
                        }
                    }
                    long int32_2 = (long)Convert.ToInt32(new SqlCommand("DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT SET @dataIni_ = '" + now1.ToString("yyyy.MM.dd 01:00:00") + "'; SET @dataFim_ = '" + now1.ToString("yyyy.MM.dd 23:59:59") + "'; SET @TZ_ = 3 SELECT count(*) FROM " + newValue + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ", connection).ExecuteScalar());
                    Speech_Analytics.Dados.Dados.NLog.Info("Ending row count = {0}", int32_2);
                    Speech_Analytics.Dados.Dados.NLog.Info("{0} rows were added.", int32_2 - int32_1);
                }
            }
            catch (Exception ex)
            {
                str1 = (string)null;
                Speech_Analytics.Dados.Dados.NLog.Error("Não foi possível conectar no banco primário [UIP], indo pro secundário");
            }
            if (str1 != "ok")
            {
                using (SqlConnection connection = new SqlConnection(connectionString2))
                {
                    Speech_Analytics.Dados.Dados.NLog.Info("Iniciando Consulta em Banco secundário [UIP2]");
                    connection.Open();
                    string appSetting3 = ConfigurationManager.AppSettings["Application"];
                    /*string str18 = string.Empty;
                    string str19 = "\\Speech_Analytics_AQM.sql";
                    using (StreamReader streamReader = new StreamReader(appSetting3 + str19, Encoding.Default, true))
                        str18 = streamReader.ReadToEnd();
                    string str20 = str18.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + I.ToString("yyyy-MM-dd HH:mm:ss") + "'").Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + F.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                    Speech_Analytics.Dados.Dados.NLog.Info(str20);*/
                    string newValue = "#TEMP_SPEECH" + I.ToString("HHmmss");
                    //SqlCommand sqlCommand = new SqlCommand(str20.Replace("##TEMP_SPEECH", newValue), connection);

                    SqlCommand sqlCommand = new SqlCommand("SP_Speech_Analytics_AQM", connection);
                    sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
                    sqlCommand.Parameters.AddWithValue("@DT_INICIO", I.ToString("yyyy-MM-dd HH:mm:ss"));
                    sqlCommand.Parameters.AddWithValue("@DT_FINAL", F.ToString("yyyy-MM-dd HH:mm:ss"));

                    sqlCommand.CommandTimeout = 99999;
                    SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                    long int32_3 = (long)Convert.ToInt32(new SqlCommand("SELECT COUNT(*) FROM " + newValue, connection).ExecuteScalar());
                    Console.WriteLine("Starting row count = {0}", (object)int32_3);
                    Logger nlog3 = Speech_Analytics.Dados.Dados.NLog;
                    long num4 = int32_3;
                    DateTime dateTime = I.AddHours(-1.0);
                    string str21 = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                    string str22 = F.ToString("yyyy-MM-dd HH:mm:ss");
                    nlog3.Debug<long, string, string>("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", num4, str21, str22);
                    Speech_Analytics.Dados.Dados.NLog.Info("Executo consulta");
                    string appSetting4 = ConfigurationManager.AppSettings["Destino"];
                    dateTime = DateTime.Today;
                    string path2_3 = dateTime.Year.ToString();
                    dateTime = DateTime.Today;
                    string path3 = dateTime.Month.ToString();
                    dateTime = DateTime.Today;
                    string path4 = dateTime.Day.ToString();
                    string str23 = Path.Combine(appSetting4, path2_3, path3, path4);
                    if (!Directory.Exists(str23))
                    {
                        Directory.CreateDirectory(str23);
                        Speech_Analytics.Dados.Dados.NLog.Debug("Criando Diretorio : " + str23);
                    }
                    DateTime now3 = DateTime.Now;
                    Speech_Analytics.Dados.Dados.DataInicioRotina = DateTime.Now;
                    Speech_Analytics.Dados.Dados.NLog.Info("Inicio:" + now3.ToString());
                    int num5 = 1;
                    try
                    {
                        while (sqlDataReader.Read())
                        {
                            try
                            {
                                string path6 = Path.Combine(ConfigurationManager.AppSettings["Audio1"], sqlDataReader[14].ToString().Split('-')[0], sqlDataReader[14].ToString());
                                string path7 = Path.Combine(ConfigurationManager.AppSettings["Audio2"], sqlDataReader[14].ToString().Split('-')[0], sqlDataReader[14].ToString());
                                string path8 = Path.Combine(ConfigurationManager.AppSettings["Audio3"], sqlDataReader[14].ToString().Split('-')[0], sqlDataReader[14].ToString());
                                string str24 = string.Empty;
                                if (File.Exists(path6))
                                    str24 = path6;
                                else if (File.Exists(path7))
                                    str24 = path7;
                                else if (File.Exists(path8))
                                    str24 = path8;
                                if (!File.Exists(Path.Combine(str23, sqlDataReader[14].ToString())))
                                {
                                    dateTime = DateTime.Now;
                                    string str25 = "_" + dateTime.ToString("yyyy_MM_dd_HHMMsss");
                                    string str26 = str23 ?? "";
                                    string from = str24;
                                    string to = sqlDataReader[14].ToString().Replace(".wav", "") + str25 + ".wav";
                                    dateTime = Speech_Analytics.Dados.Dados.DataInicioRotina.Date;
                                    string executeDate = dateTime.ToString("ddMMyyyy");
                                    string destFolder = str26;
                                    Conversor conversor = new Conversor(from, to, executeDate, destFolder);
                                    if (File.Exists(conversor.From))
                                    {
                                        if (conversor.Execute())
                                            Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> OK!", conversor.From);
                                        else
                                            Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> Erro!", conversor.From);
                                    }
                                    else
                                        Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> não existe origem!", conversor.From);
                                    string size3 = Speech_Analytics.Dados.Dados.GetSize(new FileInfo(str23 + "\\" + sqlDataReader[14].ToString().Replace(".wav", "") + str25 + ".wav").Length);
                                    string path2_4 = Speech_Analytics.Dados.Dados.EscreverMetadata(sqlDataReader[0], sqlDataReader[1], sqlDataReader[2], sqlDataReader[3], sqlDataReader[4], sqlDataReader[5], sqlDataReader[6], sqlDataReader[7], sqlDataReader[8], sqlDataReader[9], sqlDataReader[10], sqlDataReader[11], sqlDataReader[12], sqlDataReader[13], sqlDataReader[14], (object)size3, sqlDataReader[16], sqlDataReader[17], sqlDataReader[18], sqlDataReader[14].ToString().Replace(".wav", "") + str25);
                                    string size4 = Speech_Analytics.Dados.Dados.GetSize(new FileInfo(str23 + "\\" + path2_4).Length);
                                    string inputFilePath3 = Path.Combine(str23, sqlDataReader[14].ToString().Replace(".wav", "") + str25 + ".wav");
                                    string inputFilePath4 = Path.Combine(str23, path2_4);
                                    try
                                    {
                                        Speech_Analytics.Dados.Dados.Copy(inputFilePath3, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], conversor.To));
                                        string str27 = sqlDataReader[14].ToString().Replace(".wav", "") + str25 + ".wav";
                                        dateTime = DateTime.Now;
                                        string str28 = dateTime.ToString("dd/MM/yyyy HH:mm:ss");
                                        string str29 = size3;
                                        Speech_Analytics.Dados.Dados.EscreverBastao(string.Format("{0},{1},{2}", (object)str27, (object)str28, (object)str29));
                                        Speech_Analytics.Dados.Dados.Copy(inputFilePath4, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], path2_4));
                                        string str30 = path2_4;
                                        dateTime = DateTime.Now;
                                        string str31 = dateTime.ToString("dd/MM/yyyy HH:mm:ss");
                                        string str32 = size4;
                                        Speech_Analytics.Dados.Dados.EscreverBastao(string.Format("{0},{1},{2}", (object)str30, (object)str31, (object)str32));
                                        ++num5;
                                        Speech_Analytics.Dados.Dados.NLog.Info("Cliente Transferiu audio e xml OK!");
                                    }
                                    catch (Exception ex)
                                    {
                                        Speech_Analytics.Dados.Dados.NLog.Info("ERRO:" + ex.Message);
                                    }
                                }
                                else
                                    Speech_Analytics.Dados.Dados.NLog.Error("ERRO existe:" + File.Exists(Path.Combine(str23, sqlDataReader[14].ToString())).ToString());
                            }
                            catch (Exception ex)
                            {
                                Speech_Analytics.Dados.Dados.NLog.Error<Exception>(ex);
                            }
                        }
                        Speech_Analytics.Dados.Dados.NLog.Info<int, long>("Quantidade Enviados {0} arquivos de {1}", num5, int32_3);
                        int num6;
                        if (File.Exists(str23 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"))
                        {
                            dateTime = DateTime.Now;
                            num6 = dateTime.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]) ? 1 : 0;
                        }
                        else
                            num6 = 0;
                        if (num6 != 0)
                        {
                            Speech_Analytics.Dados.Dados.Copy(str23 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], "bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"));
                            Speech_Analytics.Dados.Dados.NLog.Info("arquivo bastao - {0}", str23 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
                        }
                        DateTime now4 = DateTime.Now;
                        Speech_Analytics.Dados.Dados.DataFinalRotina = DateTime.Now;
                        TimeSpan timeSpan = now4 - now3;
                        Logger nlog4 = Speech_Analytics.Dados.Dados.NLog;
                        dateTime = DateTime.Now;
                        string str33 = "Fim:" + dateTime.ToString();
                        nlog4.Info(str33);
                        Speech_Analytics.Dados.Dados.NLog.Info("Time:" + timeSpan.ToString());
                    }
                    catch (Exception ex)
                    {
                        Speech_Analytics.Dados.Dados.NLog.Info(ex.Message);
                        using (XmlWriter xmlWriter = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + now3.ToString("ddMMyyyy_HH") + ".xml"))
                        {
                            xmlWriter.WriteStartDocument();
                            xmlWriter.WriteStartElement("ALM");
                            while (sqlDataReader.Read())
                            {
                                xmlWriter.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));
                                for (int ordinal = 0; ordinal < sqlDataReader.FieldCount; ++ordinal)
                                {
                                    xmlWriter.WriteStartElement(sqlDataReader.GetName(ordinal));
                                    xmlWriter.WriteValue(sqlDataReader[ordinal].ToString());
                                    xmlWriter.WriteEndElement();
                                }
                                xmlWriter.WriteEndElement();
                            }
                            xmlWriter.WriteEndElement();
                            xmlWriter.WriteEndDocument();
                        }
                    }
                    long int32_4 = (long)Convert.ToInt32(new SqlCommand("DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT SET @dataIni_ = '" + now3.ToString("yyyy.MM.dd 01:00:00") + "'; SET @dataFim_ = '" + now3.ToString("yyyy.MM.dd 23:59:59") + "'; SET @TZ_ = 3 SELECT count(*) FROM " + newValue + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ", connection).ExecuteScalar());
                    Speech_Analytics.Dados.Dados.NLog.Info("Ending row count = {0}", int32_4);
                    Speech_Analytics.Dados.Dados.NLog.Info("{0} rows were added.", int32_4 - int32_3);
                }
            }
            DirectoryInfo directoryInfo = new DirectoryInfo(ConfigurationManager.AppSettings["Destino"]);
            Speech_Analytics.Dados.Dados.NLog.Info("total deletar {0}", directoryInfo.GetFiles().Length);
            foreach (FileInfo file in directoryInfo.GetFiles())
            {
                if (file.LastWriteTime < DateTime.Today.AddDays(-2.0))
                    file.Delete();
            }
        }
        public static void Consulta(DateTime h_atual)
        {
            using (SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings["UIP"].ToString()))
            {
                connection.Open();
                string appSetting1 = ConfigurationManager.AppSettings["Application"];
                string str1 = string.Empty;
                string str2 = "\\Speech_Analytics_AQM.sql";
                using (StreamReader streamReader = new StreamReader(appSetting1 + str2, Encoding.Default, true))
                    str1 = streamReader.ReadToEnd();
                string str3 = str1.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + h_atual.AddHours(-1.0).ToString("yyyy-MM-dd HH:mm:ss") + "'").Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + h_atual.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Speech_Analytics.Dados.Dados.NLog.Info(str3);
                string newValue = "#TEMP_SPEECH" + h_atual.ToString("HHmmss");
                SqlCommand sqlCommand = new SqlCommand(str3.Replace("##TEMP_SPEECH", newValue), connection);
                sqlCommand.CommandTimeout = 99999;
                SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                long int32_1 = (long)Convert.ToInt32(new SqlCommand("SELECT COUNT(*) FROM " + newValue, connection).ExecuteScalar());
                Console.WriteLine("Starting row count = {0}", (object)int32_1);
                Logger nlog1 = Speech_Analytics.Dados.Dados.NLog;
                long num1 = int32_1;
                DateTime dateTime = h_atual.AddHours(-1.0);
                string str4 = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                string str5 = h_atual.ToString("yyyy-MM-dd HH:mm:ss");
                nlog1.Debug<long, string, string>("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", num1, str4, str5);
                Speech_Analytics.Dados.Dados.NLog.Info("Executo consulta");
                string appSetting2 = ConfigurationManager.AppSettings["Destino"];
                dateTime = DateTime.Today;
                string path2_1 = dateTime.Year.ToString();
                dateTime = DateTime.Today;
                int num2 = dateTime.Month;
                string path3 = num2.ToString();
                dateTime = DateTime.Today;
                num2 = dateTime.Day;
                string path4 = num2.ToString();
                string str6 = Path.Combine(appSetting2, path2_1, path3, path4);
                if (!Directory.Exists(str6))
                {
                    Directory.CreateDirectory(str6);
                    Speech_Analytics.Dados.Dados.NLog.Debug("Criando Diretorio : " + str6);
                }
                DateTime now1 = DateTime.Now;
                Speech_Analytics.Dados.Dados.DataInicioRotina = DateTime.Now;
                Speech_Analytics.Dados.Dados.NLog.Info("Inicio:" + now1.ToString());
                int num3 = 0;
                try
                {
                    while (sqlDataReader.Read())
                    {
                        try
                        {
                            string inputFilePath1 = Path.Combine(ConfigurationManager.AppSettings["Audios"], sqlDataReader[14].ToString().Replace(".wav", ".vox"));
                            if (!File.Exists(Path.Combine(str6, sqlDataReader[14].ToString())))
                            {
                                dateTime = DateTime.Now;
                                string str7 = "_" + dateTime.ToString("yyyy_MM_dd_HHMMsss");
                                Speech_Analytics.Dados.Dados.Copy(inputFilePath1, Path.Combine(str6, sqlDataReader[14].ToString().Replace(".wav", "") + str7 + ".vox"));
                                string str8 = str6 ?? "";
                                string from = str6 + "\\" + sqlDataReader[14].ToString().Replace(".wav", "") + str7 + ".wav";
                                string to = sqlDataReader[14].ToString().Replace(".wav", "") + str7 + ".wav";
                                dateTime = Speech_Analytics.Dados.Dados.DataInicioRotina.Date;
                                string executeDate = dateTime.ToString("ddMMyyyy");
                                string destFolder = str8;
                                Conversor conversor = new Conversor(from, to, executeDate, destFolder);
                                if (!File.Exists(conversor.From))
                                {
                                    Speech_Analytics.Dados.Dados.NLog.Debug("arquivo existe {0} ", conversor.From.Replace(".vox", ".wav"));
                                    if (conversor.Execute())
                                        Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> OK!", conversor.From);
                                    else
                                        Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> Erro!", conversor.From);
                                }
                                else
                                    Speech_Analytics.Dados.Dados.NLog.Debug("{0} ========> não existe origem!", conversor.From);
                                string size = Speech_Analytics.Dados.Dados.GetSize(new FileInfo(str6 + "\\" + sqlDataReader[14].ToString().Replace(".wav", "") + str7 + ".wav").Length);
                                string path2_2 = Speech_Analytics.Dados.Dados.EscreverMetadata(sqlDataReader[0], sqlDataReader[1], sqlDataReader[2], sqlDataReader[3], sqlDataReader[4], sqlDataReader[5], sqlDataReader[6], sqlDataReader[7], sqlDataReader[8], sqlDataReader[9], sqlDataReader[10], sqlDataReader[11], sqlDataReader[12], sqlDataReader[13], sqlDataReader[14], (object)size, sqlDataReader[16], sqlDataReader[17], sqlDataReader[18], sqlDataReader[14].ToString().Replace(".wav", "") + str7);
                                Speech_Analytics.Dados.Dados.GetSize(new FileInfo(str6 + "\\" + path2_2).Length);
                                string inputFilePath2 = Path.Combine(str6, sqlDataReader[14].ToString().Replace(".wav", "") + str7 + ".wav");
                                string inputFilePath3 = Path.Combine(str6, path2_2);
                                try
                                {
                                    Speech_Analytics.Dados.Dados.Copy(inputFilePath2, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], conversor.To));
                                    Speech_Analytics.Dados.Dados.Copy(inputFilePath3, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], path2_2));
                                    ++num3;
                                    Speech_Analytics.Dados.Dados.NLog.Info("Cliente Transferiu audio e xml OK!");
                                }
                                catch (Exception ex)
                                {
                                    Speech_Analytics.Dados.Dados.NLog.Info("ERRO:" + ex.Message);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Speech_Analytics.Dados.Dados.NLog.Error<Exception>(ex);
                        }
                    }
                    Speech_Analytics.Dados.Dados.NLog.Info<int, long>("Quantidade Enviados {0} arquivos de {1}", num3, int32_1);
                    int num4;
                    if (File.Exists(str6 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"))
                    {
                        dateTime = DateTime.Now;
                        num4 = dateTime.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]) ? 1 : 0;
                    }
                    else
                        num4 = 0;
                    if (num4 != 0)
                        Speech_Analytics.Dados.Dados.NLog.Info("arquivo bastao - {0}", str6 + "\\bastao_" + Speech_Analytics.Dados.Dados.DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
                    DateTime now2 = DateTime.Now;
                    Speech_Analytics.Dados.Dados.DataFinalRotina = DateTime.Now;
                    TimeSpan timeSpan = now2 - now1;
                    Logger nlog2 = Speech_Analytics.Dados.Dados.NLog;
                    dateTime = DateTime.Now;
                    string str9 = "Fim:" + dateTime.ToString();
                    nlog2.Info(str9);
                    Speech_Analytics.Dados.Dados.NLog.Info("Time:" + timeSpan.ToString());
                }
                catch (Exception ex)
                {
                    Speech_Analytics.Dados.Dados.NLog.Info(ex.Message);
                    using (XmlWriter xmlWriter = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + now1.ToString("ddMMyyyy_HH") + ".xml"))
                    {
                        xmlWriter.WriteStartDocument();
                        xmlWriter.WriteStartElement("ALM");
                        while (sqlDataReader.Read())
                        {
                            xmlWriter.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));
                            for (int ordinal = 0; ordinal < sqlDataReader.FieldCount; ++ordinal)
                            {
                                xmlWriter.WriteStartElement(sqlDataReader.GetName(ordinal));
                                xmlWriter.WriteValue(sqlDataReader[ordinal].ToString());
                                xmlWriter.WriteEndElement();
                            }
                            xmlWriter.WriteEndElement();
                        }
                        xmlWriter.WriteEndElement();
                        xmlWriter.WriteEndDocument();
                    }
                }
                long int32_2 = (long)Convert.ToInt32(new SqlCommand("DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT SET @dataIni_ = '" + now1.ToString("yyyy.MM.dd 01:00:00") + "'; SET @dataFim_ = '" + now1.ToString("yyyy.MM.dd 23:59:59") + "'; SET @TZ_ = 3 SELECT count(*) FROM " + newValue + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ", connection).ExecuteScalar());
                Speech_Analytics.Dados.Dados.NLog.Info("Ending row count = {0}", int32_2);
                Speech_Analytics.Dados.Dados.NLog.Info("{0} rows were added.", int32_2 - int32_1);
            }
            DirectoryInfo directoryInfo = new DirectoryInfo(ConfigurationManager.AppSettings["Destino"]);
            Speech_Analytics.Dados.Dados.NLog.Info("total deletar {0}", directoryInfo.GetFiles().Length);
            foreach (FileInfo file in directoryInfo.GetFiles())
            {
                if (file.LastWriteTime < DateTime.Today.AddDays(-2.0))
                    file.Delete();
            }
        }
       
        public static void ConsultaAQM(DateTime h_atual)
        {
            string connectionString1 = ConfigurationManager.ConnectionStrings["UIP1"].ToString();
            string connectionString2 = ConfigurationManager.ConnectionStrings["UIP2"].ToString();
            string str1 = string.Empty;
            // Open a sourceConnection to the AdventureWorks database.

            try
            {
                using (SqlConnection sourceConnection = new SqlConnection(connectionString1))
                {
                    sourceConnection.Open();

                    //VERIFICAR NAO PROCESSADOS

                    NLog.Info("Iniciando Consulta em Banco secundário [UIP1]");

                    //CONSUL
                    #region CONSULTA DADOS

                    string dir = ConfigurationManager.AppSettings["Application"];
                    string sql = string.Empty;
                    string query = @"\Speech_Analytics_AQM.sql";

                    using (StreamReader readerFile = new StreamReader(dir + query, System.Text.Encoding.Default, true))
                    {
                        sql = readerFile.ReadToEnd();
                    }

                    string sql_ = sql.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + h_atual.AddMinutes(-15).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                    sql_ = sql_.Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + h_atual.ToString("yyyy-MM-dd HH:mm:ss") + "'");

                    NLog.Info(sql_);
                    var temporaria = "#TEMP_SPEECH" + h_atual.ToString("HHmmss");
                    sql_ = sql_.Replace("##TEMP_SPEECH", temporaria);
                    // Get data from the source table as a SqlDataReader.
                    SqlCommand commandSourceData = new SqlCommand(sql_, sourceConnection);
                    commandSourceData.CommandTimeout = 99999;

                    SqlDataReader reader =
                        commandSourceData.ExecuteReader();

                    str1 = "ok";
                    // Perform an initial count on the destination table.
                    SqlCommand commandRowCount = new SqlCommand(
                        "SELECT COUNT(*) FROM " + temporaria,
                        sourceConnection);

                    long countStart = System.Convert.ToInt32(
                        commandRowCount.ExecuteScalar());
                    Console.WriteLine("Starting row count = {0}", countStart);

                    NLog.Debug("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", countStart, h_atual.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss"), h_atual.ToString("yyyy-MM-dd HH:mm:ss"));

                    NLog.Info("Executo consulta");

                    string diretorio_novo = Path.Combine(ConfigurationManager.AppSettings["Destino"], DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString());
                    if (!Directory.Exists(diretorio_novo))
                    {
                        Directory.CreateDirectory(diretorio_novo);
                        NLog.Debug("Criando Diretorio : " + diretorio_novo);
                    }

                    //xml  
                    var inicio = DateTime.Now;
                    DataInicioRotina = DateTime.Now;
                    NLog.Info("Inicio:" + inicio.ToString());
                    int contat = 1;
                    try
                    {
                        // Write from the source to the destination.
                        while (reader.Read())
                        {

                            try
                            {
                                // string audio = Path.Combine(ConfigurationManager.AppSettings["Audios"], reader[14].ToString())
                                string audio1 = Path.Combine(ConfigurationManager.AppSettings["Audio1"], reader[14].ToString().Split('-')[0], reader[14].ToString());
                                string audio2 = Path.Combine(ConfigurationManager.AppSettings["Audio2"], reader[14].ToString().Split('-')[0], reader[14].ToString());
                                string audio3 = Path.Combine(ConfigurationManager.AppSettings["Audio3"], reader[14].ToString().Split('-')[0], reader[14].ToString());

                                var audioCurrent = string.Empty;

                                if (File.Exists(audio1))
                                {
                                    audioCurrent = audio1;
                                }
                                else if (File.Exists(audio2))
                                {
                                    audioCurrent = audio2;
                                }
                                else if (File.Exists(audio3))
                                {
                                    audioCurrent = audio3;
                                }






                                if (!File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())))
                                {
                                    //audio

                                    var nomenclatura_dt = "_" + DateTime.Now.ToString("yyyy_MM_dd_HHMMsss");
                                    // Copy(audio, Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav"));
                                    string Destino = @"" + diretorio_novo;
                                    Conversor convert = new Conversor(audioCurrent,
                                                                      // diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
                                                                      reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
                                                                      DataInicioRotina.Date.ToString("ddMMyyyy"),//datetime
                                                                      Destino
                                                                      );

                                    if (System.IO.File.Exists(convert.From))
                                    {
                                        // Copy(audioCurrent, Path.Combine(convert.DestFolder, convert.To));



                                        //NLog.Debug("Copiar {0} para {1} ", audioCurrent, Path.Combine(convert.DestFolder, convert.To));

                                        //if (File.Exists(Path.Combine(convert.DestFolder, convert.To)))
                                        //{
                                        //    NLog.Debug("Arquivo copiado com sucesso! " + Path.Combine(convert.DestFolder, convert.To));
                                        //}
                                        if (convert.Execute())
                                        {
                                            NLog.Debug("{0} ========> OK!", convert.From);
                                        }
                                        else
                                        {
                                            NLog.Debug("{0} ========> Erro!", convert.From);
                                        }
                                        //NLog.Debug("arquivo existe {0} ", convert.From.Replace(".vox", ".wav"));
                                        //if (convert.Execute())
                                        //{
                                        //    NLog.Debug("{0} ========> OK!", convert.From);
                                        //}
                                        //else
                                        //{
                                        //    NLog.Debug("{0} ========> Erro!", convert.From);
                                        //}
                                    }
                                    else
                                    {
                                        NLog.Debug("{0} ========> não existe origem!", convert.From);
                                    }
                                    long length_audio = new System.IO.FileInfo(diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav").Length;
                                    string arqTm_audio = GetSize(length_audio);

                                    //xml
                                    string arqXml = EscreverMetadata(reader[0], reader[1], reader[2], reader[3], reader[4], reader[5], reader[6], reader[7],
                                            reader[8], reader[9], reader[10], reader[11], reader[12], reader[13], reader[14], arqTm_audio, reader[16], reader[17], reader[18], reader[14].ToString().Replace(".wav", "") + nomenclatura_dt);

                                    long length = new System.IO.FileInfo(diretorio_novo + "\\" + arqXml).Length;
                                    string arqTm = GetSize(length);
                                    //enviar dados

                                    string Audio_local = Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav");

                                    string Xml_local = Path.Combine(diretorio_novo, arqXml);
                                    try
                                    {
                                        Copy(Audio_local, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], convert.To));
                                        EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
                                        Copy(Xml_local, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], arqXml));
                                        EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
                                        contat = contat + 1;
                                        //  SSH.Upload("", "");
                                        // if(winSCP.Upload(diretorio_novo, Audio_local))
                                        //if (true)
                                        //{
                                        //     //EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
                                        //     //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio audio " + Audio_local);

                                        //  //   if (winSCP.Upload(diretorio_novo, Xml_local))
                                        //     if (true)
                                        //     {
                                        //         //escrever bastao
                                        //        // EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
                                        //         //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio xml " + Xml_local);
                                        //     }
                                        //}




                                        NLog.Info("Cliente Transferiu audio e xml OK!");

                                    }
                                    catch (Exception ES)
                                    {
                                        NLog.Info("ERRO:" + ES.Message);
                                    }
                                }
                                else
                                {
                                    NLog.Error("ERRO existe:" + File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())));
                                }

                            }
                            catch (Exception errowhile)
                            {

                                NLog.Error(errowhile);
                            }

                        }


                        //
                        NLog.Info("Quantidade Enviados {0} arquivos de {1}", contat, countStart);
                        if (File.Exists(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT") && DateTime.Now.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]))
                        {
                            // winSCP.Upload(diretorio_novo, diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");
                            Copy(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], "bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"));
                            NLog.Info("arquivo bastao - {0}", diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
                        }


                        var final = DateTime.Now;
                        DataFinalRotina = DateTime.Now;
                        var final_cont = final - inicio;
                        NLog.Info("Fim:" + DateTime.Now.ToString());
                        NLog.Info("Time:" + final_cont);
                    }
                    catch (Exception ex)
                    {
                        NLog.Info(ex.Message);
                        using (XmlWriter writer_ = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + inicio.ToString("ddMMyyyy_HH") + ".xml"))
                        {
                            writer_.WriteStartDocument();
                            writer_.WriteStartElement("ALM");

                            while (reader.Read())
                            {
                                writer_.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));

                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    writer_.WriteStartElement(reader.GetName(i));
                                    writer_.WriteValue(reader[i].ToString());
                                    writer_.WriteEndElement();
                                }

                                writer_.WriteEndElement();
                            }

                            writer_.WriteEndElement();
                            writer_.WriteEndDocument();
                        }
                    }



                    #endregion

                    string validar = "DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT " +
                                      "SET @dataIni_ = '" + inicio.ToString("yyyy.MM.dd 01:00:00") + "'; " +
                                      "SET @dataFim_ = '" + inicio.ToString("yyyy.MM.dd 23:59:59") + "'; " +
                                      "SET @TZ_ = 3 " +
                                      "SELECT count(*) " +
                                      "FROM " + temporaria + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ";

                    SqlCommand commandRowCount_ = new SqlCommand(
                    validar,
                    sourceConnection);

                    // Perform a final count on the destination 
                    // table to see how many rows were added.
                    long countEnd = System.Convert.ToInt32(
                        commandRowCount_.ExecuteScalar());
                    NLog.Info("Ending row count = {0}", countEnd);
                    NLog.Info("{0} rows were added.", countEnd - countStart);
                    //NLog.Info("Press Enter to finish.");
                    //Console.ReadLine();


                }

            }
            catch (Exception)
            {

                str1 = string.Empty;
                NLog.Error("Não foi possível conectar no banco primário [UIP], indo pro secundário");
            }

            if (str1 != "ok")
            {
                using (SqlConnection sourceConnection = new SqlConnection(connectionString2))
                {
                    sourceConnection.Open();

                    //VERIFICAR NAO PROCESSADOS

                    NLog.Info("Iniciando Consulta em Banco secundário [UIP2]");

                    //CONSUL
                    #region CONSULTA DADOS

                    string dir = ConfigurationManager.AppSettings["Application"];
                    string sql = string.Empty;
                    string query = @"\Speech_Analytics_AQM.sql";

                    using (StreamReader readerFile = new StreamReader(dir + query, System.Text.Encoding.Default, true))
                    {
                        sql = readerFile.ReadToEnd();
                    }

                    string sql_ = sql.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + h_atual.AddMinutes(-15).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                    sql_ = sql_.Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + h_atual.ToString("yyyy-MM-dd HH:mm:ss") + "'");

                    NLog.Info(sql_);
                    var temporaria = "#TEMP_SPEECH" + h_atual.ToString("HHmmss");
                    sql_ = sql_.Replace("##TEMP_SPEECH", temporaria);
                    // Get data from the source table as a SqlDataReader.
                    SqlCommand commandSourceData = new SqlCommand(sql_, sourceConnection);
                    commandSourceData.CommandTimeout = 99999;

                    SqlDataReader reader =
                        commandSourceData.ExecuteReader();

                    str1 = "ok";
                    // Perform an initial count on the destination table.
                    SqlCommand commandRowCount = new SqlCommand(
                        "SELECT COUNT(*) FROM " + temporaria,
                        sourceConnection);

                    long countStart = System.Convert.ToInt32(
                        commandRowCount.ExecuteScalar());
                    Console.WriteLine("Starting row count = {0}", countStart);

                    NLog.Debug("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", countStart, h_atual.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss"), h_atual.ToString("yyyy-MM-dd HH:mm:ss"));

                    NLog.Info("Executo consulta");

                    string diretorio_novo = Path.Combine(ConfigurationManager.AppSettings["Destino"], DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString());
                    if (!Directory.Exists(diretorio_novo))
                    {
                        Directory.CreateDirectory(diretorio_novo);
                        NLog.Debug("Criando Diretorio : " + diretorio_novo);
                    }

                    //xml  
                    var inicio = DateTime.Now;
                    DataInicioRotina = DateTime.Now;
                    NLog.Info("Inicio:" + inicio.ToString());
                    int contat = 1;
                    try
                    {
                        // Write from the source to the destination.
                        while (reader.Read())
                        {

                            try
                            {
                                // string audio = Path.Combine(ConfigurationManager.AppSettings["Audios"], reader[14].ToString())
                                string audio1 = Path.Combine(ConfigurationManager.AppSettings["Audio1"], reader[14].ToString().Split('-')[0], reader[14].ToString());
                                string audio2 = Path.Combine(ConfigurationManager.AppSettings["Audio2"], reader[14].ToString().Split('-')[0], reader[14].ToString());
                                string audio3 = Path.Combine(ConfigurationManager.AppSettings["Audio3"], reader[14].ToString().Split('-')[0], reader[14].ToString());

                                var audioCurrent = string.Empty;

                                if (File.Exists(audio1))
                                {
                                    audioCurrent = audio1;
                                }
                                else if (File.Exists(audio2))
                                {
                                    audioCurrent = audio2;
                                }
                                else if (File.Exists(audio3))
                                {
                                    audioCurrent = audio3;
                                }






                                if (!File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())))
                                {
                                    //audio

                                    var nomenclatura_dt = "_" + DateTime.Now.ToString("yyyy_MM_dd_HHMMsss");
                                    // Copy(audio, Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav"));
                                    string Destino = @"" + diretorio_novo;
                                    Conversor convert = new Conversor(audioCurrent,
                                                                      // diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
                                                                      reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
                                                                      DataInicioRotina.Date.ToString("ddMMyyyy"),//datetime
                                                                      Destino
                                                                      );

                                    if (System.IO.File.Exists(convert.From))
                                    {
                                        // Copy(audioCurrent, Path.Combine(convert.DestFolder, convert.To));



                                        //NLog.Debug("Copiar {0} para {1} ", audioCurrent, Path.Combine(convert.DestFolder, convert.To));

                                        //if (File.Exists(Path.Combine(convert.DestFolder, convert.To)))
                                        //{
                                        //    NLog.Debug("Arquivo copiado com sucesso! " + Path.Combine(convert.DestFolder, convert.To));
                                        //}
                                        if (convert.Execute())
                                        {
                                            NLog.Debug("{0} ========> OK!", convert.From);
                                        }
                                        else
                                        {
                                            NLog.Debug("{0} ========> Erro!", convert.From);
                                        }
                                        //NLog.Debug("arquivo existe {0} ", convert.From.Replace(".vox", ".wav"));
                                        //if (convert.Execute())
                                        //{
                                        //    NLog.Debug("{0} ========> OK!", convert.From);
                                        //}
                                        //else
                                        //{
                                        //    NLog.Debug("{0} ========> Erro!", convert.From);
                                        //}
                                    }
                                    else
                                    {
                                        NLog.Debug("{0} ========> não existe origem!", convert.From);
                                    }
                                    long length_audio = new System.IO.FileInfo(diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav").Length;
                                    string arqTm_audio = GetSize(length_audio);

                                    //xml
                                    string arqXml = EscreverMetadata(reader[0], reader[1], reader[2], reader[3], reader[4], reader[5], reader[6], reader[7],
                                            reader[8], reader[9], reader[10], reader[11], reader[12], reader[13], reader[14], arqTm_audio, reader[16], reader[17], reader[18], reader[14].ToString().Replace(".wav", "") + nomenclatura_dt);

                                    long length = new System.IO.FileInfo(diretorio_novo + "\\" + arqXml).Length;
                                    string arqTm = GetSize(length);
                                    //enviar dados

                                    string Audio_local = Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav");

                                    string Xml_local = Path.Combine(diretorio_novo, arqXml);
                                    try
                                    {
                                        Copy(Audio_local, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], convert.To));
                                        EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
                                        Copy(Xml_local, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], arqXml));
                                        EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
                                        contat = contat + 1;
                                        //  SSH.Upload("", "");
                                        // if(winSCP.Upload(diretorio_novo, Audio_local))
                                        //if (true)
                                        //{
                                        //     //EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
                                        //     //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio audio " + Audio_local);

                                        //  //   if (winSCP.Upload(diretorio_novo, Xml_local))
                                        //     if (true)
                                        //     {
                                        //         //escrever bastao
                                        //        // EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
                                        //         //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio xml " + Xml_local);
                                        //     }
                                        //}




                                        NLog.Info("Cliente Transferiu audio e xml OK!");

                                    }
                                    catch (Exception ES)
                                    {
                                        NLog.Info("ERRO:" + ES.Message);
                                    }
                                }
                                else
                                {
                                    NLog.Error("ERRO existe:" + File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())));
                                }

                            }
                            catch (Exception errowhile)
                            {

                                NLog.Error(errowhile);
                            }

                        }


                        //
                        NLog.Info("Quantidade Enviados {0} arquivos de {1}", contat, countStart);
                        if (File.Exists(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT") && DateTime.Now.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]))
                        {
                            // winSCP.Upload(diretorio_novo, diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");
                            Copy(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], "bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"));
                            NLog.Info("arquivo bastao - {0}", diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
                        }


                        var final = DateTime.Now;
                        DataFinalRotina = DateTime.Now;
                        var final_cont = final - inicio;
                        NLog.Info("Fim:" + DateTime.Now.ToString());
                        NLog.Info("Time:" + final_cont);
                    }
                    catch (Exception ex)
                    {
                        NLog.Info(ex.Message);
                        using (XmlWriter writer_ = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + inicio.ToString("ddMMyyyy_HH") + ".xml"))
                        {
                            writer_.WriteStartDocument();
                            writer_.WriteStartElement("ALM");

                            while (reader.Read())
                            {
                                writer_.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));

                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    writer_.WriteStartElement(reader.GetName(i));
                                    writer_.WriteValue(reader[i].ToString());
                                    writer_.WriteEndElement();
                                }

                                writer_.WriteEndElement();
                            }

                            writer_.WriteEndElement();
                            writer_.WriteEndDocument();
                        }
                    }



                    #endregion

                    string validar = "DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT " +
                                      "SET @dataIni_ = '" + inicio.ToString("yyyy.MM.dd 01:00:00") + "'; " +
                                      "SET @dataFim_ = '" + inicio.ToString("yyyy.MM.dd 23:59:59") + "'; " +
                                      "SET @TZ_ = 3 " +
                                      "SELECT count(*) " +
                                      "FROM " + temporaria + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ";

                    SqlCommand commandRowCount_ = new SqlCommand(
                    validar,
                    sourceConnection);

                    // Perform a final count on the destination 
                    // table to see how many rows were added.
                    long countEnd = System.Convert.ToInt32(
                        commandRowCount_.ExecuteScalar());
                    NLog.Info("Ending row count = {0}", countEnd);
                    NLog.Info("{0} rows were added.", countEnd - countStart);
                    //NLog.Info("Press Enter to finish.");
                    //Console.ReadLine();


                }

            }

            DirectoryInfo dirDelete = new DirectoryInfo(ConfigurationManager.AppSettings["Destino"]);
            NLog.Info("total deletar {0}", dirDelete.GetFiles().Length);
            foreach (FileInfo fi in dirDelete.GetFiles())
            {
                if (fi.LastWriteTime < DateTime.Today.AddDays(-2))
                {
                    fi.Delete();
                }
                //if (fi.LastWriteTime <= DateTime.Today.AddDays(Convert.ToDouble(dia)) && fi.Extension.Contains(".csv"))
            }
        }

        public static void EscreverBastao(string Message)
        {
            string dir = ConfigurationManager.AppSettings["Destino"];
            string novo = Path.Combine(dir, DataInicioRotina.Year.ToString(), DataInicioRotina.Month.ToString(), DataInicioRotina.Day.ToString());
            NLog.Info("Arquivo Bastao: {0}", novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");

            StreamWriter sw = null;
            try
            {
                if (!File.Exists(novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"))
                {

                    sw = new StreamWriter(novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", true);
                    sw.WriteLine("Nome do arquivo, Data e hora transmissao, Tamanho do arquivo");
                    sw.Flush();
                    sw.Close();
                }
                else
                {
                    sw = new StreamWriter(novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", true);
                    sw.WriteLine(Message);
                    sw.Flush();
                    sw.Close();
                }

            }
            catch (Exception)
            {

                throw;
            }
        }

        public static string EscreverMetadata(
                       object ID_CHAMADA,
                       object CPF,
                       object PROTOCOLO,
                       object NUM_CONTRATO_GESTAO,
                       object NUMERO_DE_A,
                       object RAMAL,
                       object COD_AGENTE,
                       object DATA_LIGACAO_GMT,
                       object DATA_INICIO_GRAVACAO,
                       object DATA_FIM_GRAVACAO,
                       object TEMPO_GRAVACAO_EM_SEGUNDOS,
                       object NOME_OPERADOR,
                       object CELULA_OPERADOR,
                       object SITE,
                       object ARQUIVO,
                       object TAMANHO_KB,
                       object NUM_DE_PAUSA,
                       object NUM_DE_CONF,
                       object NUM_DE_TRANSF,
                       string NAME_FILE
           )
        {
            string dir = ConfigurationManager.AppSettings["Destino"];
            string novo = Path.Combine(dir, DataInicioRotina.Year.ToString(), DataInicioRotina.Month.ToString(), DataInicioRotina.Day.ToString());
            try
            {
                string arquivo_ = NAME_FILE + ".XML";
                NLog.Info("Arquivo XML: {0}", Path.Combine(novo, NAME_FILE + ".XML"));
                XmlWriterSettings settings = new XmlWriterSettings();
                settings.NamespaceHandling = NamespaceHandling.OmitDuplicates;

                using (XmlWriter writer = XmlWriter.Create(Path.Combine(novo, NAME_FILE + ".XML"), settings))
                {
                    writer.WriteStartDocument();
                    writer.WriteStartElement("AUDIOS");
                    writer.WriteAttributeString("xmlns", "xsd", null, "http://www.w3.org/2001/XMLSchema-instance");
                    writer.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");

                    writer.WriteStartElement("ETL");

                    // <-- These are new
                    writer.WriteStartElement("ID_CHAMADA");
                    writer.WriteValue(ID_CHAMADA);
                    writer.WriteEndElement();
                    writer.WriteStartElement("CPF");
                    writer.WriteValue(CPF);
                    writer.WriteEndElement();
                    writer.WriteStartElement("PROTOCOLO");
                    writer.WriteValue(PROTOCOLO);
                    writer.WriteEndElement();
                    writer.WriteStartElement("NUM_CONTRATO_GESTAO");
                    writer.WriteValue(NUM_CONTRATO_GESTAO);
                    writer.WriteEndElement();
                    writer.WriteStartElement("NUMERO_DE_A");
                    writer.WriteValue(NUMERO_DE_A);
                    writer.WriteEndElement();
                    writer.WriteStartElement("RAMAL");
                    writer.WriteValue(RAMAL);
                    writer.WriteEndElement();
                    writer.WriteStartElement("COD_AGENTE");
                    writer.WriteValue(COD_AGENTE);
                    writer.WriteEndElement();
                    writer.WriteStartElement("DATA_LIGACAO_GMT");
                    writer.WriteValue(DATA_LIGACAO_GMT);
                    writer.WriteEndElement();
                    writer.WriteStartElement("DATA_INICIO_GRAVACAO");
                    writer.WriteValue(DATA_INICIO_GRAVACAO);
                    writer.WriteEndElement();
                    writer.WriteStartElement("DATA_FIM_GRAVACAO");
                    writer.WriteValue(DATA_FIM_GRAVACAO);
                    writer.WriteEndElement();
                    writer.WriteStartElement("TEMPO_GRAVACAO_EM_SEGUNDOS");
                    writer.WriteValue(TEMPO_GRAVACAO_EM_SEGUNDOS);
                    writer.WriteEndElement();
                    writer.WriteStartElement("NOME_OPERADOR");
                    writer.WriteValue(NOME_OPERADOR);
                    writer.WriteEndElement();
                    writer.WriteStartElement("CELULA_OPERADOR");
                    writer.WriteValue(CELULA_OPERADOR);
                    writer.WriteEndElement();
                    writer.WriteStartElement("SITE");
                    writer.WriteValue(SITE);
                    writer.WriteEndElement();
                    writer.WriteStartElement("ARQUIVO");
                    writer.WriteValue(NAME_FILE + ".WAV");
                    writer.WriteEndElement();
                    writer.WriteStartElement("TAMANHO_KB");
                    writer.WriteValue(TAMANHO_KB);
                    writer.WriteEndElement();
                    writer.WriteStartElement("NUM_DE_PAUSA");
                    writer.WriteValue(NUM_DE_PAUSA);
                    writer.WriteEndElement();
                    writer.WriteStartElement("NUM_DE_CONF");
                    writer.WriteValue(NUM_DE_CONF);
                    writer.WriteEndElement();
                    writer.WriteStartElement("NUM_DE_TRANSF");
                    writer.WriteValue(NUM_DE_TRANSF);
                    writer.WriteEndElement();

                    writer.WriteEndElement();



                    writer.WriteEndElement();
                    writer.WriteEndDocument();
                }
                return arquivo_;
            }
            catch (Exception ex)
            {
                // EscreverErroLog("erro" + ex.Message);
                var m = ex.Message;
                return null;
            }


        }

        public static string GetSize(long size)
        {
            string postfix = "Bytes";
            long result = size;

            if (size >= 1073741824)//more than 1 GB
            {
                result = size / 1073741824;
                postfix = "GB";
            }
            else if (size >= 1048576)//more that 1 MB
            {
                result = size / 1048576;
                postfix = "MB";
            }
            else if (size >= 1024)//more that 1 KB
            {
                result = size / 1024;
                postfix = "KB";
            }

            return result.ToString();// result.ToString("F1") + " " + postfix;
        }

        public static void Copy(string inputFilePath, string outputFilePath)
        {
            int bufferSize = 1024 * 1024;

            using (FileStream fileStream = new FileStream(outputFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite))
            //using (FileStream fs = File.Open(<file-path>, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                FileStream fs = new FileStream(inputFilePath, FileMode.Open, FileAccess.ReadWrite);
                fileStream.SetLength(fs.Length);
                int bytesRead = -1;
                byte[] bytes = new byte[bufferSize];

                while ((bytesRead = fs.Read(bytes, 0, bufferSize)) > 0)
                {
                    fileStream.Write(bytes, 0, bytesRead);
                }
            }

            if (!File.Exists(outputFilePath))
            {
                using (FileStream fileStream = new FileStream(outputFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite))
                //using (FileStream fs = File.Open(<file-path>, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    FileStream fs = new FileStream(inputFilePath, FileMode.Open, FileAccess.ReadWrite);
                    fileStream.SetLength(fs.Length);
                    int bytesRead = -1;
                    byte[] bytes = new byte[bufferSize];

                    while ((bytesRead = fs.Read(bytes, 0, bufferSize)) > 0)
                    {
                        fileStream.Write(bytes, 0, bytesRead);
                    }
                }
            }

        }
    }
}
//using NLog;
//using SFTP;
//using System;
//using System.Configuration;
//using System.Data;
//using System.Data.SqlClient;
//using System.IO;
//using System.Xml;
//using System.Xml.Linq;

//namespace Speech_Analytics.Dados
//{
//    public static class Dados
//    {
//        private static NLog.Logger NLog = LogManager.GetCurrentClassLogger();
//        private static DateTime DataInicioRotina;
//        private static DateTime DataFinalRotina;


//        public static void Consulta_retroativa(DateTime I, DateTime F)
//        {
//            string connectionString = ConfigurationManager.ConnectionStrings["UIP"].ToString();
//            // Open a sourceConnection to the AdventureWorks database.
//            using (SqlConnection sourceConnection = new SqlConnection(connectionString))
//            {
//                sourceConnection.Open();

//                //VERIFICAR NAO PROCESSADOS



//                //CONSUL
//                #region CONSULTA DADOS

//                string dir = ConfigurationManager.AppSettings["Application"];
//                string sql = string.Empty;
//                string query = @"\Speech_Analytics_AQM.sql";

//                using (StreamReader readerFile = new StreamReader(dir + query, System.Text.Encoding.Default, true))
//                {
//                    sql = readerFile.ReadToEnd();
//                }

//                string sql_ = sql.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + I.ToString("yyyy-MM-dd HH:mm:ss") + "'");
//                sql_ = sql_.Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + F.ToString("yyyy-MM-dd HH:mm:ss") + "'");

//                NLog.Info(sql_);
//                var temporaria = "#TEMP_SPEECH" + I.ToString("HHmmss");
//                sql_ = sql_.Replace("##TEMP_SPEECH", temporaria);
//                // Get data from the source table as a SqlDataReader.
//                SqlCommand commandSourceData = new SqlCommand(sql_, sourceConnection);
//                commandSourceData.CommandTimeout = 99999;

//                SqlDataReader reader =
//                    commandSourceData.ExecuteReader();


//                // Perform an initial count on the destination table.
//                SqlCommand commandRowCount = new SqlCommand(
//                    "SELECT COUNT(*) FROM " + temporaria,
//                    sourceConnection);

//                long countStart = System.Convert.ToInt32(
//                    commandRowCount.ExecuteScalar());
//                Console.WriteLine("Starting row count = {0}", countStart);

//                NLog.Debug("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", countStart, I.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss"), F.ToString("yyyy-MM-dd HH:mm:ss"));

//                NLog.Info("Executo consulta");

//                string diretorio_novo = Path.Combine(ConfigurationManager.AppSettings["Destino"], DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString());
//                if (!Directory.Exists(diretorio_novo))
//                {
//                    Directory.CreateDirectory(diretorio_novo);
//                    NLog.Debug("Criando Diretorio : " + diretorio_novo);
//                }

//                //xml  
//                var inicio = DateTime.Now;
//                DataInicioRotina = DateTime.Now;
//                NLog.Info("Inicio:" + inicio.ToString());
//                int contat = 1;
//                try
//                {
//                    // Write from the source to the destination.
//                    while (reader.Read())
//                    {

//                        try
//                        {
//                            // string audio = Path.Combine(ConfigurationManager.AppSettings["Audios"], reader[14].ToString())
//                            string audio1 = Path.Combine(ConfigurationManager.AppSettings["Audio1"], reader[14].ToString().Split('-')[0], reader[14].ToString());
//                            string audio2 = Path.Combine(ConfigurationManager.AppSettings["Audio2"], reader[14].ToString().Split('-')[0], reader[14].ToString());
//                            string audio3 = Path.Combine(ConfigurationManager.AppSettings["Audio3"], reader[14].ToString().Split('-')[0], reader[14].ToString());

//                            var audioCurrent = string.Empty;

//                            if (File.Exists(audio1))
//                            {
//                                audioCurrent = audio1;
//                            }
//                            else if (File.Exists(audio2))
//                            {
//                                audioCurrent = audio2;
//                            }
//                            else if (File.Exists(audio3))
//                            {
//                                audioCurrent = audio3;
//                            }






//                            if (!File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())))
//                            {
//                                //audio

//                                var nomenclatura_dt = "_" + DateTime.Now.ToString("yyyy_MM_dd_HHMMsss");
//                                // Copy(audio, Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav"));
//                                string Destino = @"" + diretorio_novo;
//                                Conversor convert = new Conversor(audioCurrent,
//                                                                  // diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
//                                                                  reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
//                                                                  DataInicioRotina.Date.ToString("ddMMyyyy"),//datetime
//                                                                  Destino
//                                                                  );

//                                if (System.IO.File.Exists(convert.From))
//                                {
//                                    // Copy(audioCurrent, Path.Combine(convert.DestFolder, convert.To));



//                                    //NLog.Debug("Copiar {0} para {1} ", audioCurrent, Path.Combine(convert.DestFolder, convert.To));

//                                    //if (File.Exists(Path.Combine(convert.DestFolder, convert.To)))
//                                    //{
//                                    //    NLog.Debug("Arquivo copiado com sucesso! " + Path.Combine(convert.DestFolder, convert.To));
//                                    //}
//                                    if (convert.Execute())
//                                    {
//                                        NLog.Debug("{0} ========> OK!", convert.From);
//                                    }
//                                    else
//                                    {
//                                        NLog.Debug("{0} ========> Erro!", convert.From);
//                                    }
//                                    //NLog.Debug("arquivo existe {0} ", convert.From.Replace(".vox", ".wav"));
//                                    //if (convert.Execute())
//                                    //{
//                                    //    NLog.Debug("{0} ========> OK!", convert.From);
//                                    //}
//                                    //else
//                                    //{
//                                    //    NLog.Debug("{0} ========> Erro!", convert.From);
//                                    //}
//                                }
//                                else
//                                {
//                                    NLog.Debug("{0} ========> não existe origem!", convert.From);
//                                }
//                                long length_audio = new System.IO.FileInfo(diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav").Length;
//                                string arqTm_audio = GetSize(length_audio);

//                                //xml
//                                string arqXml = EscreverMetadata(reader[0], reader[1], reader[2], reader[3], reader[4], reader[5], reader[6], reader[7],
//                                        reader[8], reader[9], reader[10], reader[11], reader[12], reader[13], reader[14], arqTm_audio, reader[16], reader[17], reader[18], reader[14].ToString().Replace(".wav", "") + nomenclatura_dt);

//                                long length = new System.IO.FileInfo(diretorio_novo + "\\" + arqXml).Length;
//                                string arqTm = GetSize(length);
//                                //enviar dados

//                                string Audio_local = Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav");

//                                string Xml_local = Path.Combine(diretorio_novo, arqXml);
//                                try
//                                {
//                                    Copy(Audio_local, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], convert.To));
//                                    EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
//                                    Copy(Xml_local, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], arqXml));
//                                    EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
//                                    contat = contat + 1;
//                                    //  SSH.Upload("", "");
//                                    // if(winSCP.Upload(diretorio_novo, Audio_local))
//                                    //if (true)
//                                    //{
//                                    //     //EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
//                                    //     //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio audio " + Audio_local);

//                                    //  //   if (winSCP.Upload(diretorio_novo, Xml_local))
//                                    //     if (true)
//                                    //     {
//                                    //         //escrever bastao
//                                    //        // EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
//                                    //         //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio xml " + Xml_local);
//                                    //     }
//                                    //}




//                                    NLog.Info("Cliente Transferiu audio e xml OK!");

//                                }
//                                catch (Exception ES)
//                                {
//                                    NLog.Info("ERRO:" + ES.Message);
//                                }
//                            }
//                            else
//                            {
//                                NLog.Error("ERRO existe:" + File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())));
//                            }

//                        }
//                        catch (Exception errowhile)
//                        {

//                            NLog.Error(errowhile);
//                        }

//                    }


//                    //
//                    NLog.Info("Quantidade Enviados {0} arquivos de {1}", contat, countStart);
//                    if (File.Exists(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT") && DateTime.Now.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]))
//                    {
//                        // winSCP.Upload(diretorio_novo, diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");
//                        Copy(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], "bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"));
//                        NLog.Info("arquivo bastao - {0}", diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
//                    }


//                    var final = DateTime.Now;
//                    DataFinalRotina = DateTime.Now;
//                    var final_cont = final - inicio;
//                    NLog.Info("Fim:" + DateTime.Now.ToString());
//                    NLog.Info("Time:" + final_cont);
//                }
//                catch (Exception ex)
//                {
//                    NLog.Info(ex.Message);
//                    using (XmlWriter writer_ = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + inicio.ToString("ddMMyyyy_HH") + ".xml"))
//                    {
//                        writer_.WriteStartDocument();
//                        writer_.WriteStartElement("ALM");

//                        while (reader.Read())
//                        {
//                            writer_.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));

//                            for (int i = 0; i < reader.FieldCount; i++)
//                            {
//                                writer_.WriteStartElement(reader.GetName(i));
//                                writer_.WriteValue(reader[i].ToString());
//                                writer_.WriteEndElement();
//                            }

//                            writer_.WriteEndElement();
//                        }

//                        writer_.WriteEndElement();
//                        writer_.WriteEndDocument();
//                    }
//                }



//                #endregion

//                string validar = "DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT " +
//                                  "SET @dataIni_ = '" + inicio.ToString("yyyy.MM.dd 01:00:00") + "'; " +
//                                  "SET @dataFim_ = '" + inicio.ToString("yyyy.MM.dd 23:59:59") + "'; " +
//                                  "SET @TZ_ = 3 " +
//                                  "SELECT count(*) " +
//                                  "FROM " + temporaria + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ";

//                SqlCommand commandRowCount_ = new SqlCommand(
//                validar,
//                sourceConnection);

//                // Perform a final count on the destination 
//                // table to see how many rows were added.
//                long countEnd = System.Convert.ToInt32(
//                    commandRowCount_.ExecuteScalar());
//                NLog.Info("Ending row count = {0}", countEnd);
//                NLog.Info("{0} rows were added.", countEnd - countStart);
//                //NLog.Info("Press Enter to finish.");
//                //Console.ReadLine();


//            }

//            DirectoryInfo dirDelete = new DirectoryInfo(ConfigurationManager.AppSettings["Destino"]);
//            NLog.Info("total deletar {0}", dirDelete.GetFiles().Length);
//            foreach (FileInfo fi in dirDelete.GetFiles())
//            {
//                if (fi.LastWriteTime < DateTime.Today.AddDays(-2))
//                {
//                    fi.Delete();
//                }
//                //if (fi.LastWriteTime <= DateTime.Today.AddDays(Convert.ToDouble(dia)) && fi.Extension.Contains(".csv"))



//            }


//        }
//        public static void Consulta(DateTime h_atual)
//        {
//            string connectionString = ConfigurationManager.ConnectionStrings["UIP"].ToString();
//            // Open a sourceConnection to the AdventureWorks database.
//            using (SqlConnection sourceConnection = new SqlConnection(connectionString))
//            {
//                sourceConnection.Open();

//                //VERIFICAR NAO PROCESSADOS

//                #region NÃO PROCESSADOS
//                //XmlDocument doc = new XmlDocument();
//                //DirectoryInfo directoryInfo = new DirectoryInfo(Directory.GetCurrentDirectory());
//                //foreach (var item in directoryInfo.GetFiles())
//                //{
//                //    if (item.Name.StartsWith("dados_nao_processados"))
//                //    {
//                //        using (SqlConnection destinationConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["LOCAL"].ToString()))
//                //        {
//                //            destinationConnection.Open();
//                //            SqlTransaction transaction = destinationConnection.BeginTransaction();
//                //            DataSet ds = new DataSet();
//                //            ds.ReadXml(item.Name);

//                //            using (SqlBulkCopy sbc = new SqlBulkCopy(destinationConnection, SqlBulkCopyOptions.KeepIdentity, transaction))
//                //            {

//                //                sbc.DestinationTableName =
//                //            "[dbo].[cl_contact_event]";

//                //                // Clear the ColumnMappingCollection.
//                //                sbc.ColumnMappings.Clear();

//                //                //sbc.ColumnMappings.Add("account", "account_number");
//                //                //sbc.ColumnMappings.Add("response", "response_status");
//                //                //sbc.ColumnMappings.Add("ov_phone", "ov_phone_number");
//                //                //sbc.ColumnMappings.Add("user_id", "agent_login_name");
//                //                //sbc.ColumnMappings.Add("DT", "DT");
//                //                //sbc.ColumnMappings.Add("seqnum", "seqnum");
//                //                //sbc.ColumnMappings.Add("callid", "callid");
//                //                //sbc.ColumnMappings.Add("service_id", "service_id");

//                //                sbc.BulkCopyTimeout = 9999;

//                //                sbc.NotifyAfter = 1000;
//                //                sbc.SqlRowsCopied += (sender, eventArgs) => Console.WriteLine("Wrote " + eventArgs.RowsCopied + " records.");

//                //                try
//                //                {
//                //                    sbc.WriteToServer(ds.Tables[0]);
//                //                }
//                //                catch (Exception)
//                //                {


//                //                }
//                //                finally
//                //                {
//                //                    // Close the SqlDataReader. The SqlBulkCopy
//                //                    // object is automatically closed at the end
//                //                    // of the using block.
//                //                    ds.Dispose();
//                //                    transaction.Commit();
//                //                }

//                //            }
//                //        }
//                //    }
//                //}
//                #endregion

//                //CONSUL
//                #region CONSULTA DADOS

//                string dir = ConfigurationManager.AppSettings["Application"];
//                string sql = string.Empty;
//                string query = @"\Speech_Analytics_AQM.sql";

//                using (StreamReader readerFile = new StreamReader(dir + query, System.Text.Encoding.Default, true))
//                {
//                    sql = readerFile.ReadToEnd();
//                }

//                string sql_ = sql.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + h_atual.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss") + "'");
//                sql_ = sql_.Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + h_atual.ToString("yyyy-MM-dd HH:mm:ss") + "'");

//                NLog.Info(sql_);
//                var temporaria = "#TEMP_SPEECH" + h_atual.ToString("HHmmss");
//                sql_ = sql_.Replace("##TEMP_SPEECH", temporaria);
//                // Get data from the source table as a SqlDataReader.
//                SqlCommand commandSourceData = new SqlCommand(sql_, sourceConnection);
//                commandSourceData.CommandTimeout = 99999;

//                SqlDataReader reader =
//                    commandSourceData.ExecuteReader();


//                // Perform an initial count on the destination table.
//                SqlCommand commandRowCount = new SqlCommand(
//                    "SELECT COUNT(*) FROM "+ temporaria,
//                    sourceConnection);

//                long countStart = System.Convert.ToInt32(
//                    commandRowCount.ExecuteScalar());
//                Console.WriteLine("Starting row count = {0}", countStart);

//                NLog.Debug("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", countStart, h_atual.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss"), h_atual.ToString("yyyy-MM-dd HH:mm:ss"));

//                NLog.Info("Executo consulta");

//                string diretorio_novo = Path.Combine(ConfigurationManager.AppSettings["Destino"], DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString());
//                if (!Directory.Exists(diretorio_novo))
//                {
//                    Directory.CreateDirectory(diretorio_novo);
//                    NLog.Debug("Criando Diretorio : " + diretorio_novo);
//                }

//                //xml  
//                        var inicio = DateTime.Now;
//                        DataInicioRotina = DateTime.Now;
//                       NLog.Info("Inicio:" + inicio.ToString());
//                int contat = 0;
//                try
//                        {
//                        // Write from the source to the destination.
//                        while (reader.Read())
//                        {

//                            try
//                            {
//                            string audio = Path.Combine(ConfigurationManager.AppSettings["Audios"], reader[14].ToString().Replace(".wav", ".vox"));
//                            if (!File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())))
//                            {
//                                //audio

//                                var nomenclatura_dt = "_" + DateTime.Now.ToString("yyyy_MM_dd_HHMMsss");
//                                Copy(audio, Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".vox"));
//                                string Destino = @"" + diretorio_novo;
//                                Conversor convert = new Conversor(
//                                                                  diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
//                                                                  reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
//                                                                  DataInicioRotina.Date.ToString("ddMMyyyy"),//datetime
//                                                                  Destino
//                                                                  );

//                                if (!System.IO.File.Exists(convert.From))
//                                {
//                                    NLog.Debug("arquivo existe {0} ", convert.From.Replace(".vox", ".wav"));
//                                    if (convert.Execute())
//                                    {
//                                        NLog.Debug("{0} ========> OK!", convert.From);
//                                    }
//                                    else
//                                    {
//                                        NLog.Debug("{0} ========> Erro!", convert.From);
//                                    }
//                                }
//                                else
//                                {
//                                    NLog.Debug("{0} ========> não existe origem!", convert.From);
//                                }
//                                long length_audio = new System.IO.FileInfo(diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav").Length;
//                                string arqTm_audio = GetSize(length_audio);

//                                //xml
//                                string arqXml = EscreverMetadata(reader[0], reader[1], reader[2], reader[3], reader[4], reader[5], reader[6], reader[7],
//                                        reader[8], reader[9], reader[10], reader[11], reader[12], reader[13], reader[14], arqTm_audio, reader[16], reader[17], reader[18], reader[14].ToString().Replace(".wav", "") + nomenclatura_dt);

//                                long length = new System.IO.FileInfo(diretorio_novo + "\\" + arqXml).Length;
//                                string arqTm = GetSize(length);
//                                //enviar dados

//                                string Audio_local = Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav");

//                                string Xml_local = Path.Combine(diretorio_novo, arqXml);
//                                try
//                                {
//                                    Copy(Audio_local,Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"],convert.To));
//                                    Copy(Xml_local,Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], arqXml));
//                                    contat++;
//                                    //  SSH.Upload("", "");
//                                 // if(winSCP.Upload(diretorio_novo, Audio_local))
//                                   //if (true)
//                                   //{
//                                   //     //EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
//                                   //     //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio audio " + Audio_local);

//                                   //  //   if (winSCP.Upload(diretorio_novo, Xml_local))
//                                   //     if (true)
//                                   //     {
//                                   //         //escrever bastao
//                                   //        // EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
//                                   //         //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio xml " + Xml_local);
//                                   //     }
//                                   //}




//                                    NLog.Info("Cliente Transferiu audio e xml OK!");

//                                }
//                                catch (Exception ES)
//                                {
//                                    NLog.Info("ERRO:" + ES.Message);
//                                }
//                            }
//                        }
//                            catch (Exception errowhile)
//                            {

//                            NLog.Error(errowhile);
//                        }

//                        }


//                    //
//                    NLog.Info("Quantidade Enviados {0} arquivos de {1}", contat, countStart);
//                    if (File.Exists(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT") && DateTime.Now.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]))
//                    {
//                       // winSCP.Upload(diretorio_novo, diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");
//                        NLog.Info("arquivo bastao - {0}", diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao" );
//                    }


//                    var final = DateTime.Now;
//                    DataFinalRotina = DateTime.Now;
//                       var  final_cont = final - inicio;
//                            NLog.Info("Fim:" + DateTime.Now.ToString());
//                            NLog.Info("Time:" + final_cont);
//                        }
//                        catch (Exception ex)
//                        {
//                            NLog.Info(ex.Message);
//                            using (XmlWriter writer_ = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"]+"dados_nao_processados" + inicio.ToString("ddMMyyyy_HH") + ".xml"))
//                            {
//                                writer_.WriteStartDocument();
//                                writer_.WriteStartElement("ALM");

//                                while (reader.Read())
//                                {
//                                    writer_.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));

//                            for (int i = 0; i < reader.FieldCount; i++)
//                            {
//                                writer_.WriteStartElement(reader.GetName(i));
//                                writer_.WriteValue(reader[i].ToString());
//                                writer_.WriteEndElement();
//                            }

//                            writer_.WriteEndElement();
//                                }

//                                writer_.WriteEndElement();
//                                writer_.WriteEndDocument();
//                            }
//                        }



//                    #endregion

//                    string validar = "DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT " +
//                                      "SET @dataIni_ = '"+ inicio.ToString("yyyy.MM.dd 01:00:00") +"'; " +
//                                      "SET @dataFim_ = '"+ inicio.ToString("yyyy.MM.dd 23:59:59") +"'; " +
//                                      "SET @TZ_ = 3 " +
//                                      "SELECT count(*) " +
//                                      "FROM "+ temporaria +" where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ";

//                    SqlCommand commandRowCount_ = new SqlCommand(
//                    validar,
//                    sourceConnection);

//                    // Perform a final count on the destination 
//                    // table to see how many rows were added.
//                    long countEnd = System.Convert.ToInt32(
//                        commandRowCount_.ExecuteScalar());
//                    NLog.Info("Ending row count = {0}", countEnd);
//                    NLog.Info("{0} rows were added.", countEnd - countStart);
//                    //NLog.Info("Press Enter to finish.");
//                    //Console.ReadLine();


//            }

//            DirectoryInfo dirDelete = new DirectoryInfo(ConfigurationManager.AppSettings["Destino"]);
//            NLog.Info("total deletar {0}", dirDelete.GetFiles().Length);
//            foreach (FileInfo fi in dirDelete.GetFiles())
//            {
//                if (fi.LastWriteTime < DateTime.Today.AddDays(-2))
//                {
//                    fi.Delete();
//                }
//                //if (fi.LastWriteTime <= DateTime.Today.AddDays(Convert.ToDouble(dia)) && fi.Extension.Contains(".csv"))



//            }
//        }

//        public static void ConsultaAQM(DateTime h_atual)
//        {
//            string connectionString = ConfigurationManager.ConnectionStrings["UIP"].ToString();
//            // Open a sourceConnection to the AdventureWorks database.
//            using (SqlConnection sourceConnection = new SqlConnection(connectionString))
//            {
//                sourceConnection.Open();

//                //VERIFICAR NAO PROCESSADOS



//                //CONSUL
//                #region CONSULTA DADOS

//                string dir = ConfigurationManager.AppSettings["Application"];
//                string sql = string.Empty;
//                string query = @"\Speech_Analytics_AQM.sql";

//                using (StreamReader readerFile = new StreamReader(dir + query, System.Text.Encoding.Default, true))
//                {
//                    sql = readerFile.ReadToEnd();
//                }

//                string sql_ = sql.Replace("SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; ", "SET @dataIni = '" + h_atual.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss") + "'");
//                sql_ = sql_.Replace("SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; ", "SET @dataFim = '" + h_atual.ToString("yyyy-MM-dd HH:mm:ss") + "'");

//                NLog.Info(sql_);
//                var temporaria = "#TEMP_SPEECH" + h_atual.ToString("HHmmss");
//                sql_ = sql_.Replace("##TEMP_SPEECH", temporaria);
//                // Get data from the source table as a SqlDataReader.
//                SqlCommand commandSourceData = new SqlCommand(sql_, sourceConnection);
//                commandSourceData.CommandTimeout = 99999;

//                SqlDataReader reader =
//                    commandSourceData.ExecuteReader();


//                // Perform an initial count on the destination table.
//                SqlCommand commandRowCount = new SqlCommand(
//                    "SELECT COUNT(*) FROM " + temporaria,
//                    sourceConnection);

//                long countStart = System.Convert.ToInt32(
//                    commandRowCount.ExecuteScalar());
//                Console.WriteLine("Starting row count = {0}", countStart);

//                NLog.Debug("QUANTIDADE SQL CONSULTA: {0} - Parametros {1}-{2}", countStart, h_atual.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss"), h_atual.ToString("yyyy-MM-dd HH:mm:ss"));

//                NLog.Info("Executo consulta");

//                string diretorio_novo = Path.Combine(ConfigurationManager.AppSettings["Destino"], DateTime.Today.Year.ToString(), DateTime.Today.Month.ToString(), DateTime.Today.Day.ToString());
//                if (!Directory.Exists(diretorio_novo))
//                {
//                    Directory.CreateDirectory(diretorio_novo);
//                    NLog.Debug("Criando Diretorio : " + diretorio_novo);
//                }

//                //xml  
//                var inicio = DateTime.Now;
//                DataInicioRotina = DateTime.Now;
//                NLog.Info("Inicio:" + inicio.ToString());
//                int contat = 1;
//                try
//                {
//                    // Write from the source to the destination.
//                    while (reader.Read())
//                    {

//                        try
//                        {
//                           // string audio = Path.Combine(ConfigurationManager.AppSettings["Audios"], reader[14].ToString())
//                            string audio1 = Path.Combine(ConfigurationManager.AppSettings["Audio1"], reader[14].ToString().Split('-')[0], reader[14].ToString());
//                            string audio2 = Path.Combine(ConfigurationManager.AppSettings["Audio2"], reader[14].ToString().Split('-')[0], reader[14].ToString());
//                            string audio3 = Path.Combine(ConfigurationManager.AppSettings["Audio3"], reader[14].ToString().Split('-')[0], reader[14].ToString());

//                            var audioCurrent = string.Empty;

//                            if (File.Exists(audio1))
//                            {
//                                audioCurrent = audio1;
//                            }
//                            else if (File.Exists(audio2))
//                            {
//                                audioCurrent = audio2;
//                            }
//                            else if (File.Exists(audio3))
//                            {
//                                audioCurrent = audio3;
//                            }






//                            if (!File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())))
//                            {
//                                //audio

//                                var nomenclatura_dt = "_" + DateTime.Now.ToString("yyyy_MM_dd_HHMMsss");
//                               // Copy(audio, Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav"));
//                                string Destino = @"" + diretorio_novo;
//                                Conversor convert = new Conversor( audioCurrent,
//                                                                 // diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
//                                                                  reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav",
//                                                                  DataInicioRotina.Date.ToString("ddMMyyyy"),//datetime
//                                                                  Destino
//                                                                  );

//                                if (System.IO.File.Exists(convert.From))
//                                {
//                                   // Copy(audioCurrent, Path.Combine(convert.DestFolder, convert.To));



//                                    //NLog.Debug("Copiar {0} para {1} ", audioCurrent, Path.Combine(convert.DestFolder, convert.To));

//                                    //if (File.Exists(Path.Combine(convert.DestFolder, convert.To)))
//                                    //{
//                                    //    NLog.Debug("Arquivo copiado com sucesso! " + Path.Combine(convert.DestFolder, convert.To));
//                                    //}
//                                    if (convert.Execute())
//                                    {
//                                        NLog.Debug("{0} ========> OK!", convert.From);
//                                    }
//                                    else
//                                    {
//                                        NLog.Debug("{0} ========> Erro!", convert.From);
//                                    }
//                                    //NLog.Debug("arquivo existe {0} ", convert.From.Replace(".vox", ".wav"));
//                                    //if (convert.Execute())
//                                    //{
//                                    //    NLog.Debug("{0} ========> OK!", convert.From);
//                                    //}
//                                    //else
//                                    //{
//                                    //    NLog.Debug("{0} ========> Erro!", convert.From);
//                                    //}
//                                }
//                                else
//                                {
//                                    NLog.Debug("{0} ========> não existe origem!", convert.From);
//                                }
//                                long length_audio = new System.IO.FileInfo(diretorio_novo + "\\" + reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav").Length;
//                                string arqTm_audio = GetSize(length_audio);

//                                //xml
//                                string arqXml = EscreverMetadata(reader[0], reader[1], reader[2], reader[3], reader[4], reader[5], reader[6], reader[7],
//                                        reader[8], reader[9], reader[10], reader[11], reader[12], reader[13], reader[14], arqTm_audio, reader[16], reader[17], reader[18], reader[14].ToString().Replace(".wav", "") + nomenclatura_dt);

//                                long length = new System.IO.FileInfo(diretorio_novo + "\\" + arqXml).Length;
//                                string arqTm = GetSize(length);
//                                //enviar dados

//                                string Audio_local = Path.Combine(diretorio_novo, reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav");

//                                string Xml_local = Path.Combine(diretorio_novo, arqXml);
//                                try
//                                {
//                                    Copy(Audio_local, Path.Combine(ConfigurationManager.AppSettings["DestinoAudio"], convert.To));
//                                    EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
//                                    Copy(Xml_local, Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], arqXml));
//                                    EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
//                                    contat=contat+1;
//                                    //  SSH.Upload("", "");
//                                    // if(winSCP.Upload(diretorio_novo, Audio_local))
//                                    //if (true)
//                                    //{
//                                    //     //EscreverBastao(string.Format("{0},{1},{2}", reader[14].ToString().Replace(".wav", "") + nomenclatura_dt + ".wav", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm_audio));
//                                    //     //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio audio " + Audio_local);

//                                    //  //   if (winSCP.Upload(diretorio_novo, Xml_local))
//                                    //     if (true)
//                                    //     {
//                                    //         //escrever bastao
//                                    //        // EscreverBastao(string.Format("{0},{1},{2}", arqXml, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), arqTm));
//                                    //         //NLog.Info("Arquivo Bastao: {0} {1}", "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", "envio xml " + Xml_local);
//                                    //     }
//                                    //}




//                                    NLog.Info("Cliente Transferiu audio e xml OK!");

//                                }
//                                catch (Exception ES)
//                                {
//                                    NLog.Info("ERRO:" + ES.Message);
//                                }
//                            }
//                            else
//                            {
//                                NLog.Error("ERRO existe:" + File.Exists(Path.Combine(diretorio_novo, reader[14].ToString())));
//                            }

//                        }
//                        catch (Exception errowhile)
//                        {

//                            NLog.Error(errowhile);
//                        }

//                    }


//                    //
//                    NLog.Info("Quantidade Enviados {0} arquivos de {1}", contat, countStart);
//                    if (File.Exists(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT") && DateTime.Now.Hour >= Convert.ToInt32(ConfigurationManager.AppSettings["HoraEnvioBastao"]))
//                    {
//                        // winSCP.Upload(diretorio_novo, diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");
//                        Copy(diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", Path.Combine(ConfigurationManager.AppSettings["DestinoXML"], "bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"));
//                        NLog.Info("arquivo bastao - {0}", diretorio_novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT envio bastao");
//                    }


//                    var final = DateTime.Now;
//                    DataFinalRotina = DateTime.Now;
//                    var final_cont = final - inicio;
//                    NLog.Info("Fim:" + DateTime.Now.ToString());
//                    NLog.Info("Time:" + final_cont);
//                }
//                catch (Exception ex)
//                {
//                    NLog.Info(ex.Message);
//                    using (XmlWriter writer_ = XmlWriter.Create(ConfigurationManager.AppSettings["Destino"] + "dados_nao_processados" + inicio.ToString("ddMMyyyy_HH") + ".xml"))
//                    {
//                        writer_.WriteStartDocument();
//                        writer_.WriteStartElement("ALM");

//                        while (reader.Read())
//                        {
//                            writer_.WriteStartElement("REG_ALM_" + DateTime.Now.ToString("HH"));

//                            for (int i = 0; i < reader.FieldCount; i++)
//                            {
//                                writer_.WriteStartElement(reader.GetName(i));
//                                writer_.WriteValue(reader[i].ToString());
//                                writer_.WriteEndElement();
//                            }

//                            writer_.WriteEndElement();
//                        }

//                        writer_.WriteEndElement();
//                        writer_.WriteEndDocument();
//                    }
//                }



//                #endregion

//                string validar = "DECLARE @dataIni_ VARCHAR(19), @dataFim_ VARCHAR(19), @data_ VARCHAR(20), @NUM_DIA_ int, @TZ_ INT, @SEQNUM_ INT " +
//                                  "SET @dataIni_ = '" + inicio.ToString("yyyy.MM.dd 01:00:00") + "'; " +
//                                  "SET @dataFim_ = '" + inicio.ToString("yyyy.MM.dd 23:59:59") + "'; " +
//                                  "SET @TZ_ = 3 " +
//                                  "SELECT count(*) " +
//                                  "FROM " + temporaria + " where DATA_LIGACAO_GMT BETWEEN @dataIni_ and @dataFim_ ";

//                SqlCommand commandRowCount_ = new SqlCommand(
//                validar,
//                sourceConnection);

//                // Perform a final count on the destination 
//                // table to see how many rows were added.
//                long countEnd = System.Convert.ToInt32(
//                    commandRowCount_.ExecuteScalar());
//                NLog.Info("Ending row count = {0}", countEnd);
//                NLog.Info("{0} rows were added.", countEnd - countStart);
//                //NLog.Info("Press Enter to finish.");
//                //Console.ReadLine();


//            }

//            DirectoryInfo dirDelete = new DirectoryInfo(ConfigurationManager.AppSettings["Destino"]);
//            NLog.Info("total deletar {0}", dirDelete.GetFiles().Length);
//            foreach (FileInfo fi in dirDelete.GetFiles())
//            {
//                if (fi.LastWriteTime < DateTime.Today.AddDays(-2))
//                {
//                    fi.Delete();
//                }
//                //if (fi.LastWriteTime <= DateTime.Today.AddDays(Convert.ToDouble(dia)) && fi.Extension.Contains(".csv"))



//            }
//        }

//        public static void EscreverBastao(string Message)
//        {
//            string dir = ConfigurationManager.AppSettings["Destino"];
//            string novo =  Path.Combine(dir, DataInicioRotina.Year.ToString(), DataInicioRotina.Month.ToString(), DataInicioRotina.Day.ToString());
//            NLog.Info("Arquivo Bastao: {0}", novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT");

//            StreamWriter sw = null;
//            try
//            {
//                if(!File.Exists(novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT"))
//                {

//                    sw = new StreamWriter(novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", true);
//                    sw.WriteLine("Nome do arquivo, Data e hora transmissao, Tamanho do arquivo");
//                    sw.Flush();
//                    sw.Close();
//                }
//                else
//                {
//                    sw = new StreamWriter(novo + "\\bastao_" + DataInicioRotina.ToString("yyyy-MM-dd") + ".TXT", true);
//                    sw.WriteLine(Message);
//                    sw.Flush();
//                    sw.Close();
//                }

//            }
//            catch (Exception)
//            {

//                throw;
//            }
//        }

//        public static string EscreverMetadata(
//                        object ID_CHAMADA,
//                        object CPF,
//                        object PROTOCOLO,
//                        object NUM_CONTRATO_GESTAO ,
//                        object NUMERO_DE_A,
//                        object RAMAL,
//                        object COD_AGENTE,
//                        object DATA_LIGACAO_GMT,
//                        object DATA_INICIO_GRAVACAO,
//                        object DATA_FIM_GRAVACAO,
//                        object TEMPO_GRAVACAO_EM_SEGUNDOS,
//                        object NOME_OPERADOR,
//                        object CELULA_OPERADOR,
//                        object SITE,
//                        object ARQUIVO,
//                        object TAMANHO_KB,
//                        object NUM_DE_PAUSA,
//                        object NUM_DE_CONF,
//                        object NUM_DE_TRANSF,
//                        string NAME_FILE
//            )
//        {
//            string dir = ConfigurationManager.AppSettings["Destino"];
//            string novo =  Path.Combine(dir, DataInicioRotina.Year.ToString(), DataInicioRotina.Month.ToString(), DataInicioRotina.Day.ToString());
//            try
//            {
//                string arquivo_ =  NAME_FILE + ".XML";
//                NLog.Info("Arquivo XML: {0}",Path.Combine(novo,  NAME_FILE + ".XML"));
//                XmlWriterSettings settings = new XmlWriterSettings();
//                settings.NamespaceHandling = NamespaceHandling.OmitDuplicates;

//                using (XmlWriter writer = XmlWriter.Create(Path.Combine(novo, NAME_FILE + ".XML"),settings))
//                {
//                    writer.WriteStartDocument();
//                    writer.WriteStartElement("AUDIOS");
//                    writer.WriteAttributeString("xmlns", "xsd", null, "http://www.w3.org/2001/XMLSchema-instance");
//                    writer.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");

//                    writer.WriteStartElement("ETL");

//                            // <-- These are new
//                            writer.WriteStartElement("ID_CHAMADA");
//                            writer.WriteValue(ID_CHAMADA);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("CPF");
//                            writer.WriteValue(CPF);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("PROTOCOLO");
//                            writer.WriteValue(PROTOCOLO);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("NUM_CONTRATO_GESTAO");
//                            writer.WriteValue(NUM_CONTRATO_GESTAO);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("NUMERO_DE_A");
//                            writer.WriteValue(NUMERO_DE_A);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("RAMAL");
//                            writer.WriteValue(RAMAL);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("COD_AGENTE");
//                            writer.WriteValue(COD_AGENTE);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("DATA_LIGACAO_GMT");
//                            writer.WriteValue(DATA_LIGACAO_GMT);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("DATA_INICIO_GRAVACAO");
//                            writer.WriteValue(DATA_INICIO_GRAVACAO);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("DATA_FIM_GRAVACAO");
//                            writer.WriteValue(DATA_FIM_GRAVACAO);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("TEMPO_GRAVACAO_EM_SEGUNDOS");
//                            writer.WriteValue(TEMPO_GRAVACAO_EM_SEGUNDOS);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("NOME_OPERADOR");
//                            writer.WriteValue(NOME_OPERADOR);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("CELULA_OPERADOR");
//                            writer.WriteValue(CELULA_OPERADOR);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("SITE");
//                            writer.WriteValue(SITE);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("ARQUIVO");
//                            writer.WriteValue(NAME_FILE+".WAV");
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("TAMANHO_KB");
//                            writer.WriteValue(TAMANHO_KB);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("NUM_DE_PAUSA");
//                            writer.WriteValue(NUM_DE_PAUSA);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("NUM_DE_CONF");
//                            writer.WriteValue(NUM_DE_CONF);
//                            writer.WriteEndElement();
//                            writer.WriteStartElement("NUM_DE_TRANSF");
//                            writer.WriteValue(NUM_DE_TRANSF);
//                            writer.WriteEndElement();

//                        writer.WriteEndElement();



//                    writer.WriteEndElement();
//                    writer.WriteEndDocument();
//                }
//                return arquivo_;
//            }
//            catch (Exception ex)
//            {
//               // EscreverErroLog("erro" + ex.Message);
//                var m = ex.Message;
//                return null;
//            }


//        }

//        public static string GetSize(long size)
//        {
//            string postfix = "Bytes";
//            long result = size;

//            if (size >= 1073741824)//more than 1 GB
//            {
//                result = size / 1073741824;
//                postfix = "GB";
//            }
//            else if (size >= 1048576)//more that 1 MB
//            {
//                result = size / 1048576;
//                postfix = "MB";
//            }
//            else if (size >= 1024)//more that 1 KB
//            {
//                result = size / 1024;
//                postfix = "KB";
//            }

//            return result.ToString();// result.ToString("F1") + " " + postfix;
//        }

//        public static void Copy(string inputFilePath, string outputFilePath)
//        {
//            int bufferSize = 1024 * 1024;

//            using (FileStream fileStream = new FileStream(outputFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite))
//            //using (FileStream fs = File.Open(<file-path>, FileMode.Open, FileAccess.Read, FileShare.Read))
//            {
//                FileStream fs = new FileStream(inputFilePath, FileMode.Open, FileAccess.ReadWrite);
//                fileStream.SetLength(fs.Length);
//                int bytesRead = -1;
//                byte[] bytes = new byte[bufferSize];

//                while ((bytesRead = fs.Read(bytes, 0, bufferSize)) > 0)
//                {
//                    fileStream.Write(bytes, 0, bytesRead);
//                }
//            }

//            if (!File.Exists(outputFilePath))
//            {
//                using (FileStream fileStream = new FileStream(outputFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite))
//                //using (FileStream fs = File.Open(<file-path>, FileMode.Open, FileAccess.Read, FileShare.Read))
//                {
//                    FileStream fs = new FileStream(inputFilePath, FileMode.Open, FileAccess.ReadWrite);
//                    fileStream.SetLength(fs.Length);
//                    int bytesRead = -1;
//                    byte[] bytes = new byte[bufferSize];

//                    while ((bytesRead = fs.Read(bytes, 0, bufferSize)) > 0)
//                    {
//                        fileStream.Write(bytes, 0, bytesRead);
//                    }
//                }
//            }

//        }
//    }
//}
