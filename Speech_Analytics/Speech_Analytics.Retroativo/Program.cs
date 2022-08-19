using NLog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Speech_Analytics.Retroativo
{
    class Program
    {
        private static NLog.Logger NLog = LogManager.GetCurrentClassLogger();///esse erro so resolve removendo e inserindo novamente
        static void Main(string[] args)
        {
            //REFATORAÇÃO da rotina x
           DateTime myDate = DateTime.ParseExact("" + ConfigurationManager.AppSettings["dataInicio"] + ",531", "yyyy-MM-dd HH:mm:ss,fff",
                                      System.Globalization.CultureInfo.InvariantCulture);

            DateTime myDateF = DateTime.ParseExact("" + ConfigurationManager.AppSettings["dataFinal"] + ",531", "yyyy-MM-dd HH:mm:ss,fff",
                                     System.Globalization.CultureInfo.InvariantCulture);

            DateTime dataI = myDate;
            DateTime dataF = myDateF;

            while (dataI.Date <= myDateF.Date)
            {

                NLog.Info("Iniciando {0} -{1}", dataI, dataF);
                //dados meia em meia hora
                var I = dataI.ToString("yyyy.MM.dd 01:00:00");
                //var I = dataI.ToString("yyyy.MM.dd 08:mm:00");
                var F = dataF.ToString("yyyy.MM.dd 23:59:59");
               // p.QueryInstacia(ip, I, F, dataF);

               Speech_Analytics.Dados.Dados.Consulta_retroativa(dataI, dataF);
               // Speech_Analytics.Dados.Dados.ConsultaAQM(dataI);

                dataI = dataI.AddDays(1);
                dataF = dataF.AddDays(1);
            }
           
        }
    }
}
