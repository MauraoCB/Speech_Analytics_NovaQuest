using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Speech_Analytics.Service
{

    public partial class Service1 : ServiceBase
    {
        private static NLog.Logger NLog = LogManager.GetCurrentClassLogger();
        private Timer timer = null;
        public string[] Horario_determinado_HoraHora;
        public string[] Horario_determinado_Diario;
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            timer = new Timer();
            this.timer.Interval = 1000;
            this.timer.Elapsed += new System.Timers.ElapsedEventHandler(this.timer1_Tick);
            timer.Enabled = true;
            NLog.Info("Startando Serviço");
            try
            {
                //setando pasta do projeto para enxergar database local
                //  AppDomain.CurrentDomain.SetData("DataDirectory", Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ""));              

                Horario_determinado_HoraHora = ConfigurationManager.AppSettings["HoraHora"].Split(',');
                Horario_determinado_Diario = ConfigurationManager.AppSettings["HoraDiario"].Split(',');
            }
            catch (Exception ex)
            {
                NLog.Error(ex.Message);
            }
        }
        private void timer1_Tick(object sender, ElapsedEventArgs e)
        {
            DateTime Horario_atual_dt = DateTime.Now;
            string Horario_atual = DateTime.Now.ToString("HH:mm:ss");

            Horario_determinado_HoraHora = ConfigurationManager.AppSettings["HoraHora"].Split(',');
            Horario_determinado_Diario = ConfigurationManager.AppSettings["HoraDiario"].Split(',');

            NLog.Warn(String.Format("Processo Rodando verificação!{0} - task hours{1}", DateTime.Now, Horario_determinado_HoraHora));
            if (Horario_determinado_HoraHora.Contains(Horario_atual))
            {
                string mensagemEmailAcompanhamento = "<table class=\"table\">  <tr> <th> Relatório </th>   <th> Status </th>  <th> Diretório </th>  </tr>";//string.Empty;

                try
                {
                    var task2 = Task.Factory.StartNew((x) =>
                    {
                        NLog.Info(String.Format("Processo Rodando!{0} - task{1}", DateTime.Now, x));
                        // Wait for ALL tasks to finish
                        // Control will block here until all 3 finish in parallel

                        // mensagemEmailAcompanhamento = mensagemEmailAcompanhamento + RTN_ANALITICO.Start(Horario_atual, configA);
                        Speech_Analytics.Dados.Dados.ConsultaAQM(Horario_atual_dt);

                        NLog.Info(String.Format("Processo Parando!{0}", DateTime.Now));
                        return string.Format("executado {0}", DateTime.Now);
                    }, TaskCreationOptions.AttachedToParent | TaskCreationOptions.LongRunning
         );
                    Task.WaitAll(new[] { task2 });



                    var data = task2.AsyncState as string;
                    if (data != null || task2.Status == TaskStatus.RanToCompletion)
                    {
                        NLog.Debug("Task #{0} ",
                                          task2.Result);
                    }


                }
                catch (Exception ex)
                {

                    NLog.Error("Exceção:" + ex.Message);
                }


            }
            //if (Horario_determinado_HoraHora.Contains(Horario_atual))
            //{

            //    NLog.Info(String.Format("Processo Hora Hora Rodando!{0}", Horario_atual));
            //    string mensagemEmailAcompanhamento = "<table class=\"table\">  <tr> <th> Relatório </th>   <th> Status </th>  <th> Diretório </th>  </tr>";//string.Empty;

            //    try
            //    {

            //        //mensagemEmailAcompanhamento = mensagemEmailAcompanhamento + RTN_ACIONAMENTO.Start(Horario_atual, configA);
            //    }
            //    catch (Exception ex)
            //    {
            //       NLog.Error("Exceção:" + ex.Message);
            //    }

            //}

            //if (Horario_determinado_Diario.Contains(Horario_atual))
            //{
            //    string mensagemEmailAcompanhamento = "<table class=\"table\">  <tr> <th> Relatório </th>   <th> Status </th>  <th> Diretório </th>  </tr>";//string.Empty;

            //    try
            //    {
            //        NLog.Info(String.Format("Processo Diario Rodando!{0}", Horario_atual));
            //        // mensagemEmailAcompanhamento = mensagemEmailAcompanhamento + RTN_ANALITICO.Start(Horario_atual, configA);
            //        Speech_Analytics.Dados.Dados.Consulta();
            //    }
            //    catch (Exception ex)
            //    {

            //        NLog.Error("Exceção:" + ex.Message);
            //    }


            //}


        }

        protected override void OnStop()
        {
            timer.Enabled = false;
            NLog.Info("Serviço Parando");
        }

        protected override void OnContinue()
        {
            NLog.Info("Serviço Continue");



            Horario_determinado_HoraHora = ConfigurationManager.AppSettings["HoraHora"].Split(',');
            Horario_determinado_Diario = ConfigurationManager.AppSettings["HoraDiario"].Split(',');
        }
    }
}
