IF OBJECT_ID('tempdb..##TEMP_SPEECH') IS NOT NULL
    DROP TABLE ##TEMP_SPEECH

	IF OBJECT_ID('tempdb..#TEMP_CTL') IS NOT NULL
    DROP TABLE #TEMP_CTL
	IF OBJECT_ID('tempdb..#TMP_GERAL_CTL') IS NOT NULL
    DROP TABLE #TMP_GERAL_CTL
	IF OBJECT_ID('tempdb..#TMP_Services') IS NOT NULL
    DROP TABLE #TMP_Services

DECLARE @dataIni VARCHAR(20), @dataFim VARCHAR(20), @NUM_DIA int 
SET @NUM_DIA = 1;

SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 01:00:00'; 
SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; 

SELECT LS.Service_Id into #TMP_Services FROM [config_epro].[dbo].[Service] AS LS with(nolock) where (LS.Service_c LIKE '%BV_%')

SELECT
		me.mediaint as Id_Chave, --ID Chave da registro
		DATEADD(HOUR,-3,me.starttime) as Inicio_Gravacao, --Inicio da Gravação
		DATEADD(HOUR,-3,me.endtime) as Fim_Gravacao, --Fim da Gravação
		me.callduration as Duracao, --Duração da gravação
		--ty.mediadesc as Tipo_Ligacao, -- Tipo de ligação
		me.ani as Numero_Outbound, --Numero que Gerou a Ligação
		me.dnis as Numero_Inbound, --Numero que recebeu a Ligação
		me.filename AS Nome_Arquivo, --Nome do Arquivo
		iq.username as Usuario, --Nome do usuario
		--me.extension as Ramal, --Nume do Ramal
		ap.name as Aplicacao, --Nome da aplicação	   
		mcut.custominfovalue as Id_Cliente ,--Account ou Contrato do Cliente   
		 SUBSTRING(me.hostid,CHARINDEX('.', me.hostid)+1,LEN(me.hostid)) AS SeqNum,	
	    SUBSTRING(me.hostid,0,CHARINDEX('.', me.hostid)) as CallId 
		
	INTO #TEMP_CTL
	FROM AQM.AQM01_Current.dbo.media me (nolock)
		INNER JOIN AQM.AQM01_Current.dbo.media_custominfo mcut ON (me.mediaid = mcut.mediaid)
		INNER JOIN AQM.AQM01_Current.dbo.custominfo inf ON (inf.custominfoid = mcut.custominfoid)
		INNER JOIN AQM.AQM01_Current.dbo.iqmuser iq on (me.userid = iq.userid)
		INNER JOIN AQM.AQM01_Current.dbo.application ap on (me.applicationid = ap.applicationid)
		INNER JOIN AQM.AQM01_Current.dbo.mediatype ty on (me.mediatypeid = ty.mediatypeid)
	WHERE DATEADD(HOUR,-3,me.starttime) >= @dataIni and  DATEADD(HOUR,-3,me.endtime) <= @dataFim
		AND inf.displayname in ('Account')

		
	select 
		CAST(me.mediaint as varchar) as Id_Chave, --ID Chave da registro
		DATEADD(HOUR,-3,me.starttime) as Dt_Inicio, --Inicio da Gravação
		DATEADD(HOUR,-3,me.endtime) as Dt_Fim, --Fim da Gravação
		CAST(me.callduration  as varchar) as Duracao, --Duração da gravação
		--ty.mediadesc as Tipo_Ligacao, -- Tipo de ligação
		CAST(me.ani as varchar) as Numero_Outbound, --Numero que Gerou a Ligação
		CAST(me.dnis as varchar)  as Numero_Inbound, --Numer que recebeu a Ligação
		me.filename as Nome_Arquivo, --Nome do Arquivo
		iq.username as Usuario, --Nome do usuario
		--me.extension as Ramal, --Nume do Ramal
		ap.name as Aplicacao, --nome da aplicação
	--	CAST(tmp.Id_Cliente as varchar)  as Account,
		 SUBSTRING(me.hostid,CHARINDEX('.', me.hostid)+1,LEN(me.hostid)) AS SeqNum,	
	    SUBSTRING(me.hostid,0,CHARINDEX('.', me.hostid)) as CallId	 
		
	INTO #TMP_GERAL_CTL
	from AQM.AQM01_Current.dbo.media me (nolock)
		INNER JOIN AQM.AQM01_Current.dbo.iqmuser iq (nolock) on (me.userid = iq.userid)
		INNER JOIN AQM.AQM01_Current.dbo.application ap (nolock) on (me.applicationid = ap.applicationid)
		INNER JOIN AQM.AQM01_Current.dbo.mediatype ty (nolock) on (me.mediatypeid = ty.mediatypeid)
		INNER JOIN AQM.AQM01_Current.dbo.queue que (nolock) on (me.takenqueueid = que.queueid)
		--INNER JOIN #TEMP_CTL tmp  on (tmp.Id_Chave = me.mediaint)
	where DATEADD(HOUR,-3,me.starttime) >= @dataIni and  DATEADD(HOUR,-3,me.endtime) <= @dataFim
		and me.filename is not null
	
SELECT       
CONCAT(cast(ISNULL(CD.CallId,0) as varchar),cast(ISNULL(CD.SeqNum,0) as varchar)) AS ID_CHAMADA,
ISNULL(IIF(MD.Param3 = '','0',MD.Param3),0) 'CPF',
ISNULL(CD.SeqNum,0) 'PROTOCOLO',
ISNULL(IIF(MD.Param12 = '','0',MD.Param12),0) 'NUM_CONTRATO_GESTAO',
ISNULL(CD.DialedNum,IIF(LEN(CD.ANI)>5, CD.ANI,CD.DNIS)) 'NUMERO_DE_A',
CD.Station 'RAMAL',
CD.User_Id 'COD_AGENTE', 
DATEADD(HOUR,-3,CD.CallStartDt) 'DATA_LIGACAO_GMT',
Isnull(DATEADD(HOUR, -3, CD.FarOffHookDt) , DATEADD(HOUR, -3, CD.NearOffHookDt) ) 'DATA_INICIO_GRAVACAO',		
Isnull(DATEADD(HOUR, -3, CD.FarOnHookDt) , DATEADD(HOUR, -3, CD.NearOnHookDt) ) 'DATA_FIM_GRAVACAO',
isnull(aqm.Duracao, 0) 'TEMPO_GRAVACAO_EM_SEGUNDOS', 
SUBSTRING(CONCAT(UPPER(U.User_F_Name),' ',UPPER(U.User_L_Name)),0,30)  'NOME_OPERADOR',
S.Service_c 'CELULA_OPERADOR',
'MendesCunha_VINHEDO' 'SITE', 
aqm.Nome_Arquivo 'ARQUIVO',
1000 'TAMANHO_KB',
--RC.Recording_Secs,
'0' 'NUM_DE_PAUSA', 
'0' 'NUM_DE_CONF',
'0' 'NUM_DE_TRANSF'
,CD.CallTypeId,aqm.Nome_Arquivo
--,*
--SELECT * from #TMP_GERAL_CTL
INTO ##TEMP_SPEECH
FROM          
		detail_epro..CallDetail CD WITH(NOLOCK) left JOIN
		--[newrecordings_epro].[dbo].[Recordings] RC WITH(NOLOCK) ON(CD.SeqNum = RC.SeqNum AND CD.CallId = RC.CallId) LEFT JOIN
		config_epro..Users U WITH(NOLOCK) ON (CD.User_Id = U.User_Id) LEFT JOIN
		config_epro..service S WITH(NOLOCK) ON (S.Service_Id = CD.Service_Id) LEFT JOIN
		 #TMP_GERAL_CTL aqm with(nolock) ON (CD.SeqNum = aqm.SeqNum COLLATE Latin1_General_CI_AS AND CD.CallId = aqm.CallId COLLATE Latin1_General_CI_AS)	
		 left join 
		[detail_epro].[dbo].[MediaDataDetail] MD WITH(NOLOCK) ON (MD.SeqNum = aqm.SeqNum COLLATE Latin1_General_CI_AS AND MD.CallId = aqm.CallId COLLATE Latin1_General_CI_AS)

		INNER JOIN #TMP_Services AS SER WITH(NOLOCK) ON (CD.Service_Id = SER.Service_Id)
		
WHERE      (DATEADD(HOUR,-3,CD.CallInsertDt) between @dataIni and @dataFim and CD.FarOffHookDt IS NOT NULL --BV_
--AND MD.AgentDispId not in(3,94,95,108,122,123,130,131,129) 

)-- LIKE '%bv%') and  CD.CallTypeId = '2' --or me.starttime between @dataIni and @dataFim and que.queuename LIKE '%bv%' )
--order by me.mediaint DESC
--select * from ##TEMP_SPEECH_oi where cpf = '' or NUM_CONTRATO_GESTAO = ''
SELECT TS.ID_CHAMADA, TS.CPF,
 TS.PROTOCOLO, 
 TS.NUM_CONTRATO_GESTAO, 
 TS.NUMERO_DE_A, TS.RAMAL, TS.COD_AGENTE, TS.DATA_LIGACAO_GMT, TS.DATA_INICIO_GRAVACAO,
TS.DATA_FIM_GRAVACAO,
 TS.TEMPO_GRAVACAO_EM_SEGUNDOS,
  TS.NOME_OPERADOR, TS.CELULA_OPERADOR, TS.SITE, 
 TS.ARQUIVO,
  TS.TAMANHO_KB,
  TS.NUM_DE_PAUSA,
TS.NUM_DE_CONF,TS.NUM_DE_TRANSF FROM ##TEMP_SPEECH TS
WHERE TS.TEMPO_GRAVACAO_EM_SEGUNDOS >= 31 and TS.TEMPO_GRAVACAO_EM_SEGUNDOS != '' and TS.TEMPO_GRAVACAO_EM_SEGUNDOS IS NOT NULL
