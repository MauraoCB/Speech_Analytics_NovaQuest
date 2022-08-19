-- SEGUINDO TEMPLATE: METADADOS (Excel)

Create Procedure SP_SELECT_METADADOS

/*DECLARE @DT_INICIO varchar(20), @DT_FINAL VARCHAR(20)

SET @DT_INICIO = '2022-08-16 02:59:59'
SET @DT_FINAL =  '2022-08-17 02:59:59'
*/
AS
  Begin 	
	IF OBJECT_ID('tempdb..#METADADOS') IS NOT NULL
	DROP TABLE #METADADOS

	IF OBJECT_ID('tempdb..#tmp_UIP') IS NOT NULL
	DROP TABLE #tmp_UIP

	IF OBJECT_ID('tempdb..#tmp_ALM') IS NOT NULL
	DROP TABLE #tmp_ALM

	IF OBJECT_ID('tempdb..#EnterpriseEmployee_Info') IS NOT NULL
	DROP TABLE #EnterpriseEmployee_Info

	 IF OBJECT_ID('tempdb..#tmp_AQM') IS NOT NULL
	DROP TABLE #tmp_AQM
	--=========================================
		--RESERVA OPERADORES
		--Busca 'startdate' e 'description' de configuração no UCC de todos os operadores cadastrados, insere em temporaria.
		SELECT
		  U.User_Id,
		  e.StartDate as [DataInicio],
		  B.Description as [Cargo],
		  W.Workgroup_Name,
		  w.TeamOwner as [SupervisorWorkgroup],
		  ULeader.User_Id as [Leader_User_Id],
		  CONCAT(U.User_F_Name, ' ', U.User_L_Name) AS [User_Name],
		  CONCAT(ULeader.User_F_Name, ' ', ULeader.User_L_Name) AS [Leader_Name]
	  INTO #EnterpriseEmployee_Info
	  FROM 

	  --select top 10 * from #EnterpriseEmployee_Info
	  UIP_TEL.[config_epro].[dbo].Users U  --select top 10 * from UIP_TEL.[config_epro].[dbo].Users
	  left join UIP_TEL.[UCCConfig].[dbo].[EnterpriseEmployee] e on (e.UserLogonName = U.User_Id) 
	  left join  UIP_TEL.[UCCConfig].[dbo].[BaseEnterpriseEntity] b on (e.BaseEnterpriseEntitySyntheticID = b.BaseEnterpriseEntitySyntheticID)
	  left join UIP_TEL.[config_epro].[dbo].Agent A on (U.User_Id = A.User_Id)
	  left join UIP_TEL.[config_epro].[dbo].Workgroup W on (A.Workgroup_Id = W.Workgroup_Id)
	  left join UIP_TEL.[config_epro].[dbo].users ULeader on (W.TeamOwner = ULeader.User_Id)
	 --=========================================
 
	 -- RESERVA AQM TEL
	SELECT me.mediaint as Id_Chave, --ID Chave da registro
			DATEADD(HOUR,-3,me.starttime) as Inicio_Gravacao, --Inicio da Gravação	
			DATEADD(HOUR,-3,me.endtime) as Fim_Gravacao, --Inicio da Gravação	
			me.filename AS Nome_Arquivo, --Nome do Arquivo

			SUBSTRING(me.hostid,CHARINDEX('.', me.hostid)+1,LEN(me.hostid)) AS seqnum,	
			SUBSTRING(me.hostid,0,CHARINDEX('.', me.hostid)) as callid 

	INTO #tmp_AQM

	FROM [AQM].[AQM01_Current].[dbo].[media] AS me

	WHERE me.starttime between @DT_INICIO AND @DT_FINAL
	ORDER BY callid

	--========================================= RESERVA UIP - TEL.
	SELECT 
 	
	-- descricao de CallTypeId padrao do UIP
	 --CASE 
		--	WHEN CD.CallTypeId = 1 THEN 'ENTRADA'
		--	WHEN CD.CallTypeId = 2 THEN 'ATIVA' --SAIDA e ATIVA são a mesma coisa.
		--	WHEN CD.CallTypeId = 8  THEN 'URA'
		--	WHEN CD.CallTypeId = 9  THEN 'MANUAL'
		--	WHEN CD.CallTypeId = 14  THEN 'ENTRADA'
		--	WHEN CD.CallTypeId = 15  THEN 'ENTRADA'
	 --END AS TipoChamada,

	 -- descricao de CallTypeId solicitada pelo cliente (1 = "ATIVA", 0 = CHAMADAS DIFERENTES DE "ATIVA"
	 CASE 
			WHEN CD.CallTypeId = 1 THEN '0'
			WHEN CD.CallTypeId = 2 THEN '1' --SAIDA e ATIVA são a mesma coisa.
			WHEN CD.CallTypeId = 8  THEN '1'
			WHEN CD.CallTypeId = 9  THEN '1'
			WHEN CD.CallTypeId = 14  THEN '0'
			WHEN CD.CallTypeId = 15  THEN '0'
	 END AS TipoChamada,
	 CD.SeqNum,
	 CD.CallId,

	--Isnull(DATEADD(HOUR, -3, CD.NearOffHookDt) , DATEADD(HOUR, -3, CD.FarOffHookDt) ) AS InicioChamada,		
	--isnull(DATEADD(HOUR, -3, CD.NearOnHookDt) , DATEADD(HOUR, -3, cd.FarOnHookDt) ) AS FimChamada,

	 isnull (DATEDIFF(SECOND, DATEADD(HOUR, -3, CD.NearOffHookDt), isnull( DATEADD(HOUR, -3, CD.NearOnHookDt) , DATEADD(HOUR, -3, cd.FarOnHookDt))), 0) AS [Duracao_chamada],

	  CASE CD.CallTypeId
		WHEN 1 THEN ISNULL(CD.ANI,0)
		WHEN 2 THEN ISNULL(CD.DialedNum,0)
		WHEN 8 THEN ISNULL(CD.ANI,0)
		WHEN 9 THEN ISNULL(CD.DialedNum,0)
		WHEN 14 THEN ISNULL(CD.ANI,0)
		WHEN 15 THEN ISNULL(CD.ANI,0)
	 END  AS Telefone,
	 isnull( CD.Service_Id , 0 ) as Service_Id, 

	 CD.User_Id AS Usuario,

	CD.CallerId,
	CD.DialedNum,
	CD.User_Id AS RAMAL,


	 --ACD.AgentDispId AS [ACD_AgentDispId],
	 ACD.SwitchDispId AS [ACD_SwitchDispId],

	 --AOD.AgentDispId AS [AOD_AgentDispId],
	 AOD.SwitchDispId AS [AOD_SwitchDispId],

	 MCD.SwitchDispId AS [MCD_SwitchDispId],

	CASE
		WHEN AOD.AgentDispId IS NULL AND ACD.AgentDispId IS NULL THEN MCD.SwitchDispId
		WHEN ACD.AgentDispId IS NULL THEN AOD.AgentDispId
		WHEN AOD.AgentDispId IS NULL THEN ACD.AgentDispId
	ELSE
		'Diposicao nao mapeada'
	END AS [mapeamento_disposicoes_agente],

	CASE
		WHEN ACD.SwitchDispId IS NULL THEN AOD.SwitchDispId
		WHEN AOD.SwitchDispId IS NULL THEN ACD.SwitchDispId
	ELSE
		'Diposicao nao mapeada'
	END AS [mapeamento_disposicoes_discador]

	INTO #tmp_UIP
	--	SELECT * FROM #tmp_UIP
		FROM
			  UIP_TEL.[detail_epro].[dbo].CallDetail   AS CD with(nolock)
	LEFT JOIN UIP_TEL.DETAIL_EPRO.DBO.ACDCALLDETAIL AS ACD WITH(NOLOCK) ON (CD.SEQNUM = ACD.SEQNUM AND CD.CALLID = ACD.CALLID AND CD.CallStartDt BETWEEN @DT_INICIO AND @DT_FINAL)
	LEFT JOIN UIP_TEL.DETAIL_EPRO.DBO.AODCALLDETAIL AS AOD WITH(NOLOCK) ON (CD.SEQNUM = AOD.SEQNUM AND CD.CALLID = AOD.CALLID AND CD.CallStartDt BETWEEN @DT_INICIO AND @DT_FINAL)
	LEFT JOIN UIP_TEL.DETAIL_EPRO.DBO.MANUALCALLDETAIL AS MCD WITH(NOLOCK) ON (CD.SEQNUM = MCD.SEQNUM AND CD.CALLID = MCD.CALLID AND CD.CallStartDt BETWEEN @DT_INICIO AND @DT_FINAL)
	   WHERE 
	   CD.CallStartDt BETWEEN @DT_INICIO AND @DT_FINAL 
	   AND CD.User_Id IS NOT NULL

	   -- RETIRADA DE SERVIÇOID DE SERVIÇO SOLICITADA PELO MAICON (17/08/2022)
	   AND CD.service_id NOT IN ('2000013')
	--=================================================================================


	--========================RESERVA ALM - TEL

	SELECT * 
	INTO #tmp_ALM

	FROM 
	[ALM_TEL].[meldb].[dbo].[cl_contact_event] 

	WHERE time_of_contact BETWEEN @DT_INICIO AND @DT_FINAL
	AND agent_login_name IS NOT NULL

	--RESERVA TABELA [MAILING_IMPORT] do banco do Cortex, baseado no ultimo mailing inserido atraves do proprio Cortex.

	select
	--UIP.seqnum, 
	--UIP.callid,

	'Ativo de Rentabilização' AS [CANAL DE VENDA],
	TipoChamada AS [ATIVO OU RECEPTIVO],
	UIP.CallerId AS [NUMERO DE A],
	Telefone AS [NUMERO DE B],

	FORMAT(ME.Inicio_Gravacao, 'dd/MM/yyyy hh:mm:ss') AS [INICIO DA LIGAÇÃO],
	FORMAT(ME.Fim_Gravacao, 'dd/MM/yyyy hh:mm:ss') AS [FIM DA LIGAÇÃO],

	--FORMAT(UIP.InicioChamada, 'dd/MM/yyyy hh:mm:ss') AS [INICIO DA LIGAÇÃO],
	--FORMAT(UIP.FimChamada, 'dd/MM/yyyy hh:mm:ss') AS [FIM DA LIGAÇÃO],

	UIP.Usuario AS [RAMAL],
	'Rentabilização' AS [OPERACAO],
	'Tel Centro de Contatos' AS [EPS / PARCEIRO],
	'73663114/0019-14' AS [CNPJ PARCEIRO],

	MAIL_IMP.CEP AS [CEP DO CLIENTE],
	MAIL_IMP.NOME AS [CLIENTE],
	MAIL_IMP.CD_NET AS [CONTRATO],

	' ' AS [Nº PROPOSTA],
	' ' AS [CPF / CNPJ DO CLIENTE],

	DADOS_AGENT.LOGIN_DO_VENDEDOR,
	DADOS_AGENT.CPF_DO_VENDEDOR,
	DADOS_AGENT.LOGIN_DO_AGENTE,
	DADOS_AGENT.NOME_DO_AGENTE,

	CASE 
		WHEN DISP_TEL_AGE.[Disposition_Desc] LIKE 'VENDA' THEN CONVERT(varchar(10), me.Fim_Gravacao, 103)
		ELSE ' '
	END AS [DATA DA VENDA],

	MAIL_IMP.POSSUI_DEB_CONTA AS [FORMA DE PAGAMENTO],

	CONCAT(SUBSTRING(ME.Nome_Arquivo,1, 8), UIP.callid, UIP.seqnum) AS [ID DA GRAVAÇÃO],

	--UPDATE #TMP_RLFINAL_BTG			SET Nome_Arquivo_Novo = CONCAT(UPPER(rlf.USUARIO),'_',SUBSTRING(rlf.nome_arquivo,1,8),'_',FORMAT(rlf.ENCER, 'HHmmss'),'.wav')
	--		FROM #TMP_RLFINAL_BTG rlf with(nolock)
	--			WHERE rlf.nome_arquivo <> ''

	CASE WHEN MAIL_IMP.[MENSALIDADE_PO] < 1 THEN 'NAO'
		ELSE 'Sim'
	END AS [PONTO_ADICIONAL],

	' ' AS [PRODUTO],

	MAIL_IMP.[MIX_PRODUTOS] AS [TIPO DE ASS DOMICILIO],
	MAIL_IMP.[CIDADE] AS [CIDADE],

	DEPARA_REGIONAL.[REGIONAL],
	DEPARA_REGIONAL.[CLUSTER],
	DEPARA_REGIONAL.[SUBCLUSTER],

	CASE 
		WHEN DISP_TEL_AGE.[Disposition_Desc] NOT LIKE 'VENDA' AND DISP_TEL_AGE.[Disposition_Desc] IS NOT NULL THEN DISP_TEL_AGE.[COD_DBM]
		WHEN DISP_TEL_AGE.Disposition_Desc = 'FAX-CAIXA POSTAL-SECRETÁRIA' THEN '20170149'
		WHEN DISP_TEL_AGE.Disposition_Desc = 'AGENDAMENTO SOLICITADO PELO CLIENTE' THEN '20170081'
		WHEN DISP_TEL_AGE.Disposition_Desc = 'CONCORRÊNCIA - NEXTEL - MINUTOS' THEN '20170085'
		WHEN DISP_TEL_AGE.Disposition_Desc = 'PLANO - ACHA CARO PLANO DEPENDENTES' THEN '20170125'
		WHEN DISP_TEL_AGE.Disposition_Desc IS NULL THEN '2019121846'
		ELSE '-'
	END AS [TABULAÇÃO - MOTIVO DE NÃO VENDA],

	MAIL_IMP.[TELEFONE_RESIDENCIAL] AS [TELEFONE 1],
	'' AS [TELEFONE 2],
	'' AS [TELEFONE 3],
	'' AS [OFERTA],

	ME.Nome_Arquivo as [NOME_ARQUIVO_ORIGINAL]

	--INSERE TODA A CONSULTA TRATADA E FILTRADA NUMA TABELA TEMPORARIA VOLTADA A INSERCAO.
	INTO #METADADOS

	from  [CORTEXDB].[TELData].[dbo].[MAILING_IMPORT] AS MAIL_IMP WITH(NOLOCK) --SELECT * FROM [CORTEXDB].[TELData].[dbo].[MAILING_IMPORT] ORDER BY ACCOUNT
	INNER JOIN [CORTEXDB].[TELData].[dbo].[ANSWERED] AS ANS ON (MAIL_IMP.ACCOUNT = ANS.ACCOUNT) --SELECT * FROM [CORTEXDB].[TELData].[dbo].[ANSWERED] ORDER BY ACCOUNT
	INNER JOIN #tmp_ALM AS CE WITH(NOLOCK) ON (MAIL_IMP.CD_OPERADORA = CE.account_number COLLATE Latin1_General_CI_AS)  --AND MAIL_IMP.CD_OPERADORA = CE.contact_list_name
	INNER JOIN #tmp_UIP AS UIP ON (CE.seqnum = UIP.seqnum and CE.callid = UIP.callid)
	INNER JOIN #tmp_AQM AS ME WITH(NOLOCK) ON (UIP.callid = ME.callid AND CE.seqnum = ME.seqnum)

	-- disps ALM INNER JOIN [ALM_TEL].[meldb].[dbo].[cq_disposition] AS DISP ON (CE.dialer_disposition = DISP.status_code)--status_code = dialer_disposition //"name" = disposicao chamada
	--LEFT JOIN UIP_TEL.config_epro.dbo.Disposition AS D ON (UIP.mapeamento_disposicoes_discador = D.disp_id)
	--LEFT JOIN [TELData].[dbo].[DISPS_TELEFONIA_TEL] AS DISP_TEL_TEL ON (D.disp_c = DISP_TEL_TEL.disp_c COLLATE Latin1_General_CI_AS)

	LEFT JOIN UIP_TEL.config_epro.dbo.Disposition AS D2 ON (UIP.mapeamento_disposicoes_agente = D2.disp_id)
	LEFT JOIN [TELData].[dbo].[DISPS_AGENTES_TEL] AS DISP_TEL_AGE ON (D2.Disposition_Desc = DISP_TEL_AGE.Disposition_Desc COLLATE Latin1_General_CI_AS) --SELECT * FROM [TELData].[dbo].[DISPS_AGENTES_TEL]
	LEFT JOIN [TELReports].[dbo].[DADOS_AGENTES_TEL] AS DADOS_AGENT ON (UIP.Usuario = DADOS_AGENT.LOGIN_DO_AGENTE COLLATE Latin1_General_CI_AS)
	LEFT JOIN [TELReports].[dbo].[DEPARA_REGIONAL_TEL] AS DEPARA_REGIONAL ON (MAIL_IMP.CIDADE = DEPARA_REGIONAL.Mailing)
	LEFT JOIN #EnterpriseEmployee_Info AS EEI WITH(NOLOCK) ON (UIP.Usuario = EEI.User_Id)

	--AND agent_login_name IS NOT NULL
	AND CE.time_of_contact BETWEEN @DT_INICIO AND @DT_FINAL
	AND CE.tenant_id = 2

	--WHERE  
	--CE.agent_login_name IS NOT NULL

	SELECT DISTINCT * FROM #METADADOS
	WHERE [NUMERO DE A] NOT IN ('0')
	ORDER BY 5
  End
