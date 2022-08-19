
IF OBJECT_ID('tempdb..##TEMP_SPEECH') IS NOT NULL
    DROP TABLE ##TEMP_SPEECH

DECLARE @dataIni VARCHAR(20), @dataFim VARCHAR(20), @NUM_DIA int 
SET @NUM_DIA = 0;

SET @dataIni = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:00:00'; 
SET @dataFim = CONVERT(VARCHAR(20), GETDATE() -@NUM_DIA , 102 ) + ' 11:09:59'; 
SELECT       
CONCAT(cast(ISNULL(CD.CallId,0) as varchar),cast(ISNULL(CD.SeqNum,0) as varchar)) AS ID_CHAMADA,
ISNULL(IIF(MD.Param3 = '','0',MD.Param3),0) 'CPF',
ISNULL(RC.SeqNum,0) 'PROTOCOLO',
ISNULL(IIF(MD.Param11 = '','0',MD.Param11),0) 'NUM_CONTRATO_GESTAO',
ISNULL(CD.DialedNum,IIF(LEN(CD.ANI)>5, CD.ANI,CD.DNIS)) 'NUMERO_DE_A',
CD.Station 'RAMAL',
CD.User_Id 'COD_AGENTE', 
DATEADD(HOUR,-3,CD.CallStartDt) 'DATA_LIGACAO_GMT',
Isnull(DATEADD(HOUR, -3, CD.FarOffHookDt) , DATEADD(HOUR, -3, CD.NearOffHookDt) ) 'DATA_INICIO_GRAVACAO',		
Isnull(DATEADD(HOUR, -3, CD.FarOnHookDt) , DATEADD(HOUR, -3, CD.NearOnHookDt) ) 'DATA_FIM_GRAVACAO',
isnull(RC.Recording_Secs, 0) 'TEMPO_GRAVACAO_EM_SEGUNDOS', 
SUBSTRING(CONCAT(UPPER(U.User_F_Name),' ',UPPER(U.User_L_Name)),0,30)  'NOME_OPERADOR',
S.Service_c 'CELULA_OPERADOR',
'MendesCunha_VINHEDO' 'SITE', 
CONCAT(RC.[Seq#] , '.wav')'ARQUIVO',
RC.Recording_Bytes /1000 'TAMANHO_KB',
--RC.Recording_Secs,
'0' 'NUM_DE_PAUSA', 
'0' 'NUM_DE_CONF',
'0' 'NUM_DE_TRANSF'
,CD.CallTypeId
--,*
--SELECT * from detail_epro..AODCallDetail
INTO ##TEMP_SPEECH
FROM          
		detail_epro..CallDetail CD WITH(NOLOCK) INNER JOIN
		[newrecordings_epro].[dbo].[Recordings] RC WITH(NOLOCK) ON(CD.SeqNum = RC.SeqNum AND CD.CallId = RC.CallId) LEFT JOIN
		config_epro..Users U WITH(NOLOCK) ON (CD.User_Id = U.User_Id) LEFT JOIN
		config_epro..service S WITH(NOLOCK) ON (S.Service_Id = CD.Service_Id) LEFT JOIN
		[detail_epro].[dbo].[MediaDataDetail] MD WITH(NOLOCK) ON (RC.SeqNum = MD.SeqNum AND RC.CallId = MD.CallId)

WHERE      (DATEADD(HOUR,-3,CD.CallInsertDt) between @dataIni and @dataFim and S.Service_Id in('1',
'5',
'6',
'11',
'12',
'13',
'14',
'15',
'16',
'17',
'22',
'26',
'31',
'47',
'49',
'58',
'72',
'74',
'76',
'77',
'78',
'79',
'85',
'92',
'93',
'95',
'98',
'99'
))-- LIKE '%bv%') and  CD.CallTypeId = '2' --or me.starttime between @dataIni and @dataFim and que.queuename LIKE '%bv%' )
--order by me.mediaint DESC
--select * from ##TEMP_SPEECH_oi where cpf = '' or NUM_CONTRATO_GESTAO = ''
SELECT TS.ID_CHAMADA, TS.CPF, TS.PROTOCOLO, TS.NUM_CONTRATO_GESTAO, TS.NUMERO_DE_A, TS.RAMAL, TS.COD_AGENTE, TS.DATA_LIGACAO_GMT, TS.DATA_INICIO_GRAVACAO,
TS.DATA_FIM_GRAVACAO, TS.TEMPO_GRAVACAO_EM_SEGUNDOS, TS.NOME_OPERADOR, TS.CELULA_OPERADOR, TS.SITE, TS.ARQUIVO, TS.TAMANHO_KB,TS.NUM_DE_PAUSA,
TS.NUM_DE_CONF,TS.NUM_DE_TRANSF FROM ##TEMP_SPEECH TS
WHERE TS.TEMPO_GRAVACAO_EM_SEGUNDOS >= 30 and TS.TEMPO_GRAVACAO_EM_SEGUNDOS != '' and TS.TEMPO_GRAVACAO_EM_SEGUNDOS IS NOT NULL