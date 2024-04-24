import pip
pip.main(['install','mysql-connector-python-rf'])
import mysql.connector 
pip.main(['install','pyodbc'])
import pyodbc 


# Função para conectar ao MySQL
def conectar_mysql(host="localhost", user="root", password="", port=3306, database="alertas_bi"):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        print("Conectado ao banco de dados MySQL!")
        return conn
    except mysql.connector.Error as err:
        print(f"Erro de conexão com o MySQL: {err}")
        return None

# Função para conectar ao SQL Server
def conectar_sql_server(server="192.168.100.105", database="agrimanager", user="controladoria", password="Senha@2022"):
    try:
        conn = pyodbc.connect(
            'DRIVER={SQL Server};'
            'SERVER='+server+';'
            'DATABASE='+database+';'
            'UID='+user+';'
            'PWD='+password
        )
        print("Conectado ao banco de dados SQL Server!")
        return conn
    except pyodbc.Error as err:
        print(f"Erro de conexão com o SQL Server: {err}")
        return None
    
# Função para obter todos os registros da tabela do SQL Server
def obter_registros_sql_server():
    conn = conectar_sql_server()
    if conn:
        cursor = conn.cursor()
        consulta_sql = """WITH RESUMO_DOC AS (
SELECT DC.sequencial      ID_DOCUMENTO,
	   ID.SEQID           ID_ITEM_DOC,
       DC.empresa         ID_EMPRESA,
       DC.emitente        ID_FILIAL,
       DC.safra           ID_SAFRA,
       --(SELECT CP.safra FROM compras CP WHERE  CP.sequencialoc = ID.sequencialoc) ID_SAFRA_OC,
       --TD.tipo 		   TIPO_MOV,
       DC.numdoc          NR_DOCUMENTO,
       DC.serie           SERIE,
       DC.pessoa          ID_PESSOA,
       dc.IE              ID_DET_PESSOA,
       DC.tipodoc         ID_TIPO_DOC,--- E = SAIDA / S = ENTRADA
       CASE WHEN td.Tipo = 'E' THEN 'SAIDA' ELSE 'ENTRADA' END 
       					  TIPO_MOV,
       TD.Descricao       TIPO_DOC,
       DC.tipooperacao    ID_OPERACAO,
       DC.datadig         DT_LCTO,
       DC.emissao         DT_EMISSAO,
       DC.datarecebimento DT_ENTRADA,
       DC.datalivro       DT_LIVRO_FISCAL,
       ID.dtrecebe        DT_ACEITE,
       DC.UnidArmaz       UA,
       ROUND(DC.valortotal,2) / 1  
                              VLR_TOTAL,
       ISNULL(DC.quantidade,1) / 1  QTD_TOTAL,
       ID.produto         COD_MATERIAL,
       ISNULL(ID.qtd,0) / 1
                          QTD_ITEM,
       CASE
         WHEN ID.unidade IN ( 1, 2, 12, 7, 22, 23, 30, 31, 32, 51 ) THEN ID.qtd * 1
         WHEN ID.unidade IN ( 54, 55 ) THEN ID.qtd * 2.5
         WHEN ID.unidade IN ( 34, 48, 49 ) THEN ID.qtd * 5
         WHEN ID.unidade IN ( 35 ) THEN ID.qtd * 10
         WHEN ID.unidade IN ( 27 ) --RTRIM(UN.MASCARA) IN ('B 80')
       THEN ID.qtd * 15
         WHEN ID.unidade IN ( 33, 36 ) THEN ID.qtd * 20
         WHEN ID.unidade IN ( 26 ) THEN ID.qtd * 40
         WHEN ID.unidade IN ( 25 ) THEN ID.qtd * 60
         WHEN ID.unidade IN ( 41, 52, 53, 57 ) --RTRIM(UN.MASCARA) IN ('B 20')
       THEN ID.qtd * 200
         WHEN ID.unidade IN ( 44 ) --RTRIM(UN.MASCARA) IN ('B 30')
       THEN ID.qtd * 300
         --WHEN RTRIM(UN.MASCARA) IN ('B 40')
         --THEN ID.QTD/1 * 400
         WHEN ID.unidade IN ( 43 ) --RTRIM(UN.MASCARA) IN ('B 50')
       THEN ID.qtd * 500
         WHEN ID.unidade IN ( 40 ) --RTRIM(UN.MASCARA) IN ('B 60')
       THEN ID.qtd * 600
         WHEN ID.unidade IN ( 42 ) --RTRIM(UN.MASCARA) IN ('B 70')
       THEN ID.qtd * 700
         WHEN ID.unidade IN ( 39 ) --RTRIM(UN.MASCARA) IN ('B 80')
       THEN ID.qtd * 800
         WHEN ID.unidade IN ( 6, 38 ) THEN ID.qtd * 1000
       END                QTD_CONVERTIDA,
       ISNULL(ID.valor,ROUND(DC.valortotal,2)) / 1       
                          VLR_ITEM,
       id.unidade         UNID_MEDIDA_ITEM,
              CASE
         WHEN  (
                (SELECT OP.descricao FROM OPERACOES OP WHERE OP.Sequencial = DC.TipoOperacao) LIKE '%TRANSF%' 
				AND DC.TipoOperacao  NOT IN ( 5, 6, 190 ) 
				AND DC.dadoscomplementares NOT LIKE '%ENTRADA PROPRIA%'
			   ) 
			   OR 
			   (
			    DC.TIPOOPERACAO IN (237,253)
                AND DC.PESSOA = 1
                AND DC.dadoscomplementares NOT LIKE '%ENTRADA PROPRIA%'
			   )	 
         	THEN 
         		(CASE
                     WHEN Isnull(DC.keynfe, ' ') <> ' ' 
                 THEN 
                  	 (CASE
--CPF / CNPJ						
                        WHEN (SELECT DP.cnpj FROM DETPESSOAS DP WHERE DP.SEQUENCIAL = DC.EMITENTE) <> (SELECT DPF.cnpj FROM DETPESSOAS DPF WHERE DPF.SEQUENCIAL = DC.IE)
                          AND TD.tipo = 'E' THEN 'CPF DIVERGENTE - SAIDA'
                          
                        WHEN (SELECT DP.cnpj FROM DETPESSOAS DP WHERE DP.SEQUENCIAL = DC.EMITENTE) <> (SELECT DPF.cnpj FROM DETPESSOAS DPF WHERE DPF.SEQUENCIAL = DC.IE)
                         AND TD.tipo = 'S' THEN 'CPF DIVERGENTE - ENTRADA'
--PEDENTE DE ENTRADA/SAIDA
    					WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'S' AND TD.tipo = 'E') AND TD.tipo <> 'S' 
    				  THEN 'PENDENTE ENTRADA'
    				  	WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') AND TD.tipo <> 'E' 
    				  THEN 'SAIDA NAO LOCALIZADA'
  --DATA EMISSAO				
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                         AND (SELECT TOP 1 DCO.emissao FROM   documentos DCO, tiposdoc TDO WHERE  DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.emissao 
					  THEN 'DATA EMISSAO DIVERGENTE'
--IE ENTRADA FILIAL(E) X FORN(S)
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                         AND (SELECT TOP 1 DCO.ie       FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.emitente
					  THEN 'IE DIVERGENTE'
--IE ENTRADA FILIAL(S) X FORN(E)
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                         AND (SELECT TOP 1 DCO.emitente FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.ie 
                      THEN 'IE DIVERGENTE'
--SERIE
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                          AND (SELECT TOP 1 DCO.serie FROM documentos DCO,tiposdoc TDO WHERE  DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.serie 
                      THEN 'SERIE DIVERGENTE'
--VALOR 
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                          AND (SELECT TOP 1 Round(DCO.valortotal, 2) FROM   documentos DCO,tiposdoc TDO WHERE  DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> Round(DC.valortotal, 2) 
                      THEN 'VALOR DIVERGENTE'
                      END )
--NAO FISCAL		
					WHEN Isnull(DC.keynfe, ' ') = ' ' 
				THEN 
						(CASE
                            WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'S' AND TD.tipo = 'E') 
                             AND TD.tipo    <> 'S' 
                         THEN 'PENDENTE ENTRADA'
  							WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE  DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'E' AND TD.tipo = 'S')
  								AND TD.tipo <> 'E' 
  						 THEN 'SAIDA NAO LOCALIZADA'
--VALOR
                            WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE  DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'E' AND TD.tipo = 'S')
  								AND (SELECT TOP 1 Round(DCO.valortotal, 2) FROM documentos DCO,tiposdoc TDO WHERE  DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'E' AND TD.tipo = 'S') <> Round(DC.valortotal, 2) 
  					     THEN 'VALOR DIVERGENTE'
                         END )
              END )
END 						TRANSF_CORRETA,
		CASE
  			WHEN EXISTS (SELECT * FROM   vwdevolucoes DEV WHERE  ID.seqid = DEV.seqid) 
  				THEN 'S'
  			ELSE 'N'	
		END                DEVOLUCAO,
                CASE 
			WHEN EXISTS (SELECT * FROM DocumentosReferenciados DR WHERE DC.Sequencial = DR.SequencialRef)
				THEN 'S'
			ELSE 'N'	
		END                REFERENCIADO,
         Isnull(PSU.loginusuario, PSU.fantasia)  
         					USUARIO_LCTO,
         PSU.cpfcnpj        CPF_USUARIO,
         CASE 
           WHEN TD.TIPO = 'E' THEN DC.KEYNFE 
         END 				CHAVE_NF_SAIDA,
         CASE 
         	WHEN TD.TIPO = 'S' THEN DC.KEYNFE
         END 				CHAVE_NF_ENTRADA,                  
		DC.HISTORICO        HISTORICO,
		SeqCont             ID_CTR_FINANC,
		CASE 
			WHEN ISNULL(DC.TIPONOTA,4) = 1 THEN 'NOTAS FISCAIS AVULSAS'
			WHEN ISNULL(DC.TIPONOTA,4) = 2 THEN 'EMISSÃO DE NOTAS FISCAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 3 THEN 'NOTA FISCAL DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 4 THEN 'MOVIMENTO DE DOCUMENTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 5 THEN 'NOTA FISCAL DE PLUMA'
			WHEN ISNULL(DC.TIPONOTA,4) = 6 THEN 'NOTAS DE RECEBIMENTO DE SEMENTES'
			WHEN ISNULL(DC.TIPONOTA,4) = 7 THEN 'NOTA FISCAL DE SEMENTES'
			WHEN ISNULL(DC.TIPONOTA,4) = 8 THEN 'ENTREGA DE PRODUTOS(FOR)'
			WHEN ISNULL(DC.TIPONOTA,4) = 9 THEN 'ENTREGA DE PRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 10 THEN 'CESSÃO DE CRÉDITO'
			WHEN ISNULL(DC.TIPONOTA,4) = 11 THEN 'DEVOLUÇÃO DE VENDA DE SEMENTE'
			WHEN ISNULL(DC.TIPONOTA,4) = 12 THEN 'DEVOLUÇÃO DE COMPRA'
			WHEN ISNULL(DC.TIPONOTA,4) = 13 THEN 'GERA NOTA DE ENTREGA FUTURA'
			WHEN ISNULL(DC.TIPONOTA,4) = 14 THEN 'DEVOLUÇÃO DE VENDA PARA ENTREGA FUTURA(ARMAZEM)'
			WHEN ISNULL(DC.TIPONOTA,4) = 15 THEN 'DEVOLUÇÃO DE VENDA PARA ENTREGA FUTURA(SEMENTES)'
			WHEN ISNULL(DC.TIPONOTA,4) = 16 THEN 'DEVOLUÇÃO DE VENDA PARA ENTREGA FUTURA(FORRAGEIRA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 17 THEN 'NOTA DE SERVIÇO (MANUTENÇÃO DAS DESPESAS DE DEPÓSITO)'
			WHEN ISNULL(DC.TIPONOTA,4) = 18 THEN 'NOTAS FICAIS DE SERVIÇO (MENU ADM/FIN)'
			WHEN ISNULL(DC.TIPONOTA,4) = 19 THEN 'NOTAS FICAIS DE SERVIÇO (ROMANEIO DE ENTRADA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 20 THEN 'DEVOLUÇÃO DE COMPRA DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 21 THEN 'DEVOLUÇÃO DE PLUMA'
			WHEN ISNULL(DC.TIPONOTA,4) = 22 THEN 'NOTA FISCAL DE SUBPRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 23 THEN 'DEVOLUÇÃO DE VENDA DE SUBPRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 24 THEN 'GERA NOTA FISCAL DE CONSIGNAÇÃO'
			WHEN ISNULL(DC.TIPONOTA,4) = 25 THEN 'CONTRATOS FINANCEIROS'
			WHEN ISNULL(DC.TIPONOTA,4) = 26 THEN 'NOTA FISCAL DE FARDÕES(REMESSA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 27 THEN 'NOTA DE DEVOLUÇÃO DE PLUMA(RETORNO DE REMESSA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 28 THEN 'NOTA DE DEVOLUÇÃO DE PLUMA(RETORNO DE REMESSA F2)'
			WHEN ISNULL(DC.TIPONOTA,4) = 29 THEN 'MONTAGEM DO DOCUMENTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 30 THEN 'IMPORTAÇÃO DE XML'
			WHEN ISNULL(DC.TIPONOTA,4) = 31 THEN 'RECEBIMENTO DE NOTAS FISCAIS (ADMINISTRATIVO)'
			WHEN ISNULL(DC.TIPONOTA,4) = 32 THEN 'NOTA FISCAL DE TRANSFERÊNCIA DE SEMENTE (SAÍDA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 33 THEN 'NOTA FISCAL DE TRANSFERÊNCIA DE SEMENTE (ENTRATA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 34 THEN 'NOTA FISCAL DE COMPLEMENTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 35 THEN 'DEVOLUÇÃO DE OUTROS PRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 36 THEN 'GERA NOTA DE REMESSA(ADM/FIN)'
			WHEN ISNULL(DC.TIPONOTA,4) = 37 THEN 'DOCUMENTO DE IMPOSTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 38 THEN 'NOTA FISCAL DE TRANSFERÊNCIA (ADM/FIN)'
			WHEN ISNULL(DC.TIPONOTA,4) = 39 THEN 'NOTA FISCAL DE EXPORTAÇÃO DE PLUMA'
			WHEN ISNULL(DC.TIPONOTA,4) = 40 THEN 'NOTA FISCAL EXPEDIÇÃO AVULSA'
			WHEN ISNULL(DC.TIPONOTA,4) = 41 THEN 'FIXAÇÃO DE CONTRATOS - À FIXAR (CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 42 THEN 'FIXAÇÃO DE CONTRATOS - ARMAZENAGEM'
			WHEN ISNULL(DC.TIPONOTA,4) = 43 THEN 'DEVOLUÇÃO DE VENDAS DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 44 THEN 'NOTA FISCAL(PRODUTOR) - RECEBIMENTO DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 45 THEN 'NOTA FISCAL(REMESSA PARA O ARMAZÉM) - RECEBIMENTO DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 46 THEN 'RETORNO DE ARMAZÉM GERAL'
			WHEN ISNULL(DC.TIPONOTA,4) = 47 THEN 'RETORNO DE REMESSA PARA FORMAÇÃO DE LOTE'
			WHEN ISNULL(DC.TIPONOTA,4) = 48 THEN 'NOTA DE REMESSA (VIA ROMANEIO DE EMBARQUE)'
			WHEN ISNULL(DC.TIPONOTA,4) = 49 THEN 'REMESSA TERCEIRO (ARMAZÉM GERAL)'
			WHEN ISNULL(DC.TIPONOTA,4) = 50 THEN 'NOTA FISCAL DE RETORNO (VIA SAÍDA DE CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 51 THEN 'GERAÇÃO AUTOMÁTICA DE PAGAMANETOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 52 THEN 'DOC. VENDA P/ CONTA E ORDEM(CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 53 THEN 'REMESSA POR CONTA E ORDEM(CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 54 THEN 'DOC. VENDA P/ CONTA E ORDEM (SEMENTES)'
			WHEN ISNULL(DC.TIPONOTA,4) = 55 THEN 'DOC. VENDA P/ CONTA E ORDEM(SEMENTES)'
			WHEN ISNULL(DC.TIPONOTA,4) = 56 THEN 'DOCUMENTO VIA CONTRATO DE SEGUROS'
			WHEN ISNULL(DC.TIPONOTA,4) = 57 THEN 'DEVOLUÇÃO DE COMPRAS DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 58 THEN 'AGRUPAMENTO DE TÍTULO'
			WHEN ISNULL(DC.TIPONOTA,4) = 59 THEN 'NOTA FISCAL DE RETORNO - BENEFICIAMENTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 60 THEN 'NOTA FISCAL DE RETORNO - ARMAZEM'
			WHEN ISNULL(DC.TIPONOTA,4) = 61 THEN 'NFE IMPORTADA VIA DLL - USA IMPOSTOS JSON'
			WHEN ISNULL(DC.TIPONOTA,4) = 62 THEN 'NFE IMPORTADA VIA DLL - USA IMPOSTOS AGM'
			WHEN ISNULL(DC.TIPONOTA,4) = 63 THEN 'ENTRADA DE PRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 64 THEN 'NOTA FISCAL DE COMPRA DE SEMENTE MATRIZ'
		END                 ORIGEM
FROM   documentos DC
       INNER JOIN tiposdoc TD
               ON TD.codigo = DC.tipodoc
       LEFT JOIN itensdocumento ID
               ON ID.sequencial = DC.sequencial
       LEFT JOIN PESSOAS PSU
       		   ON PSU.CODIGO = DC.usuoriginal 
  
WHERE  1 = 1
       
	   AND TD.codigo NOT IN ( 71, 213, 85, 233,
                              204,205, --PROTOCOLO
                              234, 203, --FOLHA
                              62, --ARRENDAMENTO
                              226, --CTR COMPRA
                              15, 14, 23, 64,
                              67, --FINANCIAMENTO
                              123, 124--AJUSTES ESTOQUE
                             )     
       AND Isnull(DC.situacaoef, 'N') <> 'S'
       AND Datediff(year, Isnull(DC.datarecebimento, DC.datadig), Sysdatetime()) <= 1
       
)  

SELECT ID_DOCUMENTO,	ID_ITEM_DOC,	ID_EMPRESA,	ID_FILIAL,	ID_SAFRA,	NR_DOCUMENTO,	SERIE,	ID_PESSOA,	ID_DET_PESSOA,	ID_TIPO_DOC,	TIPO_MOV,	TIPO_DOC,	ID_OPERACAO,	DT_LCTO,	DT_EMISSAO,	DT_ENTRADA,	DT_LIVRO_FISCAL,	DT_ACEITE,	UA,	VLR_TOTAL,	QTD_TOTAL,	COD_MATERIAL,	QTD_ITEM,	QTD_CONVERTIDA,	VLR_ITEM,	UNID_MEDIDA_ITEM,	TRANSF_CORRETA,	DEVOLUCAO,	REFERENCIADO,	USUARIO_LCTO,	CPF_USUARIO,	CHAVE_NF_SAIDA,	CHAVE_NF_ENTRADA,	HISTORICO,	ID_CTR_FINANC,	ORIGEM FROM RESUMO_DOC RD

WHERE 1=1
      AND RD.TRANSF_CORRETA IS NOT NULL
      AND ISNULL(RD.REFERENCIADO,'N') <> 'S'"""
        cursor.execute(consulta_sql)  
        registros = cursor.fetchall()
        conn.close()
        # Converter os objetos Row para tuplas
        registros = [tuple(registro) for registro in registros]
        return registros
    else:
        return None
      
def obter_registros_sql_server_2():
    conn = conectar_sql_server()
    if conn:
        cursor = conn.cursor()
        consulta_sql = """WITH RESUMO_DOC AS (
SELECT DC.sequencial      ID_DOCUMENTO,
	   ID.SEQID           ID_ITEM_DOC,
       DC.empresa         ID_EMPRESA,
       DC.emitente        ID_FILIAL,
       DC.safra           ID_SAFRA,
       --(SELECT CP.safra FROM compras CP WHERE  CP.sequencialoc = ID.sequencialoc) ID_SAFRA_OC,
       --TD.tipo 		   TIPO_MOV,
       DC.numdoc          NR_DOCUMENTO,
       DC.serie           SERIE,
       DC.pessoa          ID_PESSOA,
       dc.IE              ID_DET_PESSOA,
       DC.tipodoc         ID_TIPO_DOC,--- E = SAIDA / S = ENTRADA
       CASE WHEN td.Tipo = 'E' THEN 'SAIDA' ELSE 'ENTRADA' END 
       					  TIPO_MOV,
       TD.Descricao       TIPO_DOC,
       DC.tipooperacao    ID_OPERACAO,
       DC.datadig         DT_LCTO,
       DC.emissao         DT_EMISSAO,
       DC.datarecebimento DT_ENTRADA,
       DC.datalivro       DT_LIVRO_FISCAL,
       ID.dtrecebe        DT_ACEITE,
       DC.UnidArmaz       UA,
       ROUND(DC.valortotal,2) / 1  
                              VLR_TOTAL,
       ISNULL(DC.quantidade,1) / 1  QTD_TOTAL,
       ID.produto         COD_MATERIAL,
       ISNULL(ID.qtd,0) / 1
                          QTD_ITEM,
       CASE
         WHEN ID.unidade IN ( 1, 2, 12, 7, 22, 23, 30, 31, 32, 51 ) THEN ID.qtd * 1
         WHEN ID.unidade IN ( 54, 55 ) THEN ID.qtd * 2.5
         WHEN ID.unidade IN ( 34, 48, 49 ) THEN ID.qtd * 5
         WHEN ID.unidade IN ( 35 ) THEN ID.qtd * 10
         WHEN ID.unidade IN ( 27 ) --RTRIM(UN.MASCARA) IN ('B 80')
       THEN ID.qtd * 15
         WHEN ID.unidade IN ( 33, 36 ) THEN ID.qtd * 20
         WHEN ID.unidade IN ( 26 ) THEN ID.qtd * 40
         WHEN ID.unidade IN ( 25 ) THEN ID.qtd * 60
         WHEN ID.unidade IN ( 41, 52, 53, 57 ) --RTRIM(UN.MASCARA) IN ('B 20')
       THEN ID.qtd * 200
         WHEN ID.unidade IN ( 44 ) --RTRIM(UN.MASCARA) IN ('B 30')
       THEN ID.qtd * 300
         --WHEN RTRIM(UN.MASCARA) IN ('B 40')
         --THEN ID.QTD/1 * 400
         WHEN ID.unidade IN ( 43 ) --RTRIM(UN.MASCARA) IN ('B 50')
       THEN ID.qtd * 500
         WHEN ID.unidade IN ( 40 ) --RTRIM(UN.MASCARA) IN ('B 60')
       THEN ID.qtd * 600
         WHEN ID.unidade IN ( 42 ) --RTRIM(UN.MASCARA) IN ('B 70')
       THEN ID.qtd * 700
         WHEN ID.unidade IN ( 39 ) --RTRIM(UN.MASCARA) IN ('B 80')
       THEN ID.qtd * 800
         WHEN ID.unidade IN ( 6, 38 ) THEN ID.qtd * 1000
       END                QTD_CONVERTIDA,
       ISNULL(ID.valor,ROUND(DC.valortotal,2)) / 1       
                          VLR_ITEM,
       id.unidade         UNID_MEDIDA_ITEM,
              CASE
         WHEN  (
                (SELECT OP.descricao FROM OPERACOES OP WHERE OP.Sequencial = DC.TipoOperacao) LIKE '%TRANSF%' 
				AND DC.TipoOperacao  NOT IN ( 5, 6, 190 ) 
				AND DC.dadoscomplementares NOT LIKE '%ENTRADA PROPRIA%'
			   ) 
			   OR 
			   (
			    DC.TIPOOPERACAO IN (237,253)
                AND DC.PESSOA = 1
                AND DC.dadoscomplementares NOT LIKE '%ENTRADA PROPRIA%'
			   )	 
         	THEN 
         		(CASE
                     WHEN Isnull(DC.keynfe, ' ') <> ' ' 
                 THEN 
                  	 (CASE
--CPF / CNPJ						
                        WHEN (SELECT DP.cnpj FROM DETPESSOAS DP WHERE DP.SEQUENCIAL = DC.EMITENTE) <> (SELECT DPF.cnpj FROM DETPESSOAS DPF WHERE DPF.SEQUENCIAL = DC.IE)
                          AND TD.tipo = 'E' THEN 'CPF DIVERGENTE - SAIDA'
                          
                        WHEN (SELECT DP.cnpj FROM DETPESSOAS DP WHERE DP.SEQUENCIAL = DC.EMITENTE) <> (SELECT DPF.cnpj FROM DETPESSOAS DPF WHERE DPF.SEQUENCIAL = DC.IE)
                         AND TD.tipo = 'S' THEN 'CPF DIVERGENTE - ENTRADA'
--PEDENTE DE ENTRADA/SAIDA
    					WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'S' AND TD.tipo = 'E') AND TD.tipo <> 'S' 
    				  THEN 'PENDENTE ENTRADA'
    				  	WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') AND TD.tipo <> 'E' 
    				  THEN 'SAIDA NAO LOCALIZADA'
  --DATA EMISSAO				
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                         AND (SELECT TOP 1 DCO.emissao FROM   documentos DCO, tiposdoc TDO WHERE  DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.emissao 
					  THEN 'DATA EMISSAO DIVERGENTE'
--IE ENTRADA FILIAL(E) X FORN(S)
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                         AND (SELECT TOP 1 DCO.ie       FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.emitente
					  THEN 'IE DIVERGENTE'
--IE ENTRADA FILIAL(S) X FORN(E)
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                         AND (SELECT TOP 1 DCO.emitente FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.ie 
                      THEN 'IE DIVERGENTE'
--SERIE
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                          AND (SELECT TOP 1 DCO.serie FROM documentos DCO,tiposdoc TDO WHERE  DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> DC.serie 
                      THEN 'SERIE DIVERGENTE'
--VALOR 
                        WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') 
                          AND (SELECT TOP 1 Round(DCO.valortotal, 2) FROM   documentos DCO,tiposdoc TDO WHERE  DCO.keynfe = DC.keynfe AND DCO.tipodoc = TDO.codigo AND TDO.tipo = 'E' AND TD.tipo = 'S') <> Round(DC.valortotal, 2) 
                      THEN 'VALOR DIVERGENTE'
                      END )
--NAO FISCAL		
					WHEN Isnull(DC.keynfe, ' ') = ' ' 
				THEN 
						(CASE
                            WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'S' AND TD.tipo = 'E') 
                             AND TD.tipo    <> 'S' 
                         THEN 'PENDENTE ENTRADA'
  							WHEN NOT EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE  DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'E' AND TD.tipo = 'S')
  								AND TD.tipo <> 'E' 
  						 THEN 'SAIDA NAO LOCALIZADA'
--VALOR
                            WHEN EXISTS (SELECT * FROM documentos DCO,tiposdoc TDO WHERE  DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'E' AND TD.tipo = 'S')
  								AND (SELECT TOP 1 Round(DCO.valortotal, 2) FROM documentos DCO,tiposdoc TDO WHERE  DCO.numero = DC.numero AND DCO.emissao = DC.emissao AND DC.emitente = DCO.ie AND DC.ie = DCO.emitente AND TDO.codigo = DCO.tipodoc AND TDO.tipo = 'E' AND TD.tipo = 'S') <> Round(DC.valortotal, 2) 
  					     THEN 'VALOR DIVERGENTE'
                         END )
              END )
END 						TRANSF_CORRETA,
		CASE
  			WHEN EXISTS (SELECT * FROM   vwdevolucoes DEV WHERE  ID.seqid = DEV.seqid) 
  				THEN 'S'
  			ELSE 'N'	
		END                DEVOLUCAO,
                CASE 
			WHEN EXISTS (SELECT * FROM DocumentosReferenciados DR WHERE DC.Sequencial = DR.SequencialRef)
				THEN 'S'
			ELSE 'N'	
		END                REFERENCIADO,
         Isnull(PSU.loginusuario, PSU.fantasia)  
         					USUARIO_LCTO,
         PSU.cpfcnpj        CPF_USUARIO,
         CASE 
           WHEN TD.TIPO = 'E' THEN DC.KEYNFE 
         END 				CHAVE_NF_SAIDA,
         CASE 
         	WHEN TD.TIPO = 'S' THEN DC.KEYNFE
         END 				CHAVE_NF_ENTRADA,                  
		DC.HISTORICO        HISTORICO,
		SeqCont             ID_CTR_FINANC,
		CASE 
			WHEN ISNULL(DC.TIPONOTA,4) = 1 THEN 'NOTAS FISCAIS AVULSAS'
			WHEN ISNULL(DC.TIPONOTA,4) = 2 THEN 'EMISSÃO DE NOTAS FISCAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 3 THEN 'NOTA FISCAL DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 4 THEN 'MOVIMENTO DE DOCUMENTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 5 THEN 'NOTA FISCAL DE PLUMA'
			WHEN ISNULL(DC.TIPONOTA,4) = 6 THEN 'NOTAS DE RECEBIMENTO DE SEMENTES'
			WHEN ISNULL(DC.TIPONOTA,4) = 7 THEN 'NOTA FISCAL DE SEMENTES'
			WHEN ISNULL(DC.TIPONOTA,4) = 8 THEN 'ENTREGA DE PRODUTOS(FOR)'
			WHEN ISNULL(DC.TIPONOTA,4) = 9 THEN 'ENTREGA DE PRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 10 THEN 'CESSÃO DE CRÉDITO'
			WHEN ISNULL(DC.TIPONOTA,4) = 11 THEN 'DEVOLUÇÃO DE VENDA DE SEMENTE'
			WHEN ISNULL(DC.TIPONOTA,4) = 12 THEN 'DEVOLUÇÃO DE COMPRA'
			WHEN ISNULL(DC.TIPONOTA,4) = 13 THEN 'GERA NOTA DE ENTREGA FUTURA'
			WHEN ISNULL(DC.TIPONOTA,4) = 14 THEN 'DEVOLUÇÃO DE VENDA PARA ENTREGA FUTURA(ARMAZEM)'
			WHEN ISNULL(DC.TIPONOTA,4) = 15 THEN 'DEVOLUÇÃO DE VENDA PARA ENTREGA FUTURA(SEMENTES)'
			WHEN ISNULL(DC.TIPONOTA,4) = 16 THEN 'DEVOLUÇÃO DE VENDA PARA ENTREGA FUTURA(FORRAGEIRA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 17 THEN 'NOTA DE SERVIÇO (MANUTENÇÃO DAS DESPESAS DE DEPÓSITO)'
			WHEN ISNULL(DC.TIPONOTA,4) = 18 THEN 'NOTAS FICAIS DE SERVIÇO (MENU ADM/FIN)'
			WHEN ISNULL(DC.TIPONOTA,4) = 19 THEN 'NOTAS FICAIS DE SERVIÇO (ROMANEIO DE ENTRADA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 20 THEN 'DEVOLUÇÃO DE COMPRA DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 21 THEN 'DEVOLUÇÃO DE PLUMA'
			WHEN ISNULL(DC.TIPONOTA,4) = 22 THEN 'NOTA FISCAL DE SUBPRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 23 THEN 'DEVOLUÇÃO DE VENDA DE SUBPRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 24 THEN 'GERA NOTA FISCAL DE CONSIGNAÇÃO'
			WHEN ISNULL(DC.TIPONOTA,4) = 25 THEN 'CONTRATOS FINANCEIROS'
			WHEN ISNULL(DC.TIPONOTA,4) = 26 THEN 'NOTA FISCAL DE FARDÕES(REMESSA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 27 THEN 'NOTA DE DEVOLUÇÃO DE PLUMA(RETORNO DE REMESSA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 28 THEN 'NOTA DE DEVOLUÇÃO DE PLUMA(RETORNO DE REMESSA F2)'
			WHEN ISNULL(DC.TIPONOTA,4) = 29 THEN 'MONTAGEM DO DOCUMENTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 30 THEN 'IMPORTAÇÃO DE XML'
			WHEN ISNULL(DC.TIPONOTA,4) = 31 THEN 'RECEBIMENTO DE NOTAS FISCAIS (ADMINISTRATIVO)'
			WHEN ISNULL(DC.TIPONOTA,4) = 32 THEN 'NOTA FISCAL DE TRANSFERÊNCIA DE SEMENTE (SAÍDA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 33 THEN 'NOTA FISCAL DE TRANSFERÊNCIA DE SEMENTE (ENTRATA)'
			WHEN ISNULL(DC.TIPONOTA,4) = 34 THEN 'NOTA FISCAL DE COMPLEMENTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 35 THEN 'DEVOLUÇÃO DE OUTROS PRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 36 THEN 'GERA NOTA DE REMESSA(ADM/FIN)'
			WHEN ISNULL(DC.TIPONOTA,4) = 37 THEN 'DOCUMENTO DE IMPOSTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 38 THEN 'NOTA FISCAL DE TRANSFERÊNCIA (ADM/FIN)'
			WHEN ISNULL(DC.TIPONOTA,4) = 39 THEN 'NOTA FISCAL DE EXPORTAÇÃO DE PLUMA'
			WHEN ISNULL(DC.TIPONOTA,4) = 40 THEN 'NOTA FISCAL EXPEDIÇÃO AVULSA'
			WHEN ISNULL(DC.TIPONOTA,4) = 41 THEN 'FIXAÇÃO DE CONTRATOS - À FIXAR (CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 42 THEN 'FIXAÇÃO DE CONTRATOS - ARMAZENAGEM'
			WHEN ISNULL(DC.TIPONOTA,4) = 43 THEN 'DEVOLUÇÃO DE VENDAS DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 44 THEN 'NOTA FISCAL(PRODUTOR) - RECEBIMENTO DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 45 THEN 'NOTA FISCAL(REMESSA PARA O ARMAZÉM) - RECEBIMENTO DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 46 THEN 'RETORNO DE ARMAZÉM GERAL'
			WHEN ISNULL(DC.TIPONOTA,4) = 47 THEN 'RETORNO DE REMESSA PARA FORMAÇÃO DE LOTE'
			WHEN ISNULL(DC.TIPONOTA,4) = 48 THEN 'NOTA DE REMESSA (VIA ROMANEIO DE EMBARQUE)'
			WHEN ISNULL(DC.TIPONOTA,4) = 49 THEN 'REMESSA TERCEIRO (ARMAZÉM GERAL)'
			WHEN ISNULL(DC.TIPONOTA,4) = 50 THEN 'NOTA FISCAL DE RETORNO (VIA SAÍDA DE CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 51 THEN 'GERAÇÃO AUTOMÁTICA DE PAGAMANETOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 52 THEN 'DOC. VENDA P/ CONTA E ORDEM(CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 53 THEN 'REMESSA POR CONTA E ORDEM(CEREAIS)'
			WHEN ISNULL(DC.TIPONOTA,4) = 54 THEN 'DOC. VENDA P/ CONTA E ORDEM (SEMENTES)'
			WHEN ISNULL(DC.TIPONOTA,4) = 55 THEN 'DOC. VENDA P/ CONTA E ORDEM(SEMENTES)'
			WHEN ISNULL(DC.TIPONOTA,4) = 56 THEN 'DOCUMENTO VIA CONTRATO DE SEGUROS'
			WHEN ISNULL(DC.TIPONOTA,4) = 57 THEN 'DEVOLUÇÃO DE COMPRAS DE CEREAIS'
			WHEN ISNULL(DC.TIPONOTA,4) = 58 THEN 'AGRUPAMENTO DE TÍTULO'
			WHEN ISNULL(DC.TIPONOTA,4) = 59 THEN 'NOTA FISCAL DE RETORNO - BENEFICIAMENTO'
			WHEN ISNULL(DC.TIPONOTA,4) = 60 THEN 'NOTA FISCAL DE RETORNO - ARMAZEM'
			WHEN ISNULL(DC.TIPONOTA,4) = 61 THEN 'NFE IMPORTADA VIA DLL - USA IMPOSTOS JSON'
			WHEN ISNULL(DC.TIPONOTA,4) = 62 THEN 'NFE IMPORTADA VIA DLL - USA IMPOSTOS AGM'
			WHEN ISNULL(DC.TIPONOTA,4) = 63 THEN 'ENTRADA DE PRODUTOS'
			WHEN ISNULL(DC.TIPONOTA,4) = 64 THEN 'NOTA FISCAL DE COMPRA DE SEMENTE MATRIZ'
		END                 ORIGEM
FROM   documentos DC
       INNER JOIN tiposdoc TD
               ON TD.codigo = DC.tipodoc
       LEFT JOIN itensdocumento ID
               ON ID.sequencial = DC.sequencial
       LEFT JOIN PESSOAS PSU
       		   ON PSU.CODIGO = DC.usuoriginal 
  
WHERE  1 = 1
       
	   AND TD.codigo NOT IN ( 71, 213, 85, 233,
                              204,205, --PROTOCOLO
                              234, 203, --FOLHA
                              62, --ARRENDAMENTO
                              226, --CTR COMPRA
                              15, 14, 23, 64,
                              67, --FINANCIAMENTO
                              123, 124--AJUSTES ESTOQUE
                             )     
       AND Isnull(DC.situacaoef, 'N') <> 'S'
       AND Datediff(year, Isnull(DC.datarecebimento, DC.datadig), Sysdatetime()) <= 1
       
)  

SELECT ID_DOCUMENTO,	ID_ITEM_DOC FROM RESUMO_DOC RD

WHERE 1=1
      AND RD.TRANSF_CORRETA IS NOT NULL
      AND ISNULL(RD.REFERENCIADO,'N') <> 'S'"""
        cursor.execute(consulta_sql)  
        registros = cursor.fetchall()
        conn.close()
        # Converter os objetos Row para tuplas
        registros = [tuple(registro) for registro in registros]
        return registros
    else:
        return None

# Função para obter registros do MySQL
def obter_registros_mysql(conn):
    if conn:
            cursor = conn.cursor()
            consulta_sql = "SELECT ID_DOCUMENTO,	ID_ITEM_DOC  FROM doc_transf"
            cursor.execute(consulta_sql)
            registros = cursor.fetchall()
            cursor.close()
            # Convertendo os objetos Row para tuplas
            registros = [tuple(registro) for registro in registros]
            return registros
    else:
        print("Erro: Conexão com o banco de dados SQL Server não foi estabelecida.")
        return None

# Função para verificar se um registro existe no MySQL
def verificar_registro_mysql(conn, registro):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM doc_transf WHERE ID_DOCUMENTO = %s AND ID_ITEM_DOC = %s AND doc_transf.CORRIGIDO = "N" ', (registro[0], registro[1]))  # Substitua 'doc_transf' pelo nome da sua tabela no MySQL
    resultado = cursor.fetchone()
    cursor.close()
    return resultado is not None

# Função para adicionar um registro ao MySQL
def adicionar_registro_mysql(conn, registro):
    cursor = conn.cursor()
    try:
        # Insira o registro na tabela do MySQL
        cursor.execute('INSERT INTO doc_transf (ID_DOCUMENTO,	ID_ITEM_DOC,	ID_EMPRESA,	ID_FILIAL,	ID_SAFRA,	NR_DOCUMENTO,	SERIE,	ID_PESSOA,	ID_DET_PESSOA,	ID_TIPO_DOC,	TIPO_MOV,	TIPO_DOC,	ID_OPERACAO,	DT_LCTO,	DT_EMISSAO,	DT_ENTRADA,	DT_LIVRO_FISCAL,	DT_ACEITE,	UA,	VLR_TOTAL,	QTD_TOTAL,	COD_MATERIAL,	QTD_ITEM,	QTD_CONVERTIDA,	VLR_ITEM,	UNID_MEDIDA_ITEM,	TRANSF_CORRETA,	DEVOLUCAO,	REFERENCIADO,	USUARIO_LCTO,	CPF_USUARIO,	CHAVE_NF_SAIDA,	CHAVE_NF_ENTRADA,	HISTORICO,	ID_CTR_FINANC,	ORIGEM, DATA_INSERT, CORRIGIDO) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),"N")', registro) 
        conn.commit()
        print(f"Registro inserido na tabela do MySQL: {registro}")
    except mysql.connector.Error as err:
        print(f"Erro ao adicionar registro na tabela do MySQL: {err}")
        conn.rollback()
    finally:  
        cursor.close()

# Função para atualizar um registro ao MySQL
def atualizar_registro_mysql(conn, registro):
    cursor = conn.cursor()
    try:
        cursor.execute('Update doc_transf set CORRIGIDO = "S", DATA_CORRECAO = NOW() where ID_DOCUMENTO = %s AND ID_ITEM_DOC = %s' , (registro[0], registro[1]) )  # Substitua ... pelos nomes das colunas
        conn.commit()
        print("Registro adicionado ao MySQL:", registro)
    except mysql.connector.Error as err:
        print(f"Erro ao adicionar registro ao MySQL: {err}")
        conn.rollback()
    finally:
        cursor.close()
        
# Conectar ao MySQL
conn_mysql = conectar_mysql()

# Obter todos os registros da tabela no SQL Server
registros_sql_server = obter_registros_sql_server()

print(registros_sql_server)


# Verificar e adicionar os registros ausentes no MySQL
if conn_mysql and registros_sql_server:
    for registro in registros_sql_server:
        if not verificar_registro_mysql(conn_mysql, registro):
            adicionar_registro_mysql(conn_mysql, registro)
            
# Verificar e adicionar os registros atualizados no MySQL
if conn_mysql and registros_sql_server:
    for registro in registros_sql_server:
        if not verificar_registro_mysql(conn_mysql, registro):
            adicionar_registro_mysql(conn_mysql, registro)            

# Fechar conexão com o MySQL
if conn_mysql:
    conn_mysql.close()
      