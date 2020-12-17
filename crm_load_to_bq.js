const fs = require('fs');
const csv = require('csv');
const utils = require('utility');
const {BigQuery} = require('@google-cloud/bigquery');
const projectId = 'raiffeisen-owox';
const keyFilename = './raiffeisen-owox-5f0b97071b4d.json';
const orders_schema_base = require("./crm_orders_schema_base")
const orders_schema_other = require("./crm_orders_schema_other")



const del = ms => {
  setTimeout(() => console.log(ms), ms)
}

/* Файл выгрузки crm - указать корректный путь к Выгрузки\crm_orders_2019_other.csv который зависит от ОС*/
const readStream = "\\\\raiffeisen.ru\\dfs\\RBA\\MSK\\Nagatinskoe\\Workgroups\\Digital Sales\\OWOX\\Выгрузки\\crm_orders_2019_other.csv"

/* Путь к зашифрованному файлу , рекомендуется где нибдь на локльном диске*/
//let writeStream = fs.createWriteStream("\\\\raiffeisen.ru\\dfs\\RBA\\MSK\\Nagatinskoe\\Workgroups\\Digital Sales\\OWOX\\Выгрузки\\to BQ\\crm_orders_2019_other.csv");
const writeStream = "C:\\DATA\\CRM\\crm_orders_2019_other.csv"

const BQ = new BigQuery({projectId, keyFilename});
const loadList = [
  {
  metaData:setMetadata(orders_schema_base),
  file:"C:\\DATA\\CRM\\crm_orders_2019_base.csv",
  bqTable:BQ.dataset("Orders_CRM").table("crm_orders_2019_base_d")
  }
  , 
  {
  metaData:setMetadata(),
  file:"C:\\DATA\\CRM\\crm_orders_2019_other.csv",
  bqTable:BQ.dataset("Orders_CRM").table("crm_orders_2019_other")
  }
]




async function main() {

  await Promise.all([
    fsCopy(
      fs.createReadStream('\\\\raiffeisen.ru\\dfs\\RBA\\MSK\\Nagatinskoe\\Workgroups\\Digital Sales\\OWOX\\Выгрузки\\crm_orders_2019_base.csv'),
      fs.createWriteStream('C:\\DATA\\CRM\\crm_orders_2019_base.csv')
      ),
    owoxCrypto(readStream, writeStream)
  ])

  let promList =[]
  loadList.forEach(item => {
    promList.push(loadCsvToBq(item.file, item.bqTable, item.metaData))
  })
  await Promise.all(promList)
  
  console.log('mergeOrders - запущен')
  
  let [mergeOrders] = await BQ.createQueryJob( {   
    priority: "INTERACTIVE",
    query: "MERGE Orders_CRM.crm_orders_2019_base T USING Orders_CRM.crm_orders_2019_base_d S ON T.leadid = S.leadid WHEN NOT MATCHED THEN INSERT ROW",
    useLegacySql: false,
    })

  await mergeOrders.getQueryResults()

  console.log('mergeOrders - обновлен')

  BQ.createQueryJob( {   
    priority: "INTERACTIVE",
    query: "SELECT * FROM \`raiffeisen-owox.Orders_CRM.VW_CRM_orders_19\`",
    useLegacySql: false,
    allowLargeResult: true,
    destinationTable: {
      projectId: projectId,
      datasetId: 'Orders_CRM',
      tableId: 'CRM_orders_19',
    },
    writeDisposition: 'WRITE_TRUNCATE'
  }).then(job => console.log("Старт рефреш orders"))

  console.log("Старт рефреш mergeCRM")

  let [mergeCRM] = await BQ.createQueryJob( {   
    priority: "INTERACTIVE",
    query: "MERGE raiffeisen-owox.CRM.orders  T USING (  SELECT *     FROM `raiffeisen-owox.Orders_CRM.crm_orders_2019_base_d`    LEFT JOIN (SELECT STAT FROM `raiffeisen-owox.Orders_CRM.crm_orders_2019_other` STAT) ON leadid = STAT.LEADID  ) S ON T.ORDER_ID = S.leadid AND T._PARTITIONTIME >= '2019-01-01' WHEN NOT MATCHED  THEN  INSERT  (_PARTITIONTIME,ORDER_ID, APP_ID, LEAD_DATE, PRODUCT_LINE, PRODUCT, NTB, GOOGLE_ID, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CONTENT, UTM_TERM, SEGMENT_REQUEST_LIMIT, SEGMENT_ISSUES_LIMIT, STATUSES, SALE_CH_CODE, PROCESSING_CHANNEL)  VALUES(TIMESTAMP_TRUNC(LeadDate, DAY), leadid , leadid, LeadDate, PRODUCT_LINE , PRODUCT_LINE , NTB , GOOGLE_ID, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CONTENT, utm_term, segmentRequestLimit, STAT.SEGMENTISSUESLIMIT,[STRUCT(DATE( LeadDate ), STAT.STATUS, STAT.APPROVAL)], STAT.SALE_CH_CODE, STAT.PROCESSING_CHANNEL)    " ,
    useLegacySql: false,
    })
  await mergeCRM.getQueryResults()

  console.log('готово - mergeCRM')

  BQ.createQueryJob( {   
    priority: "INTERACTIVE",
    query: "MERGE CRM.orders T  USING  CRM.MD_NewStatuses S ON T.ORDER_ID = S.LEADID AND T._PARTITIONTIME >= '2019-01-01' WHEN MATCHED THEN  UPDATE SET STATUSES = ARRAY_CONCAT( STATUSES ,[STRUCT(CURRENT_DATE(), S.STATUS, S.APPROVAL )])",
    useLegacySql: false,
    }).then(job => console.log("Старт обновления статусов mergeCRM"))

}

function fsCopy(fromStream,toStream){
  return new Promise((resolve, reject) => {
    console.log('start copy to : ' + toStream.path);
  fromStream.pipe(toStream)
  toStream.on('close', (res) => resolve(console.log('ready copy to : ' + toStream.path)))
  toStream.on('error',(err) => reject(err))

  })

}

function setMetadata(fSchema) {

  if(fSchema == undefined) {

    return {
      writeDisposition: "WRITE_TRUNCATE",
      fieldDelimiter: ";",
      skipLeadingRows: 1,
      maxBadRecords: 100,
      allowQuotedNewlines: false,
      sourceFormat: "CSV",
      allowJaggedRows: false,
      ignoreUnknownValues: true,
      autodetect: true,
    };
  } else {
return {
    schema: fSchema,
    writeDisposition: "WRITE_TRUNCATE",
    fieldDelimiter: ";",
    skipLeadingRows: 1,
    maxBadRecords: 100,
    allowQuotedNewlines: false,
    sourceFormat: "CSV",
    allowJaggedRows: false,
    ignoreUnknownValues: true,
    autodetect: false,
  }
}
}



function owoxCrypto(readStreamPath = '', writeStreamPath = '') {

    let opt = {
        skip_lines_with_error:true,
        delimiter:';',
        columns:true,
    };


    let parse = csv.parse(opt);

    let transform = csv.transform( row => {

        row.PHONE_MD5 = utils.md5(row.SITE_PHONE);

        if(row.CU_EMAIL == false) {
            row.EMAIL_MD5 = '';
        } else {
            row.EMAIL_MD5 = utils.md5(row.CU_EMAIL);
        } 

        row.PHONE_SHA256 = utils.sha256(row.SITE_PHONE);

        if(row.CU_EMAIL == false) {
            row.EMAIL_SHA256 = '';
        } else {
            row.EMAIL_SHA256 = utils.sha256(row.CU_EMAIL);
        } 

        delete(row.SITE_PHONE);
        delete(row.CU_EMAIL);   

        return row;
    });

    let toString = csv.stringify({
        header:true,
        delimiter:';',
    })

    return new Promise((resolve, reject) => {
      try {
        let writeStream = fs.createWriteStream(writeStreamPath)

        writeStream.on('ready',() => {

          console.log('start crypto to : ' + writeStream.path)
        
        fs.createReadStream(readStreamPath).on('ready', () => {return this})
          .pipe(parse)
          .pipe(transform)
          .pipe(toString)
          .pipe(writeStream);
          writeStream.on('close', () => {
            console.log('ready crypto to : ' + writeStream.path)
            resolve(writeStream)
          })})
      } catch (error) {
        reject(error)
      } 
    })
    
  
}

async function loadCsvToBq(file_path, table, metadata) {

  console.log("Загруpка: " + file_path)

  return new Promise((resolve, reject) => {

      fs.createReadStream(file_path)
      .pipe(table.createWriteStream(metadata))
      .on('complete', (job) => { 
        resolve(console.log(job.metadata.statistics)) })
      .on('error', err => {
        console.log("Загрузка не удачна ");
      })

  })

}

main() // Запуск основного потока
