// Imports the Google Cloud client library.
const fsPromises = require('fs').promises;
const fs = require('fs');
const path = require('path')
const {BigQuery} = require('@google-cloud/bigquery');
const projectId = 'raiffeisen-owox';
const keyFilename = './raiffeisen-owox-5f0b97071b4d.json';




function main(datasetId) {

  const {BigQuery} = require('@google-cloud/bigquery');
  const bigquery = new BigQuery({projectId, keyFilename});

  const projectFolder = 'C:\\BQ\\' + datasetId + '\\'

  if (!fs.existsSync(projectFolder)){
    fs.mkdirSync(projectFolder)
  }

  async function downloadViews(datasetId) {

    const dataset = bigquery.dataset(datasetId);
    const [tables] = await dataset.getTables();

    console.log('VIEWS downloaded:');

    for  (let table of tables) { // (4)

      if (table.metadata.type == 'VIEW') {
      let tableId = table.id;
      
      let [view] =  await table.get()
      let filePath = 'C:\\BQ\\'+datasetId+'\\' + table.id +'.txt'
      filehandle = await fsPromises.open(filePath, 'w+');
      await filehandle.writeFile(view.metadata.view.query, 'utf8')
      await filehandle.close();
      console.log(`For view : ${table.id} created file ${filePath}`);
      }

    }
  
  }
 
  downloadViews(datasetId);
} 

main(...process.argv.slice(2));