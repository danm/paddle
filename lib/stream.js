const zlib = require('zlib');
const request = require('request');
const fs = require('fs');
const byline = require('byline');
const AWS = require('aws-sdk');
const s3Stream = require('s3-upload-stream')(new AWS.S3({ apiVersion: '2006-03-01' }));

const download = () => {
  let lines = 0;
  let total = 0;
  let file = 0;
  let header = '';
  const gunzip = zlib.createGunzip();
  console.log('downloading file');
  const buf = request('https://s3-eu-west-1.amazonaws.com/puddle-csv.tools.bbc.co.uk/2017-06-11-15-12-news-v2-24632963.csv.gz');
  const x = byline(buf.pipe(gunzip));
  const writes = [];
  console.log(`Created split file ${file}`);
  writes.push(fs.createWriteStream(`./str/file-${file}.log`));

  x.on('data', (line) => {
    if (lines === 0) {
      header = line;
    }
    lines++;
    total++;
    if (lines > 250000) {
      file++;
      lines = 0;
      console.log(`Created split file ${file}`);
      writes.push(fs.createWriteStream(`./str/file-${file}.log`));
      writes[file].write(header + '\n');
    }
    writes[file].write(line + '\n');
  });

  x.on('end', () => {
    console.log('Finsihed downloading and splitting files');
    setUploads();
  });
};

const uploader = (x, i) => (
  new Promise((resolve, reject) => {
    const reader = fs.createReadStream(`str/${x[i]}`);
    const zip = zlib.createGzip();
    const upload = s3Stream.upload({
      Bucket: 'audience-engagement-assets.tools.bbc.co.uk',
      Key: `testing/${x[i]}.gz`
    });
    reader.pipe(zip).pipe(upload);
    upload.on('error', (e) => {
      reject(e);
    });

    upload.on('uploaded', () => {
      resolve();
    });
  })
);

const setUploads = async () => {
  const x = fs.readdirSync('str');
  for (let i in x) {
    try {
      console.log `starting row ${i}`
      await uploader(x, i);
      console.log `finished row ${i}`
    } catch (e) {
      console.log(e);
    }
  }
  console.log('Finished uploading file.');
};

download();