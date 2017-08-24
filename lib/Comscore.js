process.env.TZ = 'Europe/London';
const request = require('request');
const fs = require('fs');
const zlib = require('zlib');
const byline = require('byline');
const AWS = require('aws-sdk');
const dax = require('./dax-utils');
const Logback = require('logback');
let s3Stream;
let s3;

// bad practice - needs to be re written 
Date.prototype.fileDate = function() {
    return this.getFullYear() + '-' + ('0' + (this.getMonth() + 1)).slice(-2) + '-' + ('0' + this.getDate()).slice(-2) + '-' + ('0' + this.getHours()).slice(-2) + '-' + ('0' + this.getMinutes()).slice(-2);
};

module.exports = class Comscore {
    constructor(daxConfig, s3Config, site, settings, c) {
        if (s3Config.accessKeyId !== undefined && s3Config.secretAccessKey !== undefined) {
            let creds = new AWS.Credentials(s3Config.accessKeyId, s3Config.secretAccessKey)
            AWS.config.update({
                region: s3Config.region,
                correctClockSkew: true,
                credentials: creds
            });
        } else {
            AWS.config.update({
                region: s3Config.region,
                correctClockSkew: true
            });
        }

        s3Stream = require('s3-upload-stream')(new AWS.S3({ apiVersion: '2006-03-01' }));
        s3 = new AWS.S3({ apiVersion: '2006-03-01' });

        this.c = c;
        this.daxConfig = daxConfig;
        this.s3Config = s3Config;
        this.timer = null;
        this.options = {};
        this.site = site;
        this.settings = settings;
        this.id = 1;
        this.next = 1;
        this.status = 'Initiated';
        this.error = 0;
        this.count = 0;
    }

    start() {
        let self = this;

        this.status = 'Started';

        dax.site(self.daxConfig, self.site, self.c, (err, data) => {
            if (data === undefined || data.length === 0) {
                //beed to create a new data stream
                self.c.a('No Continue ID Found ' + self.site, 1, 'start');
                self.startNewDataStream();
            } else if (typeof data[0] === 'object' && data[0]['Continuation-Id']) {
                self.c.a('Continue ID Found ' + self.site + ' ' + data[0]['Continuation-Id'], 1, 'start');
                dax.continuation(self.daxConfig, self.site, data[0]['Continuation-Id'], self.c, (err, cont) => {
                    self.id = cont['Continuation-Id'];
                    self.next = cont['Next-Request-Date'];
                    self.continueDataStream();
                });
            } else {
                console.log('other');
                //do something else
            }
        });
    }

    continueDataStream() {
        let self = this;

        if (self.id === undefined) {
            self.start();
            return;
        }

        self.c.a('Continueing, setting continuation id ' + self.id, 1, 'startNewDataStream');
        this.status = 'Continuing';
        self.options = {
            baseUrl: self.daxConfig.url,
            uri: 'v1/continue',
            method: 'GET',
            headers: {
                "Accept-Encoding": "gzip"
            },
            qs: {
                corporate: self.daxConfig.corporate,
                user: self.daxConfig.user,
                password: self.daxConfig.password,
                continuationid: self.id
            }
        };

        self.getDataStream();
    }

    startNewDataStream() {
        let self = this;

        self.c.a('Creating new data stream ' + self.site, 1, 'startNewDataStream');

        self.options = {
            baseUrl: self.daxConfig.url,
            uri: 'v1/start',
            method: 'GET',
            headers: {
                "Accept-Encoding": "gzip"
            },
            qs: {
                corporate: self.daxConfig.corporate,
                user: self.daxConfig.user,
                password: self.daxConfig.password,
                startdate: dax.date(),
                site: self.site
            }
        };

        if (self.settings.eventfilter) {
            self.options.qs.eventfilter = self.settings.eventfilter;
        }

        if (self.settings.extralabels) {
            self.options.qs.extralabels = self.settings.extralabels;
        }

        if (self.settings.bucket) {
            self.bucket = self.settings.bucket;
        } else {
            self.bucket = 'puddle-csv.tools.bbc.co.uk';
        }

        self.getDataStream();

    }

    getDataStream() {
        let self = this;
        let timeout;

        const uploader = (x, i) => (
            new Promise((resolve, reject) => {
                const reader = fs.createReadStream(`./output/${x[i]}`);
                const zip = zlib.createGzip();
                const upload = s3Stream.upload({
                    Bucket: self.settings.bucket,
                    Key: `${x[i]}.gz`,
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

        const uploadSingle = (fileLoc) => {
            return new Promise((resolve, reject) => {
                self.c.a(`Uploading ${fileLoc}`, 1, 'getDataStream ' + self.site);
                const reader = fs.createReadStream(`./output/${fileLoc}`);
                const zip = zlib.createGzip();
                const upload = s3Stream.upload({
                    Bucket: self.settings.bucket,
                    Key: `${fileLoc}.gz`,
                });
                reader.pipe(zip).pipe(upload);
                upload.on('error', (e) => {
                    reject(e);
                });
                upload.on('uploaded', () => {
                    self.c.a(`Uploaded ${fileLoc}`, 1, 'getDataStream ' + self.site);
                    fs.unlinkSync(`./output/${fileLoc}`)
                    self.c.a(`Deleted ${fileLoc}`, 1, 'getDataStream ' + self.site);
                    upload.destroy();
                    resolve();
                });
            });
        }

        const deleteFiles = (files) => {
            for (let i in files) {
                fs.unlinkSync(files[i]);
            }
        };

        const setUploads = async () => {
            const files = fs.readdirSync('output');
            for (let i in files) {
                try {
                    self.c.a(`starting row ${i}`, 1, 'getDataStream ' + self.site);
                    await uploader(files, i);
                    self.c.a(`finished row ${i}`, 1, 'getDataStream ' + self.site);
                } catch (e) {
                    self.c.a(`Error with part ${i} ${e}`, 1, 'getDataStream ' + self.site);
                }
            }
            self.c.a(`Finished uploading file. ${i} ${e}`, 3, 'getDataStream ' + self.site);
            deleteFiles(files);
            self.processAndWait();
        };

        self.count++;
        
        //S3 settings
        let date = new Date();
        let filedate = date.fileDate();
 
        //start stream
        const writes = [];
        const gunzip = zlib.createGunzip();
        const stream = request(self.options);
        const rl = byline(stream.pipe(gunzip));
        let lines = 0;
        let total = 0;
        let part = 0;
        let announce = 0;
        const promises = [];
        let header;

        const sample = 250000;
        const dateData = {};

        self.c.a('Recieving data from Comscore', 1, 'getDataStream ' + self.site);
        self.c.a(`Created split file ${part}`, 1, 'getDataStream ' + self.site);

        var fs = require('fs');
        if (!fs.existsSync('./output')) {
            fs.mkdirSync('./output');
        }

        writes.push(fs.createWriteStream(`./output/${ filedate }-${ self.site }-${ self.id }-${ part }.csv`));

        // splits file at 1000000 rows so files dont get too big.

        rl.on('data', (line) => {
            if (total === 0) {
                header = line;
            }
            lines++;
            total++;
            
            if (lines > 1000000) {
                // upload file
                promises.push(uploadSingle(`${ filedate }-${ self.site }-${ self.id }-${ part }.csv`));
                part++;
                lines = 0;
                self.c.a(`Created split file ${part}`, 1, 'getDataStream ' + self.site);
                writes.push(fs.createWriteStream(`./output/${ filedate }-${ self.site }-${ self.id }-${ part }.csv`));
                writes[part].write(header + '\n');
            }
            writes[part].write(line + '\n');
            if (typeof line === 'string') {
                const cols = line.split('\t');
                const d = new Date(parseInt(cols[1]));
                
                if (isNaN(d.getTime()) === true) return;
                if ((total % sample) === 0) {
                    self.c.a(`processing ${d.toJSON()} at ${date.toJSON()}`, 1, 'getDataStream ' + self.site);
                }
                
                d.setUTCMinutes(0,0,0);
                if (dateData[d.toJSON()] !== undefined) {
                    dateData[d.toJSON()]++;
                } else {
                    dateData[d.toJSON()] = 1;
                }
            }
        });

        rl.on('end', () => {
            promises.push(uploadSingle(`${ filedate }-${ self.site }-${ self.id }-${ part }.csv`));
            self.c.a(JSON.stringify(dateData), 1, 'getDataStreamProcessed ' + self.site);
            self.c.a(`Finished downloading and splitting ${total} lines into ${part + 1} parts`, 1, 'getDataStream ' + self.site);
            Promise.all(promises).then(() => {
                writes.forEach((part) => {
                    part.destroy();
                })
                rl.destroy();
                clearTimeout(timeout);
                self.processAndWait();
            }, (e) => {
                console.log(e);
            })
        });

        //donwload events

        stream.on('error', (e) => {
            //there was an error 
            self.c.a(e, 2, 'getDataStream ' + self.site);
            self.error++;
            self.processAndWait();
        });

        stream.on('response', (res) => {
            self.count++;
            self.status = 'Respose Recieved';
            self.c.a('Response Recieved ' + self.site, 1, 'getDataStream->response');

            try {
                self.c.a(JSON.stringify(res), 1, 'getDataStream->response');
            } catch (e) {
                self.c.a(e, 1, 'getDataStream->response');
            }
            if (res.headers['x-cs-next-request-date'] && res.headers['x-cs-continuation-id']) {
                //we do, but lets just make sure the data is valid;
                try {
                    self.next = new Date(res.headers['x-cs-next-request-date']);
                } catch (e) {
                    //there isn't a timestamp
                    //this sometimes happens, we just need to try again
                    self.status = 'error';
                    self.c.a('No date found in header', 2, 'getDataStream ' + self.site);
                    stream.destroy('No date found in header');
                    return;
                }
                //test that this year is above epoch default
                if (self.next.getFullYear() > 2010) {
                    //we know the details for the next stream, but we don;t need it just yet,
                    //we store it, and also update the remote log so that if this crashes, we have the ability to get it directly
                    self.c.a('Next stream prepared', 1, 'getDataStream ' + self.site + ' ' + res.headers['x-cs-continuation-id']);
                    self.nextid = res.headers['x-cs-continuation-id'];
                    self.next = new Date(res.headers['x-cs-next-request-date']);
                    self.status = 'Downloading';
                    
                    // start timer
                    timeout = setTimeout(() => {
                        console.log('This interaction has been over 30 mins, we think it is in a comer and need to kill it');
                        process.abort();
                    }, 1800000);
                } else {
                    //not a valid year
                    //
                    //because this is not an actual date, i wonder if we should be sorting this out now rather than later
                    //
                    self.c.a('Comscore date response was not about year 2000', 2, 'getDataStream ' + self.site);
                    self.status = 'error';
                    stream.destroy('Comscore date response was not about year 2000');
                }
            } else {
                self.c.a('Comscore response did not provide the correct headers', 2, 'getDataStream ' + self.site, res.headers);
                //we should just restart the transfer rather than killing the process.
                self.status = 'error';
                stream.destroy('Comscore response did not provide the correct headers');
            }
        });
    }

    processAndWait() {
        let self = this;
        //wait for the time to be more then the present time
        let now;

        if (self.status === 'error') {
            self.wait(30);
            self.status = 'waiting-error';
        } else {
            //then send to continue
            self.timer = setInterval(() => {
                now = new Date();
                //wait till date is smaller then now

                //error here
                //2017-03-14-21:58:14 0|index    | TypeError: self.next.getTime is not a function


                if (self.next.getTime() <= now.getTime()) {
                    //send the request to comscore
                    clearInterval(self.timer);
                    self.c.a('Timer complete Continuing Stream ' + self.site, 1, 'processAndWait');
                    self.id = self.nextid;
                    self.continueDataStream();
                } else {
                    this.c.a('Waiting for ' + self.next + ' ' + self.site, 1, 'processAndWait');
                }
            }, 10000);
        }
    }

    stopTimer() {
        let self = this;
        clearInterval(self.timer);
    }

    wait(seconds) {
        let self = this;
        self.error++;

        if (self.error > 5) {
            self.c.a('Multilple errors on ' + self.site, 3, 'wait');
            self.c.a('Calling Comscore Continue API ' + self.site, 3, 'wait');
            self.start();
        } else {
            setTimeout(() => {
                self.processAndWait();
            }, seconds * 1000);
        }
    }

    getErrors(cb) {
        let self = this;
        self.error++;
        cb(null, self.error, self.status);
    }

};