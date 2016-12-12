'use strict';
process.env.TZ = 'Europe/London';
const request = require('request');
const AWS = require('aws-sdk');
const dax = require('./dax-utils');
const Logback = require('logback');
let s3Stream;
let s3;

Date.prototype.fileDate = function() {
    return this.getFullYear() + '' + (this.getMonth() + 1) + '' + this.getDate() + '' + this.getHours() + '' + this.getMinutes();
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

        self.getDataStream();

    }

    getDataStream() {
        let self = this;

        //S3 settings
        let date = new Date();
        let filedate = date.fileDate();
        let upload = s3Stream.upload({
            "Bucket": self.s3Config.bucket,
            "Key": filedate + "-" + self.site + "-" + self.id + ".csv.gz"
        });

        self.count++;

        //start stream
        let stream = request(self.options);
        stream.pipe(upload);

        self.c.a('Recieving data from Comscore', 1, 'getDataStream ' + self.site);

        //Stream events
        upload.on('error', (error) => {
            console.log(error);
            self.c.a('Error uploading file ' + self.site, 3, 'getDataStream->error');
            self.error++;
            self.status = 'error';
            self.processAndWait();
        });

        upload.on('uploaded', (details) => {
            self.c.a('Uploaded File ' + self.site, 1, 'getDataStream->uploaded');
            self.status = 'Uploaded File';
            self.processAndWait();
        });

        stream.on('response', (res) => {
            self.count++;
            self.status = 'Respose Recieved';
            self.c.a('Response Recieved ' + self.site, 1, 'getDataStream->response');

            if (res.headers['x-cs-next-request-date'] && res.headers['x-cs-continuation-id']) {
                //we do, but lets just make sure the data is valid;
                try {
                    self.next = new Date(res.headers['x-cs-next-request-date']);
                } catch (e) {
                    //there isn't a timestamp
                    //this sometimes happens, we just need to try again
                    self.status = 'error';
                    return;
                }

                //test that this year is above epoch defult
                if (self.next.getFullYear() > 2010) {
                    //we know the details for the next stream, but we don;t need it just yet,
                    //we store it, and also update the remote log so that if this crashes, we have the ability to get it directly
                    self.c.a('Next stream prepared', 1, 'getDataStream ' + self.site + ' ' + res.headers['x-cs-continuation-id']);
                    self.id = res.headers['x-cs-continuation-id'];
                    self.next = new Date(res.headers['x-cs-next-request-date']);
                    self.status = 'Downloading';
                } else {
                    //not a valid year
                    //
                    //because this is not an actual date, i wonder if we should be sorting this out now rather than later
                    //
                    self.c.a('Comscore date response was not about year 2000', 3, 'getDataStream ' + self.site);
                    self.status = 'error';
                }

            } else {

                self.c.a('Comscore response did not provide the correct headers', 3, 'getDataStream ' + self.site, res.headers);
                //we should just restart the transfer rather than killing the process.
                self.status = 'error';
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
                if (self.next <= now) {
                    //send the request to comscore
                    clearInterval(self.timer);
                    self.c.a('Timer complete Continuing Stream ' + self.site, 1, 'processAndWait');
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
        }

        setTimeout(() => {
            self.processAndWait();
        }, seconds * 1000);
    }

    getErrors(cb) {
        let self = this;
        self.error++;
        cb(null, self.error, self.status);
    }

};