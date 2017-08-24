'use strict';

process.env.TZ = 'Europe/London';
const request = require('request');
const Logback = require('logback');
const Comscore = require('./Comscore');
const dax = require('./dax-utils');
const Restful = require('./Restful');

module.exports = class Paddle {
    constructor(config) {
        if (config.logback) {
            this.c = new Logback('paddle', config.logback, 'json');
        } else {
            this.c = new Logback('paddle');
        }

        this.c.a('Starting Paddle', 2);
        if (config.service) { this.service = config.service; } else { this.c.a('No service file location set', 3); throw new Error('No service file location set'); }
        if (config.port) { this.port = config.port; } else { this.port = 3000; }
        this.c.a('Port set to ' + this.port);
        if (config.dax) { this.dax = config.dax; } else { this.c.a('No DAX Settings found', 3); throw new Error('No DAX Settings found'); }
        if (config.s3) { this.s3 = config.s3; } else { this.c.a('No S3 Settings found', 3); throw new Error('No S3 Settings found'); }

        this.services = [];
        this.workers = {};
        this.Rest = new Restful(this);
    }

    //gets the services file from S3
    // /config
    getGlobalConfig(callback) {
        let self = this;
        let config = {
            baseUrl: self.service,
            uri: 'service.json',
            json: true
        };

        request(config, function(err, res, body) {
            if (err) {
                this.c.a('Failed to get Status', 3);
                throw new Error('Failed to get Status');
            }
            if (typeof body === 'object') {
                self.services = body.services;
                callback(null, body.services);
            } else {
                this.c.a('Invalid response from server, probably permissions issues contacting statuf file on init', 3);
                throw new Error('Invalid response from server, probably permissions issues contacting statuf file on init');
            }
        });
    }

    //what the workers are processing
    // /workers
    getPaddleWorkers(cb) {
        let sites = [];

        for (let site in this.workers) {
            sites.push({ site: this.workers });
        }

        if (sites.length > 0) {
            cb(null, this.workers);
        } else {
            cb(null, "No Workers Running");
        }
    }

    //what the workers are processing
    // /sites
    getComscoreSites(cb) {
        //get all sites /sites
        let self = this;
        this.getGlobalConfig((err) => {
            if (err) cb(err);
            let sites = [];
            self.services.forEach((row) => {
                sites.push('/sites/' + row.site);
            });
            cb(null, sites);
        });
    }

    //start the workers
    startWorkers(cb) {
        let self = this;
        let sites = [];
        let messages = [];

        for (let site in self.workers) {
            sites.push('/sites/' + site);
        }

        if (sites.length > 0) {
            //there are already workers running
            cb(null, 'Already Running');
        } else {
            //no runners found, requesting the config file
            self.getGlobalConfig(err => {
                //process the services included in the config
                self.services.forEach((row) => {
                    //create a Comscore instance for each service
                    self.getSiteConfig(row.site, (err, settings) => {
                        self.workers[row.site] = new Comscore(self.dax, self.s3, row.site, settings, self.c);
                        self.workers[row.site].start();
                        self.workers[row.site].started = new Date();
                        messages.push('Started New Worker - ' + row.site);
                    });
                });
                messages.push('Started');
                cb(null, messages);
            });
        }
    }

    //get single site
    //sites/:site/status
    getComscoreSite(site, cb) {
        let self = this;
        dax.site(self.dax, site, c, function(err, data) {
            if (data === undefined) {
                cb(null, 'No continuation ids found');
            } else {
                cb(null, data);
            }
        });
    }

    //get single site
    //sites/:site/continuation
    getComscoreContinuation(site, continuationid, cb) {
        let self = this;
        dax.continuation(self.dax, site, continuationid, c, function(err, data) {
            cb(null, data);
        });
    }

    //get the site config
    //sites/:site/config
    getSiteConfig(site, cb) {
        let self = this;
        let config = {
            baseUrl: self.service,
            uri: 'service.json',
            json: true
        };

        request(config, (err, res, body) => {
            if (err) cb(err);
            for (let i = 0; i < body.services.length; i++) {
                if (site === body.services[i].site) {
                    cb(null, body.services[i]);
                }
            }
        });
    }

    //Start the worker for the site selected /sites/:site/start
    startSite(site, cb) {
        let self = this;
        if (self.workers[site]) {
            cb(null, 'Already Running');
        } else {
            self.getSiteConfig(site, (err, options) => {
                self.workers[site] = new Comscore(self.dax, self.s3, site, options, self.c);
                self.workers[site].start();
                cb(null, 'Started');
            });
        }
    }

    //Restart the worker for the site selected /sites/:site/restart
    resetSite(site, cb) {
        let self = this;
        //delete the remote file and create a new one
        if (self.workers[site] !== undefined) {
            self.workers[site].startNewDataStream();
            cb(null, 'Restarted Worker');
        } else {
            self.getSiteConfig(site, (err, options) => {
                self.workers[site] = new Comscore(self.dax, self.s3, site, options, self.c);
                self.workers[site].startNewDataStream();
                cb(null, 'Started New Worker');
            });
        }
    }

    //Stop the worker for the site selected /sites/:site/stop
    stopSite(site, cb) {
        let self = this;
        if (self.workers[site]) {
            self.workers[site].stopTimer();
            delete self.workers[site];
            cb(null, 'Stopped');
        } else {
            cb(null, 'Already Stopped');
        }
    }
};