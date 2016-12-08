'use strict';
const express = require('express');
const rest = express();

const startServer = function(app) {

    rest.get('/', function(req, res) {
        res.json({
            Message: 'Welcome to Paddle',
            Options: [
                '/config',
                '/errors',
                '/sites',
                '/workers',
                '/start'
            ]
        });
    });

    //Globals
    rest.get('/config', function(req, res) {
        app.getGlobalConfig((err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    //Get Errors
    rest.get('/errors', function(req, res) {
        app.getErrors((err, errors, status) => {
            if (err) res.json({ Message: err });
            res.json({ Message: [errors, status] });
        });
    });


    //get the sites from the config file
    rest.get('/sites', function(req, res) {
        app.getComscoreSites((err, sites) => {
            if (err) res.json({ Message: err });
            res.json({ Message: sites });
        });
    });

    //get the sites from the config file
    rest.get('/workers', function(req, res) {
        app.getPaddleWorkers((err, sites) => {
            if (err) res.json({ Message: err });
            res.json({ Message: sites });
        });
    });


    //Start the workers
    rest.get('/start', function(req, res) {
        app.startWorkers((err, data) => {
            res.json({ Message: data });
        });
    });

    //Indervidual site
    rest.get('/sites/:site', function(req, res) {
        let list = [
            '/sites/' + req.params.site + '/status',
            '/sites/' + req.params.site + '/paddle',
            '/sites/' + req.params.site + '/config',
            '/sites/' + req.params.site + '/start',
            '/sites/' + req.params.site + '/stop',
            '/sites/' + req.params.site + '/reset',
            '/sites/' + req.params.site + '/continuation'
        ];
        res.json({ Message: list });
    });

    //gets data from comscore about this site
    rest.get('/sites/:site/status', function(req, res) {
        app.getComscoreSite(req.params.site, (err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    //gets data from comscore about this site
    rest.get('/sites/:site/continuation', function(req, res) {
        res.json({ Message: ['Please Supply ContinuationID', '/sites/' + req.params.site + '/status'] });
    });

    //gets data from comscore about this continuation
    rest.get('/sites/:site/continuation/:cid', function(req, res) {
        app.getComscoreContinuation(req.params.site, req.params.cid, (err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    //gets config data from s3 about this site
    rest.get('/sites/:site/config', function(req, res) {
        app.getSiteConfig(req.params.site, (err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    //starts streaming (starts new or continues based on status)
    rest.get('/sites/:site/start', function(req, res) {
        app.startSite(req.params.site, (err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    //starts new stream
    rest.get('/sites/:site/reset', function(req, res) {
        app.resetSite(req.params.site, (err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    //stops the stream
    rest.get('/sites/:site/stop', function(req, res) {
        app.stopSite(req.params.site, (err, data) => {
            if (err) res.json({ Message: err });
            res.json({ Message: data });
        });
    });

    rest.listen(app.port);
};

module.exports = class Restful {
    constructor(app) {
        startServer(app);
    }
};