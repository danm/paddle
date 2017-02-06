'use strict';
process.env.TZ = 'Europe/London';
const request = require('request');

exports.site = (dax, site, c, cb) => {

    let config = {
        baseUrl: dax.url,
        uri: 'v1/status',
        gzip: true,
        headers: {
            "Accept-Encoding": "gzip"
        },
        qs: {
            corporate: dax.corporate,
            user: dax.user,
            password: dax.password,
            site: site
        }
    };

    c.a('Getting Comscore Site status for ' + site);

    //annoying the body comes back as a string so we have to parse it


    request(config, (err, res, body) => {

        if (err) cb(err);
        let data = body.split('\r\n');

        let obj;
        c.a('requesting Status from Comscore Live', 1, 'comscore');
        c.a(JSON.stringify(data), 1, 'comscore');
        if (data.length === 1) {
            cb(null);
        } else if (data.length === 4) {

            //4 lines is would be what we expect, 
            if (data[0].indexOf('Continuation-Id: ') === 0 &&
                data[1].indexOf('Request-Date: ') === 0) {

                c.a('single Cont ID found', 1, 'comscore');

                if (data[0].indexOf('Continuation-Id: ') === 0) {
                    obj = { 'Continuation-Id': parseInt(data[0].slice(16).trim()) };
                }
                if (data[1].indexOf('Request-Date: ') === 0) {
                    obj['Request-Date'] = new Date(data[1].slice(14).trim());
                }
                obj.url = '/sites/' + site + '/continuation/' + obj['Continuation-Id'];

                c.a(JSON.stringify(obj), 1, 'comscore');

                cb(null, [obj]);
            }

        } else if (data.length > 4) {
            console.log(data.length);
            let arr = [];

            //what is going on here?

            if (data[0] && data[1] && data[0].indexOf('Continuation-Id: ') === 0 && data[1].indexOf('Request-Date: ') === 0) {
                let obj = {
                    'Continuation-Id': parseInt(data[0].slice(16).trim()),
                    'Request-Date': new Date(data[1].slice(14).trim()),
                    url: '/sites/' + site + '/continuation/' + parseInt(data[0].slice(16).trim())
                };
                arr.push(obj);
            }

            if (data[3] && data[4] && data[3].indexOf('Continuation-Id: ') === 0 && data[4].indexOf('Request-Date: ') === 0) {
                let obj = {
                    'Continuation-Id': parseInt(data[3].slice(16).trim()),
                    'Request-Date': new Date(data[4].slice(14).trim()),
                    url: '/sites/' + site + '/continuation/' + parseInt(data[3].slice(16).trim())
                };
                arr.push(obj);
            }

            if (data[6] && data[7] && data[6].indexOf('Continuation-Id: ') === 0 && data[7].indexOf('Request-Date: ') === 0) {
                let obj = {
                    'Continuation-Id': parseInt(data[6].slice(16).trim()),
                    'Request-Date': new Date(data[7].slice(14).trim()),
                    url: '/sites/' + site + '/continuation/' + parseInt(data[6].slice(16).trim())
                };
                arr.push(obj);
            }

            if (data[9] && data[10] && data[9].indexOf('Continuation-Id: ') === 0 && data[10].indexOf('Request-Date: ') === 0) {
                let obj = {
                    'Continuation-Id': parseInt(data[9].slice(16).trim()),
                    'Request-Date': new Date(data[10].slice(14).trim()),
                    url: '/sites/' + site + '/continuation/' + parseInt(data[9].slice(16).trim())
                };
                arr.push(obj);
            }

            if (data[12] && data[13] && data[12].indexOf('Continuation-Id: ') === 0 && data[13].indexOf('Request-Date: ') === 0) {
                let obj = {
                    'Continuation-Id': parseInt(data[12].slice(16).trim()),
                    'Request-Date': new Date(data[13].slice(14).trim()),
                    url: '/sites/' + site + '/continuation/' + parseInt(data[12].slice(16).trim())
                };
                arr.push(obj);
            }

            arr.sort(function(a, b) {
                return a['Request-Date'] < b['Request-Date'];
            });

            c.a('Multile Continue Options Available. Picking most recent: ' + arr[0]['Request-Date'] + ' out of ' + arr.length + ' choices');
            cb(null, arr);

        } else {
            cb(null);
        }
    });
};

exports.continuation = (dax, site, cont, c, cb) => {
    let config = {
        baseUrl: dax.url,
        uri: 'v1/status',
        gzip: true,
        headers: {
            "Accept-Encoding": "gzip"
        },
        qs: {
            corporate: dax.corporate,
            user: dax.user,
            password: dax.password,
            site: site,
            continuationid: cont
        }
    };

    c.a('Getting Comscore Continuation for Site ' + site);

    //annoying the body comes back as a string so we have to parse it
    request(config, (err, res, body) => {
        if (err) cb(err);
        let data = body.split('\r\n');
        let obj = {};
        if (data[0].indexOf('Continuation-Id: ') === 0 &&
            data[1].indexOf('Site: ') === 0 &&
            data[2].indexOf('Previous-Continuation-Id: ') === 0 &&
            data[3].indexOf('Request-Date: ') === 0 &&
            data[4].indexOf('Next-Request-Date: ') === 0 &&
            data[5].indexOf('Extra-Labels: ') === 0 &&
            data[6].indexOf('Events-Returned: ') === 0 &&
            data[7].indexOf('Total-Events-Returned: ') === 0 &&
            data[8].indexOf('Bytes-Returned: ') === 0 &&
            data[9].indexOf('Total-Bytes-Returned: ') === 0 &&
            data[10].indexOf('Event-Filter-Id: ') === 0
        ) {
            obj = {
                'Continuation-Id': parseInt(data[0].slice('Continuation-Id:'.length).trim()),
                'Site': data[1].slice('Site:'.length).trim(),
                'Previous-Continuation-Id': data[2].slice('Previous-Continuation-Id:'.length).trim(),
                'Request-Date': data[3].slice('Request-Date:'.length).trim(),
                'Next-Request-Date': data[4].slice('Next-Request-Date:'.length).trim(),
                'Extra-Labels': data[5].slice('Extra-Labels:'.length).trim(),
                'Events-Returned': data[6].slice('Events-Returned:'.length).trim(),
                'Total-Events-Returned': data[7].slice('Total-Events-Returned:'.length).trim(),
                'Bytes-Returned': data[8].slice('Bytes-Returned:'.length).trim(),
                'Total-Bytes-Returned': data[9].slice('Total-Bytes-Returned:'.length).trim(),
                'Event-Filter-Id': data[10].slice('Event-Filter-Id:'.length).trim()
            };
            cb(null, obj);
        } else {
            cb(null, res);
        }
    });
};

exports.date = () => {

    //yyyymmdd
    var today = new Date();
    var todayString = String(today.getFullYear());
    var month = today.getMonth() + 1;

    if (month < 10) {
        todayString += '0' + String(month);
    } else {
        todayString += String(month);
    }

    var day = today.getDate();

    if (day < 10) {
        todayString += '0' + String(day);
    } else {
        todayString += String(day);
    }

    return todayString;

};