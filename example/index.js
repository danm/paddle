const Paddle = require('../index');

const config = {
    port: 3000,
    service: "",
    s3: {
        bucket: "",
        region: "eu-west-1",
        accessKeyId: '', //optional
        secretAccessKey: '' //optional
    },
    dax: {
        url: "",
        corporate: "",
        user: "",
        password: ""
    },
    logback: {
        elastic: {
            endpoint: '',
            level: 0 //warnings and errors (json only)
        },
        cloudwatch: {
            level: 3, //Errors only,
            region: 'eu-west-1',
            logGroupName: '',
            logStreamName: '',
            accessKeyId: '', //optional
            secretAccessKey: '' //optional
        }
    }
};

const paddle = new Paddle(config);