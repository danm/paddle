const Paddle = require('../index');
const configuration = require('abacus-pipline-configurator');

const config = configuration.getConfig;

const paddle = new Paddle(config);
paddle.startWorkers((err, res) => {
    console.log(err, res);
});