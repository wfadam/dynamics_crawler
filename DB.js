const redis = require('redis');
const bluebird = require('bluebird');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const IP = '10.71.25.105';

async function redisClient(port, name = '') { 
    if(! port) {
        return Promise.reject('Forget to specify database port ?');
    }
    return new Promise((resolve, reject) => {
        let client = redis.createClient(port, IP);
        client.on('error', err => reject(err));
        client.on('ready', _ => resolve(client));
        client.client('SETNAME', name);
    });
}

exports.redisClient = redisClient;
