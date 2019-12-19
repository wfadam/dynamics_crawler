const {Builder, By, Key} = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const cluster = require('cluster');
const {redisClient} = require('./DB.js');
const {workOn, workDoc, getWebDriver} = require('./CRWorker.js');
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const DAY = 24 * 3600 * 1000;
const timeStamp = _ => new Date().toLocaleString();

const JOB = 'JOBS:CR';
const JOB_NEW = 'JOBS:NEW:CR';
const JOB_DOC = 'JOBS:DOC';
const PORT = 6380;

async function assignWorker(fn, jobKind) {
    let driver;
    let interval;
    async function worker() {
        try {
            const client = await redisClient(PORT);
            const msg = await client.spopAsync(jobKind);
            client.quit();
            if(msg) {
                if(! driver) driver = await getWebDriver({headless: true});
                await fn(driver, JSON.parse(msg));
                interval = 0;
            } else {
                if(driver) {
                    await driver.browser().close();
                    driver = null;
                }
                interval = 5 * SECOND;
            }
        } catch(e) {
            console.error(e);
        }
        setTimeout(worker, interval);
    }    

    worker();
    console.log(`${timeStamp()} > Created worker[${process.pid}] for ${jobKind}`);
}

//------------------------------------------------------------------

module['workOn'] = workOn;
module['workDoc'] = workDoc;

if (cluster.isMaster) {

    const PARALLEL = require('os').cpus().length;
    for(let cnt = 0; cnt < PARALLEL; cnt++) {
        let opts;
        if(cnt < 2)
            opts = {fn: 'workOn', jobKind: JOB_NEW};
        else if(cnt < 4) 
            opts = {fn: 'workOn', jobKind: JOB};
        else
            opts = {fn: 'workDoc', jobKind: JOB_DOC};

        cluster.fork().send(opts);
    }

} else {

    process.on('message', msg => {
        assignWorker(module[msg.fn], msg.jobKind);
    });

}

