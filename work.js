const {redisClient} = require('./DB.js');
const {workOn, workDoc} = require('./CRWorker.js');
const {getWebDriver} = require('./browser.js');
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const DAY = 24 * 3600 * 1000;
const timeStamp = _ => new Date().toLocaleString();

const JOB = 'JOBS:CR';
const JOB_NEW = 'JOBS:NEW:CR';
const JOB_DOC = 'JOBS:DOC';
const PORT = 6380;

async function assignWorker(driver, fn, jobKind) {
    let page;
    async function worker() {
        let interval = 5 * SECOND;
        try {
            const client = await redisClient(PORT, 'assignWorker()');
            const msg = await client.spopAsync(jobKind);
            await client.quitAsync();
            if(msg) {
                if(! page) {
                    page = await driver.newPage();
                }
                await fn(page, JSON.parse(msg));
                interval = 0;
            } else {
                if(page) {
                    await page.close();
                    page = null;
                }
            }
        } catch(e) {
            console.error(`worker[${process.pid}]`, e);
        }
        setTimeout(worker, interval);
    }    

    worker();
    console.log(`${timeStamp()} > Created worker for ${jobKind}`);
}

//------------------------------------------------------------------

module['workOn'] = workOn;
module['workDoc'] = workDoc;

(async _ => {
    let driver = await getWebDriver({headless: true});
    const cpus = require('os').cpus();
    const PARALLEL = Math.min(8, cpus.length);
    for(let cnt = 0; cnt < PARALLEL; cnt++) {
        let opts;
        if(cnt < 1)         opts = {fn: 'workOn', jobKind: JOB_NEW};
        else if(cnt < 2)    opts = {fn: 'workOn', jobKind: JOB};
        else                opts = {fn: 'workDoc', jobKind: JOB_DOC};

        assignWorker(driver, module[opts.fn], opts.jobKind);
    }
})();

