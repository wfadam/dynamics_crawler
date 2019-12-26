const cluster = require('cluster');
const {redisClient} = require('./DB.js');
const {getWebDriver} = require('./CRWorker.js');

const SERVER_ADDR = 'http://dynamics.company.com';
const getTaskURL = (otype = '') => `${SERVER_ADDR}/Dynamics/_root/homepage.aspx?etc=${otype}&pagemode=iframe`;
const timeStamp = _ => new Date().toLocaleString();
const isCRNum = (str = '') => str.match(/^[TS]CR-\d+(\.(\w+))+$/);
const isDate = (str = '') => str.match(/^\d+/) && str.match(/ (AM|PM)$/);
const trimCurly = (str = '') => str.slice(1, -1);

const JOB_NEW = 'JOBS:NEW:CR';
const PORT = 6380;
const LAZY_JOB_DELAY = 3 * 60 * 1000;
const POLL_DELAY = 30 * 1000;

async function crawl(otype) {
    const selector = `tr[class="ms-crm-List-Row"][otype="${otype}"]`;
    const pageFunc = arr => arr.map(e => [e.getAttribute('oid'), e.innerText]);

    const t0 = Date.now();
    const page = await getWebDriver({headless: true});
    await page.goto(getTaskURL(otype));
    const rawArr = await page.$$eval(selector, pageFunc); //console.log(rawArr);
    await page.browser().close();

    console.log(`${timeStamp()} > Poll ${rawArr.length} otype[${otype}] in ${Date.now() - t0}mS`); 
    return rawArr.map(e => {
        const oid = trimCurly(e[0]);
        const cols = e[1].split('\n');
        const crn = cols.filter(isCRNum)[0];
        const age = Date.parse(cols.filter(isDate)[0]);
        return {crn, oid, age};
    });
}

async function getJobs(otype, list) {
    const t0 = Date.now();
    const client = await redisClient(PORT, 'getJobs()');
    const taskList = [];
    await Promise.all(list.map(async ele => {
        const {crn, oid, age} = ele;
        const msg = JSON.stringify({crn, otype, oid});
        const cnt = await client.zaddAsync('RECORD:CR', 'CH', age, msg);
        if(cnt > 0) {
            taskList.push(msg);
        }
    }));
    await client.quitAsync();
    console.log(`${timeStamp()} > Query otype[${otype}] database in ${Date.now() - t0}mS`); 
    return taskList;
}

async function addJobs(job, list) {
    const client = await redisClient(PORT, 'addJobs()');
    await client.saddAsync(job, list);
    await client.quitAsync();
    console.log(`${timeStamp()} > Observed`, list);
}

async function buildPoll({otype, job = JOB_NEW}, interval = POLL_DELAY) {
    async function poll() {
        let delay = interval;
        try {
            const rawList = await crawl(otype);
            const taskList = await getJobs(otype, rawList);
            if(taskList.length > 0) {
                addJobs(job, taskList);
                setTimeout(_ => addJobs(job, taskList), LAZY_JOB_DELAY);
            }
        } catch(e) {
            console.error(e);
            delay = interval * 4;
            console.error(`${timeStamp()} > Try in ${delay / 1000}S`);
        } finally {
            setTimeout(poll, delay);
        }
    };
    return poll;
}

//------------------------------------------------------------------

if (cluster.isMaster) {
    cluster.fork().send({otype: 10061});
    setTimeout(_ => cluster.fork().send({otype: 10173}), POLL_DELAY / 2);
} else {
    process.on('message', async msg => {
        const poll = await buildPoll(msg);
        poll();
    });
}
