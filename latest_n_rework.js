const puppeteer = require('puppeteer');
const cluster = require('cluster');
const {redisClient} = require('./DB.js');
const {getWebDriver} = require('./CRWorker.js');

const SERVER_ADDR = 'http://dynamics.company.com';
const getTaskURL = (otype = '') => `${SERVER_ADDR}/Dynamics/_root/homepage.aspx?etc=${otype}&pagemode=iframe`;
const timeStamp = _ => new Date().toLocaleString();
const isCRNum = (str = '') => str.match(/^[TS]CR-\d+(\.(\w+))+$/);
const isDate = (str = '') => str.match(/^\d+/) && str.match(/ (AM|PM)$/);
const trimCurly = (str = '') => str.slice(1, -1);
const SECOND = 1000;

const JOB = 'JOBS:CR';
const JOB_NEW = 'JOBS:NEW:CR';
const PORT = 6380;

async function latest(page, otype) {
    await page.goto(getTaskURL(otype));

    const selector = `tr[class="ms-crm-List-Row"][otype="${otype}"]`;
    const pageFunc = arr => arr.map(e => [e.getAttribute('oid'), e.innerText]);
    const rawArr = await page.$$eval(selector, pageFunc); //console.log(rawArr);

    return rawArr.map(e => {
        const oid = trimCurly(e[0]);
        const cols = e[1].split('\n');
        const crn = cols.filter(isCRNum)[0];
        const age = Date.parse(cols.filter(isDate)[0]);
        return {crn, oid, age};
    });
}

async function startPoll({otype, job = JOB_NEW}, interval = 30 * SECOND) {
    async function refresh() {
        let taskList = [];
        let delay = interval;
        try {
            const client = await redisClient(PORT);

            let page = await getWebDriver({headless: true});
            taskList = await latest(page, otype);
            page.browser().close();

            await Promise.all(taskList.map(async ele => {
                const {crn, oid, age} = ele;
                const msg = JSON.stringify({crn, otype, oid});
                const cnt = await client.zaddAsync('RECORD:CR', 'CH', age, msg);
                if(cnt > 0) {
                    client.saddAsync(job, msg);
                    console.log(timeStamp(), `> Observed ${msg}`);
                    setTimeout(async _ => {
                        const secondClient = await redisClient(PORT);
                        await secondClient.saddAsync(job, msg);
                        secondClient.quit();
                        console.log(timeStamp(), `> Lazy observed ${msg}`);
                    }, 3 * 60 * SECOND);
                }
            }));
            client.quit();
        } catch(e) {
            console.error(e);
            delay = interval * 4;
        }
        setTimeout(refresh, delay);
        console.log(timeStamp(), `> Polled ${taskList.length || 0} otype[${otype}] tasks every ${delay}mS`); 
    };
    return refresh;
}


async function startRework({otype, job = JOB}, interval = 24 * 3600 * SECOND) {
    async function rework() {
        try {
            const browser = await puppeteer.launch({headless: true});
            let [page] = await browser.pages();
            const taskList = await latest(page, otype);
            const client = redisClient();
            await client.saddAsync(job, taskList.map(ele => JSON.stringify({crn: ele.crn, otype, oid: ele.oid})));
            client.quit();
            await browser.close();
            setTimeout(rework, interval);
            console.log(timeStamp(), `> Reworking ${taskList.length} otype[${otype}] tasks every ${interval / SECOND}S`);
        } catch(e) {
            console.error(e);
            setTimeout(rework, interval);
            console.log(timeStamp(), `> Try to rework otype[${otype}] after ${interval / SECOND}S`); 
        }
    };
    return rework;
    //await browser.close();
}


//------------------------------------------------------------------

if (cluster.isMaster) {

    cluster.fork().send({otype: 10061});
    cluster.fork().send({otype: 10173});

} else {

    process.on('message', async msg => {
        const poll = await startPoll(msg);
        poll();

        //const rework = await startRework(msg);
        //rework();
    });
}

