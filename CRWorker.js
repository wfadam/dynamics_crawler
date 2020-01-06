const R = require('ramda');
const path = require('path');

const {getWebDriver} = require('./browser.js');
const {redisClient} = require('./DB.js');
const PORT = 6380;

const SERVER_ADDR = 'http://dynamics.company.com';
const JOB_DOC = 'JOBS:DOC';
const getDataURL = (otyp, uid) => `${SERVER_ADDR}/Dynamics/form/Data.aspx?etc=${otyp}&id=%7b${uid}%7d&oid=${uid}`;
const getPageURL = (otyp, uid) => `${SERVER_ADDR}/Dynamics/main.aspx?etc=${otyp}&id=%7b${uid}%7d&newWindow=true&pagetype=entityrecord`;
const getFilePageURL = (otype, uid) => `${SERVER_ADDR}/Dynamics/tools/documentmanagement/areas.aspx?oType=${otype}&oId=%7b${uid}%7d`;
const timeStamp = _ => `${new Date().toLocaleString()}`;
const trimCurly = (str = '') => str.slice(1, -1);
const TIMEOUT = 60 * 1000;

function flatProps(...props) {
    return (obj = {}) => {
        let slimObj = {};
        Object.keys(obj).forEach(key => {
            slimObj[key] = props.map(p => obj[key][p]).toString();
        });
        return slimObj;
    }
}

function filterKey(pat = '') {
    return (obj = {}) => {
        let thin = {};
        Object.keys(obj)
            .filter(k => k.match(pat))
            .forEach(k => thin[k] = obj[k]);
        return thin;
    };
}

function filterPeople() {
    return (obj = {}) => {
        let slimObj = {};
        Object.keys(obj).forEach(key => {
            if(obj[key]['otype'] === '8') {
                slimObj[key] = obj[key];
            }
        });
        return slimObj;
    }
}

function pickProps(...props) {
    return (obj = {}) => {
        let slimObj = {};
        Object.keys(obj).forEach(key => {
            let tmp = {};
            for(let prop of props) {
                if(obj[key][prop]) {
                    tmp[prop] = obj[key][prop];
                }
            }
            if(Object.keys(tmp).length > 0) {
                slimObj[key] = tmp;
            }
        });
        return slimObj;
    }
}

const cherryPick = R.compose(
    flatProps('value'),
    filterKey(/^[^_]/)
);

const pickPeople = R.compose(
    pickProps('value', 'otype', 'oid'),
    filterPeople()
);

function xmlPath(obj = {}) {
    for(let key in obj) {
        if(key.toLowerCase().endsWith('.xml')) {
            return obj[key];
        }
    }
}

async function fetchJson(page, otype, oid) {
    try {
        await page.goto(getDataURL(otype, oid));
        let raw = await page.$eval('body', e => e.innerText);
        let {formData} = JSON.parse(raw.replace('while(1);', ''));
        if(formData) return formData;
        throw new Error(JSON.parse(raw)._error.Description);
    } catch(e) {
        console.error(`${timeStamp()} >`, e);
    } 
}

async function fetchAttachment(driver, otype, oid) {
    try {
        await driver.goto(getFilePageURL(otype, oid), {timeout: TIMEOUT});
        const frames = await driver.frames();

        let errstyle = await frames[0].$eval('#errorMessageArea', e => e.getAttribute('style'));
        if(errstyle.includes('display: inline')) {
            return undefined;
        }

        const rst = await frames[1].$$eval('tr[docurl]', arr => arr.map(ele => {
            const docurl = ele.getAttribute('docurl');
            const obj = ele.querySelectorAll('td');
            const modT = obj[2].innerText;
            const owner = obj[3].innerText;
            return {docurl, modT, owner};
        }));

        const docObj = {};
        for(let file of rst) {
            let {docurl, modT, owner} = file;
            let fileName = [modT, owner, path.basename(docurl)].join(' | ');
            docObj[fileName] = docurl;
        }

        return docObj;
    } catch(e) {
        return undefined;
    }
}

async function savePeople(modifiedon = '', obj = {}) {
    const client = await redisClient(PORT, 'savePeople()');
    try {
        await Promise.all(Object.keys(obj).map(async key => {
            let {value, otype, oid} = obj[key];
            oid = trimCurly(oid);
            const orgModT = await client.hgetAsync('LASTUPDATE:PEOPLE', JSON.stringify({name: value, otype, oid}));
            let realModT = Math.max(Date.parse(modifiedon) || 0, orgModT || 0);
            await client.hsetAsync('LASTUPDATE:PEOPLE', JSON.stringify({name: value, otype, oid}), realModT);
        }));
    } catch(e) {
        console.error(e);
    } finally {
        client.quit();
    }
};

async function workOn(page, {crn, otype, oid}) {
    let t0 = Date.now();

    const raw = await fetchJson(page, otype, oid);

    const obj = cherryPick(raw); 
    if(Object.keys(obj).length === 0) {
        return console.error(`${timeStamp()} > Bad object from ${oid} ${crn}`);
    }

    let {modifiedon = ''} = obj;
    savePeople(modifiedon, pickPeople(raw));

    obj['URL'] = getPageURL(otype, oid);
    obj['LAST_POLL'] = timeStamp();

    let {zsd_scrnumber, zsd_tcrnumber} = obj;
    let key = zsd_scrnumber || zsd_tcrnumber;

    const client = await redisClient(PORT, 'workOn()');
    await Promise.all([
        client.hmsetAsync(key, obj),
        client.zaddAsync('TIMELINE:CR', Date.parse(obj['modifiedon']), JSON.stringify(summarize(key, obj))),
        client.zaddAsync('RECORD:CR', Date.parse(obj['modifiedon']), JSON.stringify({crn: key, otype, oid: oid.toUpperCase()})) 
    ])
    .then(async _ => {
        await client.saddAsync(JOB_DOC, JSON.stringify({crn: key, otype, oid}));
        console.log(`${timeStamp()} > Worker[${process.pid}] updated ${key} ${Date.now() - t0}mS`);
    })
    .then(_ => client.quit())
    .catch(e => console.error(`${timeStamp()} > Bad data was written to ${crn}`, e));
}

async function workDoc(page, {crn, otype, oid}) {
    let t0 = Date.now();

    const doc = await fetchAttachment(page, otype, oid);
    if(! doc) {
        return console.error(`${timeStamp()} > Not found DOC of ${crn}`);
    }

    const client = await redisClient(PORT, 'workDoc()');
    let docStr = await client.hgetAsync(crn, 'DOC');
    let docObj;
    try {
        docObj = JSON.parse(docStr || '{}');
    } catch(e) {
        console.error(`${timeStamp()} > Bad JSON of ${crn}`);
    }
    Object.assign(docObj, doc);

    let partialObj = {};
    partialObj['DOC'] = JSON.stringify(docObj);
    partialObj['LAST_DOC_POLL'] = timeStamp();

    client
    .hmsetAsync(crn, partialObj)
    .then(_ => {
        client.quit();
        console.log(`${timeStamp()} > Worker[${process.pid}] updated DOC of ${crn} ${Date.now() - t0}mS`)
    });
}


function summarize(crn, obj = {}) {
    const keys = [
        'zsd_commitdate',
        'zsd_category',
        'zsd_purpose',
        'zsd_stage',
        'zsd_pemanager',
        'ownerid',
        'modifiedon',
        'statecode',
        'createdby',
        'zsd_assignedte',
        'zsd_assignedtotpe',
        'zsd_assignedtpe',
        'zsd_assignedsdsste',
        'zsd_assignedspe',
        'zsd_speassignedto',
        'zsd_testtime',
        'zsd_testprogamname',
        'zsd_programimpact',
        'zsd_testtimetestflow',
        'zsd_productdescription',
        'zsd_tcrrequestname',
        'zsd_screquestname'
    ];

    let rtnObj = {crn};
    keys.forEach(k => {
        if(obj[k] && obj[k].trim()) {
            rtnObj[k] = obj[k];
        }});
    return rtnObj;
}

if(require.main === module) {
    (async () => {
        try {
            let driver = await getWebDriver({headless: false});
            let [page] = await driver.pages(); 
            //await workOn(page, { crn: 'TCR-28842.0', otype: 10061, oid: '35d94e26-74c4-e911-80ec-005056ab451f'});
            //await workOn(page, { crn: 'TCR-22623.13', otype: 10061, oid: 'adce324d-1613-e911-80e9-005056ab451f'});
            //await workDoc(page, { crn: 'TCR-25217.24', otype: 10061, oid: 'a5e1502f-91d3-e911-80ec-005056ab451f'});
            await workDoc(page, { crn: 'TCR-23890.1', otype: 10061, oid: '01a2d4ad-30a2-e911-80eb-005056ab451f'});
            //await workDoc(page, { crn: 'TCR-20421.13', otype: 10061, oid: '1824e771-9ac8-e911-80eb-005056ab4520'});
        } catch(e) {
            console.error(e);
        }
    })();
}

exports.workOn = workOn;
exports.workDoc = workDoc;

