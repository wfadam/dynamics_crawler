const puppeteer = require('puppeteer');

var singleBrowser;

async function getWebDriver(opts = {headless: true}) {
    if(! singleBrowser) {
        singleBrowser = await puppeteer.launch(opts);
    }
    return singleBrowser;
}

async function getPage(opts = {headless: true}) {
    if(! singleBrowser) {
        singleBrowser = await puppeteer.launch(opts);
    }
    return singleBrowser.newPage();
}

exports.getWebDriver = getWebDriver;
exports.getPage = getPage;

