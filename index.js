'use strict'
const Transform = require('stream').Transform
const PassThrough = require('stream').PassThrough
const fs = require('fs');

const src = require('fs').createReadStream('./germany-cities.csv');
const googleMapsClient = require('@google/maps').createClient({
  Promise,
  key: '',
});

const transformData = (data) => {
  const identifier = data.name.replace(' ', '-').toLowerCase();
  const { type: { category } } = data;
  const { name } = data;
  const plzRegex = /\d{4,5}/;

  const [street = '', [plz = 0, city = ''] = [], country = ''] = data
    .formatted_address
    .split(',')
    .map(el => el.trim())
    .map(str => str.match(plzRegex) ? str.split(' ') : str);

  const { lat, lng } = data.geometry.location;

  return {
    identifier,
    category,
    name,
    address: {
      street,
      city,
      country,
      plz,
      raw_address: data.formatted_address,
      location: {
        lat,
        lng,
      },
    },
  };
};

const sleep = (delay) => new Promise(resolve => setTimeout(resolve, delay));

const performGoogleAPIRequest = response => async (options) => {
  const { results, next_page_token } = await sleep(2000)
    .then(() => googleMapsClient
      .places(options)
      .asPromise())
    .then(r => ({
      result: r.json.results.map(el => transformData(el)),
      next_page_token: r.json.next_page_token
    }))
    .catch(e => console.error(e));

  const currentResult = [...response, ...results];

  return next_page_token
    ? performGoogleAPIRequest(currentResult)({ pagetoken: next_page_token })
    : currentResult;
}


const search = async (city) => {
  console.log(`Searching for ${city}`);

  // NEED TO CHANGE TWO VARIABLES BELLOW
  const query = `search here near the ${city}`;
  const type = 'hotel';
  const language = 'en';
  const radius = 1000;

  let result = [];

  return performGoogleAPIRequest([])({
    query,
    language,
    radius,
    type,
  });
};

const parseAndDelay = new Transform({
  objectMode: true,
  transform: async (chunk, encoding, callback) => {
    if(chunk === '') return callback()
    try {
      const result = await search(chunk);
      callback(null, result.map(r => JSON.stringify(r)).join(',') + ',');
    } catch(e) {
      callback(`Can't parse: ${chunk}. ${e}`)
    }
  }
})

src
  .pipe(require('split')())
  .pipe(parseAndDelay)
  .pipe(fs.createWriteStream('./result.json'))
  .on('error', console .error)
  .on('finish', () => console.log('End of stream'))
