const fs = require('fs');
const through2 = require('through2');
const MyStream = require('json2csv-stream');
const parser = new MyStream({
  del: ';'
});
// const parser = new Parser();

let buf = '';
let i = 0;

const flatten = (data) => {
  if (!data) return data;

  const plzRegex = /\d{4,5}/;
  const cityPlz = /\d{4,5}(.*)(?=\(?)/;
  const streePlz = /[a-zß|ü|ö|ä|ß|é|\s]+\.?\d{0,3}-?\d{1,3}[A-Z]?$/;

  const { address: { raw_address = '' } } = data;
  const countries = [
    'Germany', 'Denmark', 'France', 'Poland', 'Switzerland',
    'Netherlands', 'Belgium', 'Czechia', 'Austria',
  ];

  const { identifier, category, name, types, address : { location: { lat = 0, lng = 0 } } } = data;

  const address = { street: '', city: '', country: '', plz: 0 };

  raw_address.split(',').forEach(token => {
    let match, plz, city, street;

    token = token.trim();

    if (match = token.match(plzRegex)) {
      address.plz = match[0];
      if (token.match(cityPlz)) {
        address.city = token.match(cityPlz)[1].trim();
        
      }
      return;
    }

    // street + building number
    if (streePlz.test(token)) {
      address.street = token;
      return;
    }

    // country
    if (countries.indexOf(token) !== -1) {
      address.country = countries[countries.indexOf(token)];
      return;
    }
  });

  const { street, city, country, plz } = address;

  return {
    identifier, category, name, types, street, city, country, plz, raw_address, lat, lng
  }
};

// read the output file from index.js
fs.createReadStream('./tmp/result.json')
  .pipe(through2.obj(function(chunk, enc, cb) {
    let string = (buf + chunk.toString()).replace(/\}\}\}(\,){2,}\{/g, '}}},{');
    buf = '';
    const regex = /\}\}\}(\,)\{/;
    let m, lastBracket, jsObject;
    const start = 0;

    while((m = regex.exec(string)) !== null) {
      i++;
  
      console.log(i);
      let length = m.index + 3;
      let stringObj = string.substr(start, length)

      try {
        jsObject = JSON.parse(stringObj);
      } catch (e) {
        console.error(e);
        console.log(string);
        process.exit();
      }

      this.push(JSON.stringify(flatten(jsObject)));
      string = string.substr(length+1);
    }

    if (string) {
      buf += string;
    }

    cb();
  }))
  .pipe(parser)
  // .pipe(process.stdout);
  .pipe(fs.createWriteStream('out2.csv'));
