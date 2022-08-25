var fs = require('fs-extra');

require('./index').testrun({
    dbhost: 'localhost',
    dbport: 3306,
    dbname: 'wp',
    dbuser: 'root',
    dbpass: 'password',

    tablePrefix: 'wp_'
}, function(err, results) {
    fs.writeFileSync('./tmp.json', JSON.stringify(results, undefined, 2));
});