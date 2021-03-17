const { MongoClient, GridFSBucket } = require( 'mongodb' );

var _client;
var _db;
var _bucket;

module.exports = {

  getClient: function(globalOptions) {
        var options = globalOptions || {};
        
        _.defaults(options, {
            uri: 'mongodb://localhost:27017/mydatabase'
        });
        _.defaults(options, {
            mongoOptions: {}
        });
        
        options.mongoOptions = Object.assign({ useNewUrlParser: true, useUnifiedTopology: true, poolSize: 200 }, options.mongoOptions);

        return new Promise(function(resolve,reject){
            if(_client) resolve(_client);
            MongoClient.connect (options.uri, options.mongoOptions).then((client) => {
                _client = client;
                _db  = client.db();
                _bucket = new GridFSBucket(_db);
                resolve(client);
            }).catch((err) => {
                reject(err);
            })
        })
    },

    getDb: function() {
        return _db;
    },

    getBucket: function() {
        return _bucket;
    }
};