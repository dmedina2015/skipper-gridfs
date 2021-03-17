const { MongoClient, GridFSBucket } = require( 'mongodb' );

var _client;
var _db;
var _bucket;

var calls = 0;
var interval;

checkIdle = function(){
    // If no calls made to connection pool in the last interval,
    // drop connection and stop checking idle.
    if (calls){
        calls = 0;
    }
    else{
        
        if(_client) _client.close();
        _client = undefined;
        _db = undefined;
        _bucket = undefined;
        clearInterval(interval);
    }
}

module.exports = {

  getClient: function(globalOptions) {
        calls++; //Increase counter
        var options = globalOptions || {};
        var mongoOptions = options.mongoOptions || {};
        
        // MongoDB URI
        _.defaults(options, {
            uri: 'mongodb://localhost:27017/mydatabase'
        });

        // Idle time to drop MongoClient
        _.defaults(options, {
            maxIdleConnTime: 10000 // in ms, default 10s
        });

        // Mongo Options
        _.defaults(mongoOptions, {
            useUnifiedTopology: true,
            poolSize: 10
        });

        

        return new Promise(function(resolve,reject){
            if(_client) return resolve(_client);
            MongoClient.connect (options.uri, mongoOptions).then((client) => {
                _client = client;
                _db  = client.db();
                _bucket = new GridFSBucket(_db);

                resolve(client);
            }).catch((err) => {
                reject(err);
            })
            interval = setInterval(checkIdle,options.maxIdleConnTime); //Check idle every 10 secs.
        })
    },

    getDb: function() {
        return _db;
    },

    getBucket: function() {
        return _bucket;
    }
};