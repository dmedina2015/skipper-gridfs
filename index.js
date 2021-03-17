const Transform = require('stream').Transform;
const path = require('path');
const mime = require('mime');
const concat = require('concat-stream');
const mongoUtil = require('./mongoUtil');

module.exports = function SkipperGridFS(globalOptions) {
    const options = globalOptions || {};

    const adapter = {};
    adapter.rm = (fd, cb) => {
        const errorHandler = (err) => {
            if (cb) cb(err);
        }

        mongoUtil.getClient(options).then(() =>{
            mongoUtil.getBucket().delete(fd, (err) => { if (err) errorHandler(err) });
            if (cb) cb();
        })
        // Catch error from mongoUtil promise
        .catch((err) => {
            if (err) errorHandler(err);
        });
    }

    adapter.ls = (dirpath, cb) => {
        const errorHandler = (err) => {
            if (cb) cb(err);
        }

        mongoUtil.getClient(options).then(() =>{
            const results = mongoUtil.getBucket().find({ 'metadata.dirname' : dirpath })
            results.toArray().then((listing) => {
                var filteredListing = listing.map(o => o._id);
                if (cb) cb(null, filteredListing);
            }).catch((err) => {
                if (err) errorHandler(err);
            })
        })
        // Catch error from mongoUtil promise
        .catch((err) => {
            if (err) errorHandler(err);
        });
    }

    adapter.read = (fd, cb) => {
        const errorHandler = (err) => {
            if (cb) cb(err);
        }
        const __transform__ = Transform();

        __transform__._transform = (chunk, encoding, callback) => {
            return callback(null, chunk);
        };

        __transform__.once('error', (err) => {
            errorHandler (err);
        });
        
        mongoUtil.getClient(options).then(() =>{
            const download__ = mongoUtil.getBucket().openDownloadStream(fd);
            
            download__.once('error', (err) => {
              __transform__.emit('error', err);
            });

            download__.pipe(__transform__);

        })
        // Catch error from mongoUtil promise
        .catch((err) => {
            if (err) errorHandler(err);
        });

        if (cb) {
            __transform__.pipe(concat((data) => {
                return cb(null, data);
            }));
        } else {
            return __transform__;
        }
    }

    adapter.receive = (fd, cb) => {
        const receiver__ = require('stream').Writable({ objectMode: true });

        receiver__._write = function onFile(__newFile, _unused, done) {
            
            mongoUtil.getClient(options).then(() => {
               
                var outs__ = mongoUtil.getBucket().openUploadStreamWithId(__newFile.fd, __newFile.filename, {
                    metadata: {
                        filename: __newFile.filename,
                        fd: __newFile.fd,
                        dirname: __newFile.dirname || path.dirname(__newFile.fd)
                    },
                    contentType: mime.getType(__newFile.fd)
                })
                
                __newFile.pipe(outs__);

                outs__.once('finish', function () {
                    receiver__.emit('writefile', __newFile);
                    done();
                });

                // Handle potential error from the outgoing stream
                outs__.on('error', function (err) {

                    return done({
                        incoming: __newFile,
                        outgoing: outs__,
                        code: 'E_WRITE',
                        stack: typeof err === 'object' ? err.stack : new Error(err),
                        name: typeof err === 'object' ? err.name : err,
                        message: typeof err === 'object' ? err.message : err
                    });
                  
                });
            })
            // Catch error from mongoUtil promise
            .catch((err) => {
                if (err) outs_.emit('error', err);
            });
            
        };
        return receiver__;
    }
    return adapter;
};

