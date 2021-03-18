========================================

## DISCLAIMER

This is a fork package from skipper-gridfs. Below are the diferences from [original project](https://www.npmjs.com/package/skipper-gridfs):

- Bug fix in function `adapter.rm()` that causes callback to be called twice.
  Detais about it you find [here](https://github.com/willhuang85/skipper-gridfs/pull/44).

- Added support to `maxBytes` option, using similar logic from `skipper-disk`. Behavior:
  * An error is thrown when upload stream exceeds bytes defined in `maxBytes` parameter.
  * Limit is applied for all upload stream (all files in the same request, not for each file individually)
  * Files uploaded before limit is reached are saved in GridFS. Only the one that exceeds and the subsequent are not saved.
  * Garbage of unfinished upload is removed using `GridFSBucketWriteStream.abort()` function

- Added support to `onProgress` using same logic from `skipper-disk`

- Added support to Node >= 14. 

- CI using official [skipper-adapter-test](https://github.com/balderdashy/skipper-adapter-tests)

All the credits about the original package belongs to @willhuang85 and the staff. Great job guys!

========================================


# [<img title="skipper-gridfs - GridFS filesystem adapter for Skipper" src="http://i.imgur.com/P6gptnI.png" width="200px" alt="skipper emblem - face of a ship's captain"/>](https://github.com/willhuang85/skipper-gridfs) GridFS Filesystem Adapter

[![npm version](https://badge.fury.io/js/%40dmedina2015%2Fskipper-gridfs.svg)](https://badge.fury.io/js/%40dmedina2015%2Fskipper-gridfs) &nbsp; 
[![Build Status](https://travis-ci.org/dmedina2015/skipper-gridfs.svg?branch=master)](https://travis-ci.org/dmedina2015/skipper-gridfs)
&nbsp;


GridFS adapter for receiving [upstreams](https://github.com/balderdashy/skipper#what-are-upstreams). Particularly useful for handling streaming multipart file uploads from the [Skipper](https://github.com/balderdashy/skipper) body parser.

Currently only supports Node 6 and up. Node 15 included!


========================================

## Installation

```
$ npm install @dmedina2015/skipper-gridfs --save
```

Also make sure you have skipper [installed as your body parser](http://beta.sailsjs.org/#/documentation/concepts/Middleware?q=adding-or-overriding-http-middleware).

> Skipper is installed by default in [Sails](https://github.com/balderdashy/sails) v1.4.2.

========================================


## Usage

```javascript
req.file('avatar')
.upload({
  adapter: require('@dmedina2015/skipper-gridfs'),
  uri: 'mongodb://username:password@myhost.com:27017/myDatabase'
}, function whenDone(err, uploadedFiles) {
  if (err) return res.negotiate(err);
  else return res.ok({
    files: uploadedFiles,
    textParams: req.params.all()
  });
});
```

For more detailed usage information and a full list of available options, see the Skipper docs, especially the section on "[Uploading to GridFS](https://github.com/balderdashy/skipper#uploading-files-to-gridfs)".


| Option          | Type       | Details                                                                                                                                                                                                 |
| --------------- | :--------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`           | ((string)) | URI to connect to Mongo instance, e.g. `mongodb://username:password@localhost:27107/databasename`.<br/> (Check [mongo client URI syntax](https://docs.mongodb.com/manual/reference/connection-string)). |
| `bucketOptions` | ((object)) | An optional parameter that matches the GridFSBucket options (Check [mongo gridfs bucket options](http://mongodb.github.io/node-mongodb-native/3.1/api/GridFSBucket.html)).                              |
| `mongoOptions`  | ((object)) | An optional paramter that matches the MongoClient.connect options (Check [mongo client options](http://mongodb.github.io/node-mongodb-native/3.1/api/MongoClient.html#.connect)).                       |
| `maxBytes`      | ((integer))| Optional. Max total number of bytes permitted for a given upload, calculated by summing the size of all files in the upstream; e.g. if you created an upstream that watches the "avatar" field (`req.file('avatar')`), and a given request sends 15 file fields with the name "avatar", `maxBytes` will check the total number of bytes in all of the 15 files.  If maxBytes is exceeded, the already-written files will be left untouched, but unfinshed file uploads will be garbage-collected, and not-yet-started uploads will be cancelled.  (Note that `maxBytes` is currently experimental) |
| `onProgress`    | ((function)) | Optional. This function will be called again and again as the upstream pumps chunks into the receiver with a dictionary (plain JavaScript object) representing the current status of the upload, until the upload completes.|
========================================

## Contributions

are welcomed :ok_hand:

To run the tests:

```shell
$ URI=mongodb://username:password@localhost:27107/databasename npm test
```


========================================

## License

MIT
