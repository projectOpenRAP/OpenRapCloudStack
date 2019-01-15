let fs  = require('fs');
let q = require('q');
let request = require('request');
let elasticsearch = require('elasticsearch');
let couchbase = require('couchbase');
let zip = new require('node-zip')();
let unz = require('unzip');
let rimraf = require('rimraf');
//TODO remove what you dont need
let devMgmtTelemetryIndex = 'devmgmt';
let indices = {
    gok : 'gok',
    devMgmt : 'devmgmt'
}
let gokTelemetryIndex = 'gok';
let hostAddress = '0.0.0.0:9200';
let couchBaseAddress = '0.0.0.0:8091';
// http://kong:8001/consumers --data "username=uname"
let cluster = new couchbase.Cluster(`${couchBaseAddress}`); //TODO just use a variable
cluster.authenticate('aiadmin', 'telemetry@!@#$'); // TODO move to config and env variable
let bucket = cluster.openBucket('telemetry');
let bucket2 = cluster.openBucket('users');

bucket.on('error', (err) => {
    console.log(err);
    console.log("Connect Error");
});

bucket.on('connect', () => {
    console.log("Connected to couchbase!");
});

bucket2.on('error', (err) => {
    console.log(err);
    console.log("Error connecting to users bucket!");
});

bucket2.on('connect', () => {
    console.log("Connected to users bucket!");
});

bucket.operationTimeout = 120 * 1000;
bucket2.operationTimeout = 120 * 1000;

let client = new elasticsearch.Client({
    host : `${hostAddress}`, //TODO change to variable
    log : 'trace'
});

//HALP: PLS HALP
let loadJsonData = (fileName) => {
    let defer = q.defer();
    if (fileName.indexOf("_failed") > 0) {
        fileName = fileName.slice(0, -7);
    }
    fs.readFile(fileName, (err, data) => {
        if (err) {
            return defer.reject({err});
        } else {
            data = data.toString();
            return defer.resolve({data});
        }
    });
    return defer.promise;
}

let fixJsonData = (line) => {
    let defer = q.defer();
    console.log(line.edata);
    if (typeof line.edata === 'undefined' || line.edata.message === 'Number of users fetched') {
        defer.resolve({line});
    } else if (line.edata.message === 'Client requested content') {
        console.log("Fixing");
        let old_params = line.edata.params[0];
        let sizeString = old_params.results.size;
        let sizeSplit = sizeString.split(" ");
        let units = sizeSplit[1];
        let actualSize = 0.001;
        switch(units) {
            case 'KB' : actualSize = parseFloat(sizeSplit[0])/1024;
                        break;
            case 'MB' : actualSize = parseFloat(sizeSplit[0]);
                        break;
            case 'GB' : actualSize = parseFloat(sizeSplit[0])*1024;
                        break;
            default   : actualSize = (parseFloat(sizeSplit[0])/1024)/1024;
        }
        line.edata.params = {
            file_name : old_params.results.file,
            file_size : actualSize,
            timestamp : old_params.timestamp,
            uaspec : old_params.uaspec,
            path : old_params.query.path,
        }
        defer.resolve({line});
    } else {
        defer.resolve({line});
    }
    return defer.promise;
}


let cleanJsonData = (jsonData) => {
    let defer = q.defer();
    let stack = 0;
    let lines = jsonData;
    for (let i = 0; i < lines.length; i++) {
        lines[i] = JSON.stringify(lines[i]).trim();
    }
    let newString = lines.join('');
    let cleanedString = '';
    for (let i = 0; i < newString.length; i++) {
        let char = newString[i];
        if (char === '{') {
            stack++;
            cleanedString += char;
        } else if (char === '}') {
            stack--;
            cleanedString += char;
            if (stack === 0) {
                cleanedString += '\n';
            }
        } else if (char !== '\n' || char !== ' ') {
            cleanedString += char;
        }
    }
    defer.resolve({data : cleanedString});
    return defer.promise;
}

let getAllIndexCounts = () => {
    let defer = q.defer();
    let getIndexPromises = [];
    for (let key in indices){
        getIndexPromises.push(getCount(indices[key]));
    }
    q.allSettled(getIndexPromises).then(values => {
        let counts = [];
        for (let i = 0; i < values.length; i++) {
            let val = values[i];
            let ind = val.value.index;
            let cnt = val.value.count;
            counts.push({
                ind : cnt
            })
        }
        return defer.resolve({counts});
    });
    return defer.promise;
}


let getCount = (index) => {
    let defer = q.defer();
    client.indices.exists({index}, (err, exists) => {
        if (err) {
            return defer.reject({err});
        } else {
            if (!(exists)) {
                client.indices.create({index}, (err, res) => {
                    if (err) {
                        return defer.reject({err});
                    } else {
                        return defer.resolve({index, count : 0})
                    }
                });
            } else {
                client.count({index}, (err, res) => {
                    let response = res;
                    return defer.resolve({count : response.count});
                });
            }
        }
    });
    return defer.promise;
}

let fixThisData = (data) => {
    let defer = q.defer();
    let fixPromises = [];
    let returnableData = [];
    for (let i = 0; i < data.length; i++) {
        fixPromises.push(fixJsonData(data[i]));
    }
    q.allSettled(fixPromises).then(values => {
        for (let i = 0; i < values.length; i++) {
            returnableData.push(values[i].value.line);
        }
        return defer.resolve({data : returnableData});
    });
    return defer.promise;
}


let prepareBulkData = (data) => {
    let defer = q.defer();
    let newData = [];
    let returnableData = [];
    cleanJsonData(data).then(value => {
        newData = value.data.split("\n");
        return getAllIndexCounts();
    }).then(value => {
        let counts = value.counts;
        for (let i = 0; i < newData.length-1;i++) {
            let line = JSON.parse(newData[i]);
            let index_mux = 'gok';
            if (line.edata.message === 'Number of users fetched') {
                index_mux = 'devMgmt';
            }
            let requestIdentifier = {
                index : {
                    "_index" : indices[index_mux],
                    "_type" : "_doc",
                    "_id" : ++counts[indices[index_mux]]
                }
            };
            returnableData.push(requestIdentifier);
            returnableData.push(line);
        }
        return defer.resolve({data : returnableData});
    }).catch(err => {
        console.log(err);
        return defer.reject({err});
    });
    return defer.promise;
}

let prepareBulkDataForCouchBase = (data) => {
    let defer = q.defer();
    let newData = [];
    let returnableData = [];
    cleanJsonData(data).then(value => {
        newData = value.data.split("\n");
        for (let i = 0; i < newData.length -1; i++) {
            newData[i] = JSON.parse(newData[i]);
        }
        return fixThisData(newData);
    }).then(value => {
        let fixedData = value.data.slice(0, -1);
        for (let i = 0; i < fixedData.length;i++) {
            console.log(fixedData.length-i);
            let line = [];
            try{
                line = fixedData[i];
            } catch (e) {
                console.log("Error at:");
                console.log(fixedData[i]);
                throw(e);
            }
            returnableData.push(line);
        }
        return defer.resolve({data : returnableData});
    }).catch(err => {
        console.log(err);
        return defer.reject({err});
    });
    return defer.promise;
}


let createAccessKey = (name) => {
    let defer = q.defer();
    request.post({
        url: `http://0.0.0.0:8001/consumers?username=${name}`
    }, (err, resp, body) => {
        if (err) {
            defer.reject({err});
        } else {
            request.post({
                url: `http://0.0.0.0:8001/consumers/${name}/key-auth/`
            }, (err, resp, body) => {
                if (err) {
                    return defer.reject({err});
                } else {
                    let jBody2 = JSON.parse(body);
                    return defer.resolve({token: jBody2});
                }
            });
        }
    });
    return defer.promise;
}

let verifyCredentials = (username, password) => {
    let defer = q.defer();
    bucket2.get(username, (err, result) => {
        console.log(result);
        if (err && err.code === 13) {
            return defer.reject({code: 404, err: "User Does Not Exist"});
        } else if (err) {
            return defer.reject({code: 500, err});
        } else if (result.value.password !== password) {
            return defer.reject({code: 401, err: "Unauthorized"});
        } else {
            return defer.resolve();
        }
    });
    return defer.promise;
}

let createNewAccount = (req, res) => {
    let uName = req.body.username;
    let password = req.body.password;
    console.log(`Creating username: ${uName} and password: ${password}`);
    bucket2.get(uName, (err, result) => {
        if (err) {
            console.log(err);
            if (err.code === 13) {
                bucket2.upsert(uName, {password}, (err, result) => {
                    if (err) {
                        return res.status(500).json({success: false, err});
                    } else {
                        createAccessKey(uName).then(value => {
                            return res.status(200).json({success: true, token: value.token});
                        }).catch(err => {
                            return res.status(500).json({success: false, err});
                        });
                    }
                });
            } else {
                return res.status(500).json({success: false, err});
            }
        } else {
            console.log("Already there");
            console.log(result);
            return res.status(403).json({success: false, msg: "User already exists"});
        }
    });
}



let uploadToCouchBase = (data, index) => {
    let defer = q.defer();
    console.log("uploading document " + index);
    bucket.upsert(index, data, (err, res) => {
        if (err) {
            return defer.reject({err});
        } else {
            return defer.resolve({res});
        }
    });
    return defer.promise;
}

let uploadDataToCouchBase = (indexedData) => {
    let defer = q.defer();
    console.log("Uploading all data...");
    let uploadDocumentPromises = [];
    for (let i in indexedData) {
        let doc = indexedData[i];
        uploadDocumentPromises.push(uploadToCouchBase(doc, doc.mid));
    }
    q.all(uploadDocumentPromises).then(values => {
        for(v in values) {
            if (v.state === 'rejected') {
                console.log("Some have been rejected");
                return defer.reject({err : v.reason});
            }
        }
        return defer.resolve({msg: 'Successfully uploaded'});
    }).catch(err => {
        console.log("Could not upload all documents");
        console.log(err);
        return defer.reject({err});
    });
    return defer.promise;
}


let uploadDataToElasticSearch = (data) => {
    console.log("Attempting to upload");
    let defer = q.defer();
    client.bulk({
        body : data
    }, (err, res) => {
        if (err) {
            return defer.reject({err});
        } else {
            return defer.resolve();
        }
    });
    return defer.promise;
}

let unzipTelemetry = (path, name) => {
    let defer = q.defer();
    let zipper = fs.createReadStream(path).pipe(unz.Extract({ path : `./extracted_telemetry/${name}/`}));
    zipper.on('close', (err) => {
        if (err) {
            return defer.reject({err});
        } else {
            return defer.resolve();
        }
    });
    return defer.promise;
}

let deleteDirectory = (path) => {
    let defer = q.defer();
    rimraf(path, (err) => {
        if (err) {
            return defer.reject({err});
        } else {
            return defer.resolve();
        }
    });
    return defer.promise;
}

let uploadLocalTelemetry = (req, res) => {
    let fileName = req.body.fileName;
    console.log(fileName);
    let responseStructure = {
        success : false,
        msg : ''
    }
    loadJsonData(fileName).then(value => {
        console.log("Loaded JSON Data");
        return prepareBulkData(value.data);
    }).then(value => {
        console.log("Prepared Bulk Data");
        return fixThisData(value.data);
    }).then(value => {
        return uploadDataToElasticSearch(value.data);
    }).then(value => {
        responseStructure.success = true;
        return res.status(200).json(responseStructure);
    }).catch(err => {
        console.log(err);
        responseStructure.msg = err.err;
        return res.status(500).json(responseStructure);
    });
}

let uploadTelemetryToElastic = (req, res) => {
    console.log(req);
    let filePath = req.files.file.path;
    let name = req.files.file.originalFilename;
    let rawData = '';
    console.log(filePath);
    console.log(name);
    console.log("\n\n\n\n\n\n\n\n");
    let responseStructure = {
        success : false,
        msg : ''
    }
    unzipTelemetry(filePath).then(value => {
        return loadJsonData(`./extracted_telemetry/${name}/${name}`);
    }).then(value => {
        rawData = JSON.parse(value.data);
        console.log(rawData);
        return prepareBulkData(rawData);
    }).then(value => {
        return uploadDataToElasticSearch(value.data);
    }).then(value => {
        return deleteDirectory(`./extracted_telemetry/${name}`);
    }).then(value => {
        responseStructure.success = true;
        return res.status(200).json(responseStructure);
    }).catch(err => {
        responseStructure.msg = err.err;
        return res.status(200).json(responseStructure);
    });
}

let uploadTelemetryToCouchbase = (req, res) => {
    console.log(req);
    //return res.status(200).json({success: false});
    let filePath = req.files.file.path;
    let name = req.files.file.originalFilename;
    let rawData = '';
    console.log(filePath);
    console.log(name);
    console.log("\n\n\n\n\n\n\n\n");
    let responseStructure = {
        success : false,
        msg : ''
    }
    unzipTelemetry(filePath, name).then(value => {
        return loadJsonData(`./extracted_telemetry/${name}/${name}`);
    }).then(value => {
        rawData = JSON.parse(value.data);
        console.log(rawData);
        return prepareBulkDataForCouchBase(rawData);
    }).then(value => {
        console.log("Bulkified data");
        return uploadDataToCouchBase(value.data);
    }).then(value => {
        console.log("Uploaded");
        responseStructure.success = true;
        return deleteDirectory(`./extracted_telemetry/${name}`);
    }).then(value => {
	console.log("Cleaned up files");
        return res.status(200).json(responseStructure);
    }).catch(err => {
        console.log("Error!");
        console.log(err);
        responseStructure.msg = err;
        return res.status(500).json(responseStructure);
    });
}

let initialize = (req, res) => {
    let username = req.body.username;
    let password = req.body.password;
    console.log(`Username is ${username} and Password is ${password}`);
    let consumer = '';
    let key = '';
    let responseStructure = {
        success : false,
        msg : ''
    }
    verifyCredentials(username, password).then(value => {
        request.post(`http://0.0.0.0:8001/consumers?username=${username}`, (err, resp, body) => {
            if (!(err)) {
                let response = JSON.parse(body);
                let id = response.id;
                console.log("Got ID");
                responseStructure.success = true;
                responseStructure.id = id;
                request.post(
                    {
                         url: `http://0.0.0.0:8001/consumers/${username}/key-auth`,
                    }, (err, resp, body) => {
                        if (!(err)) {
                            let b = JSON.parse(body);
                            responseStructure.success = true;
                            responseStructure.key = b.key;
                            responseStructure.consumer_id = b.consumer_id;
                            console.log("Got key");
                            return res.status(200).json(responseStructure);
                        } else {
                            responseStructure.msg = err;
                            return res.status(200).json(responseStructure);
                        }
                    });
                    // return res.status(200).json(responseStructure);
            } else {
                responseStructure.msg = err;
                return res.status(200).json(responseStructure);
            }
        });
    }).catch(err => {
        responseStructure.err = err.err;
        return res.status(err.code).json(responseStructure);
    });
        /*
    generateConsumer({username, password}).then(value => {
        consumer = value.consumer;
        responseStructure.consumer = consumer;
        return generateKey({consumer});
    }).then(value => {
        key = value.key;
        responseStructure.key = key;
        responseStructure.success = true;
        return res.status(200).json(responseStructure);
    }).catch(e => {
        console.log(e.err);
        responseStructure.msg = e.err;
        return res.status(500).json(responseStructure);
    });
    */
}

let sayHello = (req, res) => {
    return res.status('200').json({hello : 'world'});
}

let authorizedRoute = (req, res) => {
    console.log("Authorized route called!");
    return res.status(200).json({route : 'authorized'});
}

module.exports = {
    uploadLocalTelemetry,
    uploadTelemetryToElastic,
    uploadTelemetryToCouchbase,
    initialize,
    sayHello,
    authorizedRoute,
    createNewAccount
}
