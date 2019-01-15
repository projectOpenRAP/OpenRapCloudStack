let couchbase = require('couchbase');
let q = require('q');
let request = require('request');

let couchBaseAddress = 'localhost:8091';
let cluster = new couchbase.Cluster(`${couchBaseAddress}`);
cluster.authenticate('KyloRen', 'darthvader')
let bucket = cluster.openBucket('users');

bucket.on('error', (err) => {
    console.log(err);
    console.log("Connect error!");
});

bucket.on('connect', () => {
    console.log("Connected to couchbase!");
});

bucket.operationTimeout = 120 * 1000;

let createNewAccount = (req, res) => {
    let uName = req.body.username;
    let password = req.body.password;
    console.log(`username: ${uName} and password: ${password}`);
    bucket.get(uName, (err, result) => {
        if (err) {
            console.log(err);
            if (err.code === 13) {
                bucket.upsert(uName, {password}, (err, result) => {
                    if (err) {
                        return res.status(500).json({success: false, err});
                    } else {
                        createAccessToken(uName).then(value => {
                            return res.status(200).json({success: true, token: value.token});
                        });
                    }
                });
            } else {
                return res.status(500).json({success: false, err});
            }
        } else {
            console.log("Already there lol");
            console.log(result);
            return res.status(403).json({success: false, msg: "User already exists"});
        }
    });
}

let login = (req, res) => {
    return res.status(200).json({please: "wait"});
}

module.exports = {
    createNewAccount,
    login
}
