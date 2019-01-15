let multiparty = require('connect-multiparty');
let multipartMiddle = multiparty();
let request = require('request');

let { uploadTelemetryToElastic, uploadLocalTelemetry, uploadTelemetryToCouchbase, initialize, sayHello, authorizedRoute, createNewAccount } = require('../controllers/telemetry.controller.js');
//TODO move both objects to config

let routes = {
    TELEMETRY_UPLOAD : '/api/auth/v1/telemetry/couchbase',
    HELLO : '/api/auth/v1/hello',
    AUTHOR : '/api/auth/v1/authorized'
}

let methods = {
    TELEMETRY_UPLOAD : 'POST',
    HELLO : 'GET',
    AUTHOR : 'GET'
}
//TODO move this to middleware file
let validateIp = (req, res, next) => {
    console.log(req.ip);
    if (req.ip !== '::ffff:35.240.131.174') {
        return res.status(403).json({success: false, msg: "Please use the KONG endpoint"});
    }
    next();
}


module.exports = app => {
    app.post('/api/auth/v1/telemetry/local', uploadLocalTelemetry); //TODO: REMOVE
    app.post('/api/auth/v1/telemetry/elastic', multipartMiddle, uploadTelemetryToElastic); //TODO REMOVE
    app.post(routes.TELEMETRY_UPLOAD, validateIp, multipartMiddle, uploadTelemetryToCouchbase);
    app.post('/api/auth/v1/init/', initialize); //TODO change to a better name
    app.get(routes.HELLO, validateIp, sayHello);
    app.get(routes.AUTHOR, validateIp, authorizedRoute);//TODO remove
    app.post('/api/admin/v1/create', createNewAccount); // TODO push to creator
}
//TODO move this to a service and invoke in index.
let registerRoutes = () => {
    let routeNames = Object.keys(routes);
    routeNames.forEach(route => {
        request.post({url: 'http://localhost:8001/apis',
                      form: {
                         name: route,
                         upstream_url: 'http://35.240.131.174:8888' + routes[route],
                         uris: routes[route],
                         methods: methods[route]
                    }
        }, (err, resp, body) => {
                     if (err) {
                         console.log('error');
                         console.log(route);
                         console.log(err);
                     } else {
                         let b = JSON.parse(body);
                         if (typeof b.upstream_url === "upstream_url is not a url") {
                             console.log(route + " has invalid route");
                         }
                         else if (typeof b.created_at === "undefined") {
                             console.log(route + " already registered");
                         } else {
                             console.log("Created new route " + route);
                             request.post(`http://localhost:8001/apis/${route}/plugins?name=key-auth`, (err, res, body) => {
                                if (!(err)) {
                                    console.log(`Added route ${route} to authorized list`);
                                } else {
                                    console.log(`Couldn't add route ${route}`);
                                }
                            });
                        }
                    }
        });
    });
}

registerRoutes();
