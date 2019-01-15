const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express();


app.use(cors());
app.use(bodyParser.urlencoded({ extended : true }));
app.use(bodyParser.json());


require('./routes/telemetry.routes.js')(app);


let port = 8888;
app.listen(port, err => {
    console.log(err || ("Listening on port " + port));
})
