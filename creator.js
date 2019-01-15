const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express();


app.use(cors());
app.use(bodyParser.urlencoded({ extended : true }));
app.use(bodyParser.json());


require('./routes/creator.routes.js')(app);


let port = 8889;
app.listen(port, err => {
    console.log(err || ("Listening on port " + port));
})
