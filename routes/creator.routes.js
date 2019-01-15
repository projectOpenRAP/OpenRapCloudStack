let { createNewAccount, login } = require('../controllers/creator.controller.js');


module.exports = app => {
    app.post('/api/admin/v1/create', createNewAccount);
    app.post('/api/admin/v1/login', login);
}
