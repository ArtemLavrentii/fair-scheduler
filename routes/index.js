var express = require('express');
var router = express.Router();
const Scheduler = require('../scheduler.js');

const scheduler = new Scheduler(4);

/* GET home page. */
router.get('/queue', function(req, res, next) {
  scheduler.execute(req.query);
  res.json({ ok: true });
});

module.exports = router;
