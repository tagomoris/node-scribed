var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    scribe = require('gen-nodejs/scribe'),
    ttypes = require('gen-nodejs/scribe_types'),
    mongodb = require('mongodb');

var datetimeParser = function(str) {
  // [ '16/Jul/2011', '19:10:37', '+0900' ]
  var matched = /^(\d\d)\/([a-zA-Z]+)\/(\d\d\d\d):(\d\d:\d\d:\d\d)/.exec(str);
  // 'December 17, 1995 03:24:00'
  return new Date(matched[2] + ' ' + matched[1] + ', ' + matched[3] + ' ' + matched[4]);
};

var requestParser = function(str) {
  // GET /SVqrOBfFNUz/index HTTP/1.1
  var parts = str.split(' ');
  return {method: parts[0], path: parts.slice(1,parts.length - 1).join(' ')};
};

var client = new mongodb.Db('logs', new mongodb.Server('127.0.0.1', 27017, {}), {native_parser:true});

var lastsecond = null;
var counter = 0;
var putsMillisecs = 0;
var transportLogs = function() {
  var re = /^([^\s]*) [^\s]* ([^\s]*) \[([^\]]*)\] "([^"]*)" ([^\s]*) ([^\s]*) "([^"]*)" "([^"]*)"(.*)$/gm;
  return function(messages, success) {
    var logLines = 0;
    var now = new Date();
    //         ([^\s]*) [^\s]* ([^\s]*) \[([^]]*)\] "([^"]*)" ([^\s]*) ([^\s]*) "([^"]*)" "([^"]*)" (.*)
    //combined: rhost, logname, user,   datetime,   request, status,   bytes,   refer,     agent
    //log object:
    // {rhost, user, datetime(Date), method, path, status(Number), bytes(Number), refer, agent, additional}
    var logs = [];
    for(var i = 0, len = messages.length; i < len; i++) {
      var match;
      while((match = re.exec(messages[i].message)) != null) {
        var req = requestParser(match[4]);
        logs.push({
          rhost: match[1],
          user: match[2],
          datetime: datetimeParser(match[3]),
          method: req.method,
          path: req.path,
          status: Number(match[5]),
          bytes: Number(match[6]),
          refer: match[7],
          agent: match[8],
          additional: match[9].trim().split(' ')
        });
        logLines += 1;
      }
    }
    if (logs.length < 1) {
      success(ttypes.ResultCode['OK']);
      return;
    }

    var collection = new mongodb.Collection(client, 'log' + now.getDate());
    var starts = (new Date()).getTime();
    collection.insert(logs, {safe:true}, function(err, objects) {
      var ends = (new Date()).getTime();
      if (err) {
        console.warn('Mongo insert error: ' + err.message);
        success(ttypes.ResultCode['OK']);
        return;
      }
      var timelabel = now.toLocaleTimeString();
      if (! lastsecond) {
        lastsecond = timelabel;
        counter = logLines;
        putsMillisecs = ends - starts;
      }
      else if (lastsecond == timelabel) {
        counter += logLines;
        putsMillisecs += ends - starts;
      }
      else {
        if (counter > 0)
          console.log(lastsecond + '\tinsert ' + counter + 'records, ' + (putsMillisecs / counter).toPrecision(4) + ' msecs/log');
        lastsecond = timelabel;
        counter = logLines;
        putsMillisecs = ends - starts;
      }
      success(ttypes.ResultCode['OK']);
    });
  };
};

var server = thrift.createServer(scribe, {
  Log: transportLogs()
}, {transport: ttransport.TFramedTransport});

var disconnect = function() {
  client.close();
  server.close();
  process.exit(0);
};
process.addListener('SIGTERM', disconnect);

client.open(function(err, client){
  server.listen(1463, function(){});
});
