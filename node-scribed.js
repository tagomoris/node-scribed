var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    scribe = require('gen-nodejs/scribe'),
    mongodb = require('mongodb');

/*
        ($pairs->{date}, $pairs->{time}, $pairs->{timezone}) = ($pairs->{datetime} =~ m!^([^: ]+):([^ ]+)\s([-+0-9]+)$!);   
        if ($pairs->{request} =~ m!^(.*) (HTTP/\d\.\d)$!) {
            $pairs->{proto} = $2;
            $req = $1;
        }
        else {
            $pairs->{proto} = undef;
            $req = $pairs->{request};
        }
        ($pairs->{method}, $pairs->{path}) = split(/\s+/, $req, 2);
        next unless reduce { $a and defined($pairs->{$b}) } 1, qw( path time date );

        next if defined($rule->[2]) and not $rule->[2]->($pairs);
    
 */
var datetimeParser = function(str) {
    var dtre = /^([^: ]+):([^ ]+)\s([-+0-9]+)$/;
};

var requestParser = function(str) {
};

var client = new mongodb.Db('logs', new mongodb.Server('127.0.0.1', 27017, {}), {native_parser:true});
var server = thrift.createServer(scribe, {
  Log: function(messages, success) {
    /* TODO: send messages to mongodb client */
    //         ([^\s]*) [^\s]* [^\s]* \[([^]]*)\] "([^"]*)" ([^\s]*) [^\s]* "([^"]*)" "([^"]*)"
    //combined: rhost, logname, user, datetime,   request, status,   bytes, refer,     agent
    //log object:
    // {rhost, user, datetime(Date), method, path, status(Number), bytes(Number), refer, agent}

    var re = /^([^\s]*) [^\s]* [^\s]* \[([^\]]*)\] "([^"]*)" ([^\s]*) [^\s]* "([^"]*)" "([^"]*)"/;
    var logs = [];
    for(var i = 0, len = messages.length; i < len; i++) {
      var match = re.exec(messages[i]);
      if (! match) { continue; }
      
    }
  }
}, {transport: ttransport.TBufferedTransport});

var disconnect = function() {
  client.close();
  server.close();
  process.exit(0);
};
process.addListener('SIGTERM', disconnect);

client.open(function(err, client){
  server.listen(1463, function(){});
});
