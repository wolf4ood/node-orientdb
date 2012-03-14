var http = require("http");
var sys = require('sys');
var events = require('events');
var querystring = require('querystring');

var ODatabase = module.exports = function(databasePath, databasePort) {

	this.databasePath = databasePath;
	this.databasePort = databasePort;
	this.client = http.createClient(databasePort, databasePath);
	this.databaseInfo = null;
	this.encodedDatabaseName = "";
	this.commandResult = null;
	this.commandResponse = null;
	this.errorMessage = null;
	this.evalResponse = true;
	this.parseResponseLink = true;
	this.removeObjectCircleReferences = true;
	this.urlPrefix = "";
	this.auth = "";
	this.username = "admin";
	this.passwd = "admin";

	if (databasePath) {
		var pos = databasePath.indexOf('orientdb_proxy', 8); // JUMP HTTP
		if (pos > -1) {
			pos = databasePath.indexOf('/', pos); // END OF PROXY
		} else {
			pos = databasePath.indexOf('/', 8);
		}

		this.databaseUrl = databasePath.substring(0, pos + 1);
		this.databaseName = databasePath.substring(pos + 1);
		if (this.databaseName.indexOf('/') > -1) {
			this.encodedDatabaseName = "";
			var parts = this.databaseName.split('/');
			for (p in parts) {
				if (this.encodedDatabaseName.length > 0)
					this.encodedDatabaseName += '$';
				this.encodedDatabaseName += parts[p];
			}
		} else
			this.encodedDatabaseName = this.databaseName;
	}

	events.EventEmitter.call(this);
};
sys.inherits(ODatabase, events.EventEmitter);

ODatabase.prototype.open = function(username, passwd, db, callback) {
	var self = this;
	this.db = db;
	var auth = 'Basic '
			+ new Buffer(this.username + ':' + this.passwd).toString('base64');
	var header = {
		'Host' : this.databasePath,
		'Authorization' : auth
	};
	var request = this.client.request('GET', '/connect/' + this.db, header);

	request.on('response', function(response) {
		var body = '';
		response.on('data', function(chunk) {
			body += chunk;
		});
		response.on('end', function() {
			self.databaseInfo = body;
			self.emit("open", body);
		});
	});
	request.end();
};

ODatabase.prototype.close = function() {

};

ODatabase.prototype.query = function(iQuery, iLimit, iFetchPlan, callback) {

	var self = this;
	var rquery = function(iQuery, iLimit, iFetchPlan) {
		if (iLimit == null || iLimit == '') {
			iLimit = '';
		} else {
			iLimit = '/' + iLimit;
		}
		if (iFetchPlan == null || iFetchPlan == '') {
			iFetchPlan = '';
		} else {
			if (iLimit == '') {
				iLimit = '/20';
			}
			iFetchPlan = '/' + iFetchPlan;
		}
		var auth = 'Basic '
				+ new Buffer(self.username + ':' + self.passwd)
						.toString('base64');
		var header = {
			'Host' : self.databasePath,
			'Authorization' : auth
		};
		var url = '/query/' + self.db + '/sql/' + iQuery + iLimit + iFetchPlan;
		var request = self.client.request('GET', escape(url), header);
		request.on('response', function(response) {
			var body = '';
			response.on('data', function(chunk) {
				body += chunk;
			});
			response.on('end', function() {
				self.databaseInfo = body;
				self.emit("query", body);
			});
		});
		request.end();
	};
	if (this.databaseInfo == null) {
		this.on("open", function() {
			rquery(iQuery, iLimit, iFetchPlan);
		});
	} else {
		rquery(iQuery, iLimit, iFetchPlan);
	}
};

ODatabase.prototype.load = function(iRID, iFetchPlan, callback) {

	var self = this;
	var rload = function() {
		if (iFetchPlan != null && iFetchPlan != '') {
			iFetchPlan = '/' + iFetchPlan;
		} else {
			iFetchPlan = '';
		}
		if (iRID && iRID.charAt(0) == '#')
			iRID = iRID.substring(1);
		var auth = 'Basic '
				+ new Buffer(self.username + ':' + self.passwd)
						.toString('base64');
		var header = {
			'Host' : self.databasePath,
			'Authorization' : auth
		};
		var url = '/document/' + self.db + "/" + iRID + iFetchPlan;
		var request = self.client.request('GET', escape(url), header);
		request.on('response', function(response) {
			var body = '';
			response.on('data', function(chunk) {
				body += chunk;
			});
			response.on('end', function() {
				self.emit("load", body);
			});
		});
		request.end();
	};
	if (this.databaseInfo == null) {
		this.on("open", function() {
			rload(iRID, iFetchPlan);
		});
	} else {
		rload(iRID, iFetchPlan);
	}
};

ODatabase.prototype.save = function(obj, callback) {

	var self = this;
	var rsave = function(obj) {
		var rid = obj['@rid'];
		var methodType = rid == null || rid == '-1:-1' ? 'POST' : 'PUT';

		if (self.removeObjectCircleReferences && typeof ojb == 'object') {
			self.removeCircleReferences(obj, {});
		}
		var data = JSON.stringify(obj);
		var auth = 'Basic '
				+ new Buffer(self.username + ':' + self.passwd)
						.toString('base64');
		var header = {
			'Host' : self.databasePath,
			'Authorization' : auth,
			'Content-Type' : 'application/json',
			'Content-Length' : data.length
		};
		var url = '/document/' + self.db + "/" + rid;
		var request = self.client.request(methodType, escape(url), header);
		request.on('response', function(response) {
			var body = '';
			response.on('data', function(chunk) {
				body += chunk;
			});
			response.on('end', function() {
				self.emit("save", body);
			});
		});
		request.write(data);
		request.end();
	};
	if (this.databaseInfo == null) {
		this.on("open", function() {
			rsave(obj);
		});
	} else {
		rsave(obj);
	}

}

;
ODatabase.prototype.remove = function(obj, callback) {

	var self = this;
	var rremove = function(obj) {
		var rid;
		if (typeof obj == "string")
			rid = obj;
		else
			rid = obj['@rid'];
		var auth = 'Basic '
				+ new Buffer(self.username + ':' + self.passwd)
						.toString('base64');
		var header = {
			'Host' : self.databasePath,
			'Authorization' : auth
		};
		var url = '/document/' + self.db + "/" + rid;
		var request = self.client.request('DELETE', escape(url), header);
		request.on('response', function(response) {
			var body = '';
			response.on('data', function(chunk) {
				body += chunk;
			});
			response.on('end', function() {
				self.emit("remove", body);
			});
		});
		request.end();
	};
	if (this.databaseInfo == null) {
		this.on("open", function() {
			rremove(obj);
		});
	} else {
		rremove(obj);
	}
};
ODatabase.prototype.executeCommand = function(iCommand) {

};

ODatabase.prototype.getDatabaseInfo = function() {
	return this.databaseInfo;
};
ODatabase.prototype.setDatabaseInfo = function(iDatabaseInfo) {
	this.databaseInfo = iDatabaseInfo;
};

ODatabase.prototype.getCommandResult = function() {
	return this.commandResult;
};
ODatabase.prototype.setCommandResult = function(iCommandResult) {
	this.commandResult = iCommandResult;
};

ODatabase.prototype.getCommandResponse = function() {
	return this.commandResponse;
};
ODatabase.prototype.setCommandResponse = function(iCommandResponse) {
	this.commandResponse = iCommandResponse;
};

ODatabase.prototype.getErrorMessage = function() {
	return this.errorMessage;
};
ODatabase.prototype.setErrorMessage = function(iErrorMessage) {
	this.errorMessage = iErrorMessage;
};

ODatabase.prototype.getDatabaseUrl = function() {
	return databaseUrl;
};
ODatabase.prototype.setDatabaseUrl = function(iDatabaseUrl) {
	this.databaseUrl = iDatabaseUrl;
};

ODatabase.prototype.getDatabaseName = function() {
	return this.databaseName;
};
ODatabase.prototype.setDatabaseName = function(iDatabaseName) {
	this.databaseName = iDatabaseName;
};

ODatabase.prototype.getEvalResponse = function() {
	return this.evalResponse;
};
ODatabase.prototype.setEvalResponse = function(iEvalResponse) {
	this.evalResponse = iEvalResponse;
};

ODatabase.prototype.getParseResponseLinks = function() {
	return this.parseResponseLink;
};
ODatabase.prototype.setParseResponseLinks = function(iParseResponseLinks) {
	this.parseResponseLink = iParseResponseLinks;
};

ODatabase.prototype.getRemoveObjectCircleReferences = function() {
	return this.removeObjectCircleReferences;
};
ODatabase.prototype.setRemoveObjectCircleReferences = function(
		iRemoveObjectCircleReferences) {
	this.removeObjectCircleReferences = iRemoveObjectCircleReferences;
};

ODatabase.prototype.removeCircleReferences = function(obj, linkMap) {
	for (field in obj) {
		var value = obj[field];
		if (typeof value == 'object') {
			if (linkMap[value] != null && value['@rid'] != null) {
				if (value['@rid'].indexOf('#', 0) > -1) {
					obj[field] = value['@rid'];
				} else {
					obj[field] = '#' + value['@rid'];
				}
			} else {
				linkMap[value] = 'foo';
				this.removeCircleReferences(value, linkMap);
			}
		} else if ($.isArray(value)) {
			for (i in value) {
				var arrayValue = value[i];
				if (typeof arrayValue == 'object') {
					if (linkMap[value] != null && value['@rid'] != null) {
						if (value['@rid'].indexOf('#', 0) > -1) {
							obj[field] = value['@rid'];
						} else {
							obj[field] = '#' + value['@rid'];
						}
					} else {
						linkMap[value] = 'foo';
						this.removeCircleReferences(value, linkMap);
					}
				}
			}
		}
	}
};
