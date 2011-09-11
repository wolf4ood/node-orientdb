var OrientDB = require("../lib/orientdb.js");
var database = new OrientDB("localhost",2480);

database.on("open",function(chunk){
	database.load('#5:3');
	//database.remove('#5:3');
});
database.on("remove",function(chunk){
	console.log(chunk);
});
database.on("load",function(chunk){
	console.log(chunk);
});

database.open("admin","admin");

