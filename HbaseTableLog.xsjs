
//import a library for doing PUT & GET to HBase Stargate
$.import("HanaHbase","Hbase"); 
var Hbase = $.HanaHbase.Hbase;

var HbaseTable = 'HanaTableLog';
var rowKey = $.request.parameters.get("key") || "TEST:DOC2:1"; 
var versions = $.request.parameters.get("versions") || "1"; 

var HbaseResponse;

switch ($.request.method) {
	case $.net.http.GET:
		HbaseResponse = Hbase.Get(HbaseTable, rowKey,versions);
		break;
	case $.net.http.PUT:	
		if (  $.request.headers.get("Content-Type") === 'application/json' ) {
			var reqBody = JSON.parse( $.request.body.asString() ); 
			HbaseResponse = Hbase.Put(HbaseTable, rowKey,reqBody);
		}
		else {
			HbaseResponse =	{"status": $.request.method  + " 'Content-Type' must be 'application/json' "};
		}
		break;
	default:
		HbaseResponse = {"status": "Method "  + $.request.method  + " not Defined"};

}  
 
// send the response as JSON
$.response.contentType = "application/json";
$.response.setBody(JSON.stringify(HbaseResponse));