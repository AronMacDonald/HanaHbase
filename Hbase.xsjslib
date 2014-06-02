//import a library for decoding base64 string
$.import("HanaHbase","base64");  //aa
var base64 = $.HanaHbase.base64;
 
//create client
var client = new $.net.http.Client();
 
// Use HBASE destination defined in Hbase.xshttpdest
var dest = $.net.http.readDestination("HanaHbase", "Hbase");

//Next need to build up the url string expected by HBASE Stargate REST service
//to return a single tweet records from the tweet table, by the key
// e.g. /HanaTableLog/TEST:DOC2:1/
var hBaseUrl;
 
var request;  
var response;

//get all the cookies and headers from the response
var co = [], he = [];
//get the body of the Response from Hbase
var body = undefined;


function Get (HbaseTable, rowKey, versions) {
	//Currently hard coding the name of the Hbase table 'tweets'
	//input is 'key'  of the HBASE table to return a single row
	// e.g. hBaseUrl = /HanaTableLog/TEST:DOC2:1/
	hBaseUrl = '/' + HbaseTable + '/' + rowKey + '/' ;

	//Get HBASE Change Versions (max limit set on Hbase table per column family)
	hBaseUrl += "?v=" + versions;
	
	request = new $.net.http.Request($.net.http.GET, hBaseUrl);
	request.headers.set("Accept", "application/json");
 

	// send the request and synchronously get the response
	client.request(request, dest);
	response = client.getResponse();
	
	//split response into components cookies, header and body
	splitRepsonse(response);
	
	var objBody;
	objBody = JSON.parse(body);
	
	return {"HbaseUrl" : hBaseUrl, "status": response.status, "cookies": co, "headers": he, "body": decodeHbase(objBody) } ;

}

function Put (HbaseTable, rowKey, reqBody) {
	hBaseUrl = '/' + HbaseTable + '/' + rowKey + '/' ;
	request = new $.net.http.Request($.net.http.PUT, hBaseUrl);
	request.headers.set("Content-Type", "application/json");
	
	
	// Example inpboud row structure reqBody
	/*e.g.
	{
	  "Doc_Name" : "DOC2",
	  "Row" : "1",
	  "Free_Text" : "a",
	  "Value" : "1" 
	}
	*/
	
	
	//Hbase needs strings in base64 encoding and in predefined format
	/* E.g.
	{
        "Row": {
            "key": "VEVTVDpET0MyOjE=",
            "Cell": [
                {
                    "column": "dGFibGU6RG9jX05hbWU=",
                    "$": "RE9DMg=="
                },
                {
                    "column": "dGFibGU6Um93",
                    "$": "MQ=="
                },
                {
                    "column": "dGFibGU6RnJlZV9UZXh0",
                    "$": "YQ=="
                },
                {
                    "column": "dGFibGU6VmFsdWU=",
                    "$": "MQ=="
                }
            ]
        }
    }   
                
	 */
	
	// Set Hbase body structure
	var hbaseBody = {};
	hbaseBody.Row = {};
	hbaseBody.Row.key = base64.encode(rowKey) ;
	hbaseBody.Row.Cell = [];
	
	//Populate and Encode reqBody fields
	if (reqBody != undefined ) {
		for (var key in reqBody) {
			var hbaseCell = { "column" : '' , "$" : ''};
			hbaseCell.column = base64.encode("table:"+key);
			hbaseCell.$ = base64.encode(String(reqBody[key]));
			hbaseBody.Row.Cell.push(hbaseCell);
			
		}
	} 
	
	//Send Request oHbase Stargate
	request.setBody(JSON.stringify(hbaseBody));
	client.request(request, dest);
	response = client.getResponse();
	splitRepsonse(response);
	
	return {"HbaseUrl" : hBaseUrl, "status": response.status, "cookies": co, "headers": he, "reqBody": reqBody, "hbaseBody": hbaseBody } ;
}

//Function to split response into components: cookies, header and body
function splitRepsonse(response)	{

	//get all the cookies and headers from the response
	for(var c in response.cookies) {
	    co.push(response.cookies[c]);
	}
	 
	 
	for(var c in response.headers) {
	     he.push(response.headers[c]);
	}
	 
	 
	// get the body of the Response from Hbase
	if(!response.body)
	    body = "";
	else
	    body = response.body.asString();

}




// Function to decode HBASE Stargate body of response
// Hbase returns strings in base64 encoding
// Strings need to be decoded which could be done in XSJS or in the Front End JS
// I've opted to do in XSJS as I am also reformatting the body before sending to front end
function decodeHbase(objBody) {
	
	if (objBody.Row != undefined ) {
	  for (var i=0;i<objBody.Row.length;i++) {
	  objBody.Row[i].key = base64.decode(objBody.Row[i].key);
	  for (var j=0;j<objBody.Row[i].Cell.length;j++) {
	  objBody.Row[i].Cell[j].column = base64.decode(objBody.Row[i].Cell[j].column);
	  objBody.Row[i].Cell[j].$ = base64.decode(objBody.Row[i].Cell[j].$);
	  
	  // change formats a bit:
	  
	  // Change date format to human readable
	  objBody.Row[i].Cell[j].timestamp =    new Date(objBody.Row[i].Cell[j].timestamp);
	  // remove HBASE column family from output
	  objBody.Row[i].Cell[j].column = objBody.Row[i].Cell[j].column.replace("table:","");
	  
	  }
	  }
	}
	
	return objBody;
}

