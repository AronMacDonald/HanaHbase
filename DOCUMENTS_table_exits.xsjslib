//import a library for doing PUT to HBase Stargate
$.import("HanaHbase","Hbase"); 
var Hbase = $.HanaHbase.Hbase;


/*
@param
 SAP HANA SPS 07 XSODATA Exits in XSJS   has  at 4 minute mark
*/
function update_instead(param) {
 $.trace.debug("entered function");
 let before = param.beforeTableName;
 let after = param.afterTableName;
 //let pStmt = param.connection.prepareStatement('select * from "' + after +'"');
 
 var pStmt = param.connection.prepareStatement('select * from "' + after +'"');
 
 //Load the 'after' table into JSON structure
 var afterRecord = recordSetToJSON(pStmt.executeQuery(), 'Details');
 pStmt.close();

 //Get latest timestamp
 pStmt = param.connection.prepareStatement('select current_timestamp from dummy');
 var info = recordSetToJSON(pStmt.executeQuery(), 'Details');
 pStmt.close();
 
 //Take the single record in 'after' and update DB table
 for ( var i = 0; i < 2; i++) {
	 var pStmt
	 if (i < 1) {
		 //insert
		 //pStmt = param.connection.prepareStatement('insert into "HADOOP"."s0000716522trial.hanamed1.HBASE1::TEST" values(?,?,?,?,?)');
		 
		 //update UPDATE <table> SET <field>=? WHERE <field>=?
		 var sql_stmt =  
             'update "HADOOP"."HanaHbase::DOCUMENTS" ' +  
             'SET "Free_Text"=? , "Value"=?, "Changed_by"=user(), "Changed_at"=now() ' +  
             'WHERE "Doc_Name"=? and "Row"=? '; 
		 
		 //throw sql_stmt;
		 pStmt = param.connection.prepareStatement(sql_stmt);
		 
		 //SET
		 pStmt.setString(1, afterRecord.Details[0].Free_Text);
		 pStmt.setFloat(2, afterRecord.Details[0].Value);
		 
		 
		 //Date format is wrong- can't parse
		 //var vDT =  new Date().getTime(); 
		 //pStmt.setString(3,  "/Date(" + vDT + ")/") ; 
		 //pStmt.setString(3, info.Details[0].CURRENT_TIMESTAMP);
		
		 //WHERE
		 pStmt.setString(3, afterRecord.Details[0].Doc_Name);
		 pStmt.setInteger(4, afterRecord.Details[0].Row);
		 
		 //pStmt.executeUpdate();
		 //pStmt.close();
	 } else {
		 //
		 pStmt = param.connection.prepareStatement('TRUNCATE TABLE "' + after +'"');
		 pStmt.executeUpdate();
		 pStmt.close();
		 
		 pStmt = param.connection.prepareStatement('insert into "' + after +'" values(?,?,?,?,user(),now())');
		 
		 pStmt.setString(1, afterRecord.Details[0].Doc_Name);
		 pStmt.setInteger(2, afterRecord.Details[0].Row);
		 pStmt.setString(3, afterRecord.Details[0].Free_Text);
		 pStmt.setFloat(4, afterRecord.Details[0].Value);
		 //pStmt.setString(5, afterRecord.Details[0].Col5); //timestamp painful


		 
	 }
	 
	 pStmt.executeUpdate();
	 pStmt.close();
	 
	 
	 
	 
 }
 
 // ...
 if (1 == 1) {
 // update HADOOP HBASE
	 var Doc_Name = afterRecord.Details[0].Doc_Name;
	 var Row = afterRecord.Details[0].Row;
	 var HbaseTable = 'HanaTableLog';
	 var rowKey =  "DOCUMENTS:" + Doc_Name + ":" + Row;
     
	 var RowChanges ={};
	 
	 // Hbase Put function expects changes in JSON Form for all fields changed
	 /* E.g.
		{
			 // "Doc_Name" : "DOC2",
			 // "Row" : "1",
			  "Free_Text" : "a",
			  "Value" : "1" 
			}
	 */	
	 
	 //Get the before and after records, compare and update Hbase with only those field that changed
	 pStmt = param.connection.prepareStatement('select * from "' + before +'"');
	 var beforeRecord = recordSetToJSON(pStmt.executeQuery(), 'Details');
	 pStmt.close();
	 
	 pStmt = param.connection.prepareStatement('select * from "' + after +'"');
	 afterRecord = recordSetToJSON(pStmt.executeQuery(), 'Details');
	 pStmt.close();
	 
	 if (afterRecord.Details[0].Free_Text != beforeRecord.Details[0].Free_Text) {
		 RowChanges.Free_Text = afterRecord.Details[0].Free_Text; 
	 }

	 if (afterRecord.Details[0].Value != beforeRecord.Details[0].Value) {
		 RowChanges.Value = afterRecord.Details[0].Value; 
	 }

	 if (afterRecord.Details[0].Changed_by != beforeRecord.Details[0].Changed_by ) {
		 RowChanges.Changed_by  = afterRecord.Details[0].Changed_by ; 
	 }
	 
	 Hbase.Put(HbaseTable, rowKey,RowChanges);
	 
 } else {
 throw "an error occurred; check access privileges";
 }
}


// Following logic from Thomas Jung
// http://scn.sap.com/thread/3447784

/**
@function Escape Special Characters in JSON strings
@param {string} input - Input String
@returns {string} the same string as the input but now escaped
*/
function escapeSpecialChars(input) {
          if(typeof(input) != 'undefined' && input != null)
          {
          return input
    .replace(/[\\]/g, '\\\\')
    .replace(/[\"]/g, '\\\"')
    .replace(/[\/]/g, '\\/')
    .replace(/[\b]/g, '\\b')
    .replace(/[\f]/g, '\\f')
    .replace(/[\n]/g, '\\n')
    .replace(/[\r]/g, '\\r')
    .replace(/[\t]/g, '\\t'); }
          else{
 
                    return "";
          }
}
 

/**
@function Converts any XSJS RecordSet object to a JSON Object
@param {object} rs - XSJS Record Set object
@param {optional String} rsName - name of the record set object in the JSON
@returns {object} JSON representation of the record set data
*/
function recordSetToJSON(rs,rsName){
          rsName = typeof rsName !== 'undefined' ? rsName : 'entries';
 
          var meta = rs.getMetaData();
          var colCount = meta.getColumnCount();
          var values=[];
          var table=[];
          var value="";
          while (rs.next()) {
          for (var i=1; i<=colCount; i++) {
                    value = '"'+meta.getColumnLabel(i)+'" : ';
               switch(meta.getColumnType(i)) {
               case $.db.types.VARCHAR:
               case $.db.types.CHAR:
                    value += '"'+ escapeSpecialChars(rs.getString(i))+'"';
                    break;
               case $.db.types.NVARCHAR:
               case $.db.types.NCHAR:
               case $.db.types.SHORTTEXT:
                    value += '"'+escapeSpecialChars(rs.getNString(i))+'"';
                    break;
               case $.db.types.TINYINT:
               case $.db.types.SMALLINT:
               case $.db.types.INT:
               case $.db.types.BIGINT:
                    value += rs.getInteger(i);
                    break;
               case $.db.types.DOUBLE:
                    value += rs.getDouble(i);
                    break;
               case $.db.types.DECIMAL:
                    value += rs.getDecimal(i);
                    break;
               case $.db.types.REAL:
                    value += rs.getReal(i);
                    break;
               case $.db.types.NCLOB:
               case $.db.types.TEXT:
                    value += '"'+ escapeSpecialChars(rs.getNClob(i))+'"';
                    break;
               case $.db.types.CLOB:
                    value += '"'+ escapeSpecialChars(rs.getClob(i))+'"';
                    break;                   
               case $.db.types.BLOB:
                          value += '"'+ $.util.convert.encodeBase64(rs.getBlob(i))+'"';
                    break;                   
               case $.db.types.DATE:
                    value += '"'+rs.getDate(i)+'"';
                    break;
               case $.db.types.TIME:
                    value += '"'+rs.getTime(i)+'"';
                    break;
               case $.db.types.TIMESTAMP:
                    value += '"'+rs.getTimestamp(i)+'"';
                    break;
               case $.db.types.SECONDDATE:
                    value += '"'+rs.getSeconddate(i)+'"';
                    break;
               default:
                    value += '"'+escapeSpecialChars(rs.getString(i))+'"';
               }
               values.push(value);
               }
             table.push('{'+values+'}');
          }
          return           JSON.parse('{"'+ rsName +'" : [' + table          +']}');
 
}