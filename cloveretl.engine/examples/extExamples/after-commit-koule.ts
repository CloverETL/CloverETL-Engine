<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="ext-examples-koule" description="Engine extended examples" useJMX="true">

	<DBConnection ident="postgre_test" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/test" driver="org.postgresql.Driver" />
	<DBConnection ident="oracle" type="ORACLE" user="test" password="test" URL="jdbc:oracle:thin:@koule:1521:orcl" driver="oracle.jdbc.OracleDriver" >
	    	 <DBUnitFeature name="http://www.dbunit.org/features/qualifiedTableNames" enabled="true"/>
	    	 <DBUnitFeature name="http://www.dbunit.org/features/skipOracleRecycleBinTables" enabled="true"/>
	</DBConnection>
	<DBConnection ident="mysql" type="MYSQL" user="test" password="" URL="jdbc:mysql://koule/test?zeroDateTimeBehavior=convertToNull" driver="org.gjt.mm.mysql.Driver" />
	<DBConnection ident="infobright" type="MYSQL" user="root" password="semafor" URL="jdbc:mysql://localhost:5029/test" driver="org.gjt.mm.mysql.Driver" />
	<DBConnection ident="db2" type="DB2" user="db2inst" password="semafor" URL="jdbc:db2://koule:50002/test" driver="com.ibm.db2.jcc.DB2Driver" />
	<DBConnection ident="infobright" type="MYSQL" user="root" password="" URL="jdbc:mysql://koule:5029/test" driver="org.gjt.mm.mysql.Driver" />
	
	<FunctionalTest ident="PostgreDataWriter" graphFile="graph/graphPostgreSqlDataWriter.grf">
	      <SQLStatement connection="postgre_test">DELETE FROM test</SQLStatement>
	      <DBTableToTable
	      	 outputTable="test" 
	      	 outputTableConnection="postgre_test"
	      	 supposedTable="test_supposed"
	      	 supposedTableConnection="postgre_test"
	      />
	</FunctionalTest>
	
	<FunctionalTest ident="OracleDataWriter" graphFile="graph/graphOracleDataWriter.grf">
	      	<SQLStatement connection="oracle">DELETE FROM test.writer_test</SQLStatement>
<!--	      <DBTableToTable
	      	 outputTable="test.writer_test" 
	      	 outputTableConnection="oracle"
	      	 supposedTable="test.test_supposed"
	      	 supposedTableConnection="oracle"
	      />
      	 <DBTableToXMLFile outputTable="writer_test" supposedTable="test" outputTableConnection="oracle" supposedXMLFile="supposed-out/oracle_supposed.xml"/>--> 
	 	  <FlatFile outputFile="data-out/bad0Port.bad" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad1.kkk" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad1Port.bad" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad2.kkk" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad2Port.bad" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad3Port.bad" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="MySqlDataWriter" graphFile="graph/graphMysqlDataWriter.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
        <Property name="connection_file" value="mysql-localhost.cfg" />
	      	<SQLStatement connection="mysql">DELETE FROM test</SQLStatement>
<!--	      <DBTableToTable
	      	 outputTable="test" 
	      	 outputTableConnection="mysql"
	      	 supposedTable="test_supposed"
	      	 supposedTableConnection="mysql"
	      />-->
	 	  <FlatFile outputFile="data-out/out.dat" supposedFile="supposed-out/out.MysqlDataWriter.dat"/>	                                                                    
	 	  <FlatFile outputFile="data-out/mysql.out" supposedFile="supposed-out/mysql.MysqlDataWriter.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DB2DataWriter" graphFile="graph/graphDb2Load.grf">
	      <SQLStatement connection="db2">DELETE FROM mytab</SQLStatement>
	      <DBTableToTable
	      	 outputTable="mytab" 
	      	 outputTableConnection="db2"
	      	 supposedTable="mytab_supposed"
	      	 supposedTableConnection="db2"
	      />
	 	  <FlatFile outputFile="data-out/rejected_delimited.txt" supposedFile="supposed-out/rejected_delimited.Db2Load.txt"/>	                                                                    
	 	  <FlatFile outputFile="/home/db2inst/rejected_fix.txt" supposedFile="supposed-out/rejected_fix.Db2Load.txt"/>	                                                                    
	</FunctionalTest>
	
	<FunctionalTest ident="InfobrightDataWriter" graphFile="graph/graphInfobrightDataWriter.grf">
	      <SQLStatement connection="infobright">drop table test</SQLStatement>
	      <DBTableToTable
	      	 outputTable="test" 
	      	 outputTableConnection="infobright"
	      	 supposedTable="test_supposed"
	      	 supposedTableConnection="infobright"
	      />
	 	  <FlatFile outputFile="data-tmp/infobright_out.txt" supposedFile="supposed-out/infobright_out.txt"/>	                                                                    
	</FunctionalTest>
	
</TestScenario>
