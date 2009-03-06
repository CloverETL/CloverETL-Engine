<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="ext-examples" description="Engine extended examples" useJMX="true">

	<DBConnection ident="postgre" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/test" driver="org.postgresql.Driver" />
	<DBConnection ident="oracle" type="ORACLE" user="test" password="test" URL="jdbc:oracle:thin:@koule:1521:xe" driver="oracle.jdbc.OracleDriver" />
	<DBConnection ident="mysql" type="MYSQL" user="test" password="" URL="jdbc:mysql://koule/test" driver="org.gjt.mm.mysql.Driver" />

	<FunctionalTest ident="LDAPReaderWriter" graphFile="graph/graphLdapReaderWriter.grf">
	</FunctionalTest>

	<FunctionalTest ident="PostgreDataWriter" graphFile="graph/graphPostgreSqlDataWriter.grf">
	      <SQLStatement connection="postgre">DELETE FROM test</SQLStatement>
	      <DBTableToTable
	      	 outputTable="test" 
	      	 outputTableConnection="postgre"
	      	 supposedTable="test_supposed"
	      	 supposedTableConnection="postgre"
	      />
	</FunctionalTest>
	
	<FunctionalTest ident="OracleDataWriter" graphFile="graph/graphOracleDataWriter.grf">
	      	<SQLStatement connection="oracle">DELETE FROM test</SQLStatement>
	      <DBTableToTable
	      	 outputTable="test" 
	      	 outputTableConnection="oracle"
	      	 supposedTable="test_supposed"
	      	 supposedTableConnection="oracle"
	      />
	 	  <FlatFile outputFile="data-out/bad0Port.bad" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad1.kkk" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad1Port.bad" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad2.kkk" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad2Port.bad" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad3Port.bad" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="MySqlDataWriter" graphFile="graph/graphMysqlDataWriter.grf">
	      	<SQLStatement connection="mysql">DELETE FROM test</SQLStatement>
	      <DBTableToTable
	      	 outputTable="test" 
	      	 outputTableConnection="mysql"
	      	 supposedTable="test_supposed"
	      	 supposedTableConnection="mysql"
	      />
	 	  <FlatFile outputFile="data-out/out.dat" supposedFile="supposed-out/out.MysqlDataWriter.dat"/>	                                                                    
	</FunctionalTest>

</TestScenario>
