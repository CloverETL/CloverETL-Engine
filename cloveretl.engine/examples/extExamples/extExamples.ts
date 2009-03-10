<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="simple-examples" description="Engine extended examples" useJMX="true">    

	<DBConnection ident="postgre" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/test" driver="org.postgresql.Driver" />
	<DBConnection ident="oracle" type="ORACLE" user="test" password="test" URL="jdbc:oracle:thin:@koule:1521:xe" driver="oracle.jdbc.OracleDriver" />
	<DBConnection ident="mysql" type="MYSQL" user="test" password="" URL="jdbc:mysql://koule/test" driver="org.gjt.mm.mysql.Driver" />

	<FunctionalTest ident="LDAPReaderWriter" graphFile="graph/graphLdapReaderWriter.grf">
	</FunctionalTest>

	<FunctionalTest ident="PostgreDataWriter" graphFile="graph/graphPostgreSqlDataWriter.grf">
		  <Wildcard>
	      	<SQLStatement connection="postgre">DELETE FROM test</SQLStatement>
	      </Wildcard>
	      <DBTableComparison>
	      	 <OutputTable name="test" connection="postgre"/>
	      	 <SupposedTable name="test_supposed" connection="postgre"/>
	      </DBTableComparison>
	</FunctionalTest>
	
	<FunctionalTest ident="OracleDataWriter" graphFile="graph/graphOracleDataWriter.grf">
		  <Wildcard>
	      	<SQLStatement connection="oracle">DELETE FROM test</SQLStatement>
	      </Wildcard>
	      <DBTableComparison>
	      	 <OutputTable name="test" connection="oracle"/>
	      	 <SupposedTable name="test_supposed" connection="oracle"/>
	      </DBTableComparison>
	 	  <FlatFile outputFile="data-out/bad0Port.bad" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad1.kkk" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad1Port.bad" supposedFile="supposed-out/bad1.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad2.kkk" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad2Port.bad" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	 	  <FlatFile outputFile="data-out/bad3Port.bad" supposedFile="supposed-out/bad2.OracleDataWriter.kkk"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="MySqlDataWriter" graphFile="graph/graphMysqlDataWriter.grf">
		  <Wildcard>
	      	<SQLStatement connection="mysql">DELETE FROM test</SQLStatement>
	      </Wildcard>
	      <DBTableComparison>
	      	 <OutputTable name="test" connection="mysql"/>
	      	 <SupposedTable name="test_supposed" connection="mysql"/>
	      </DBTableComparison>
	 	  <FlatFile outputFile="data-out/out.dat" supposedFile="supposed-out/out.MysqlDataWriter.dat"/>	                                                                    
	</FunctionalTest>

</TestScenario>
