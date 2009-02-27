<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="ext-examples" description="Engine extended examples" useJMX="true">

	<DBConnection ident="postgre" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/test" driver="org.postgresql.Driver" />

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
	</FunctionalTest>

</TestScenario>
