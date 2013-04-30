<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="ext-examples" description="Engine extended examples" useJMX="true">

	<DBConnection ident="postgre_foodmart" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/foodmart" driver="org.postgresql.Driver" />
	<DBConnection ident="infobright" type="MYSQL" user="root" password="" URL="jdbc:mysql://koule:5029/test" driver="org.gjt.mm.mysql.Driver" />
	<DBConnection ident="infobright_3_5" type="MYSQL" user="root" password="" URL="jdbc:mysql://koule:5030/test" driver="org.gjt.mm.mysql.Driver" />
	<DBConnection ident="derby" type="DERBY" user="app" password="derby" 
		URL="jdbc:derby://localhost:1527/${PROJECT_DIR}/data-in/derby.db;" 
		driver="org.apache.derby.jdbc.ClientDriver" />

    <FunctionalTest ident="DBExecuteDerby" graphFile="graph/graphDBExecuteDerby.grf" absoluteProjectPath="true">
        <FlatFile outputFile="data-out/cities.txt" supposedFile="supposed-out/cities.DBExecutePostgre.txt"/>	                                                                    
        <DeleteFile file="seq/seq.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteMsSql" graphFile="graph/graphDBExecuteMsSql.grf">
        <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
        <FlatFile outputFile="data-out/mssql.out" supposedFile="supposed-out/mssql.DBExecuteMsSql.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteMySql" graphFile="graph/graphDBExecuteMySql.grf">
         <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/foundClients.txt" supposedFile="supposed-out/foundClients.DBExecuteMySql.txt"/>	                                                                    
	     <DeleteFile file="seq/id.seq"/>
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteOracle" graphFile="graph/graphDBExecuteOracle.grf">
        <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
        <Property name="LIB_DIR" value="../../cloveretl.test.scenarios/lib" />

	 	  <FlatFile outputFile="data-out/countries.txt" supposedFile="supposed-out/countries.DBExecuteOracle.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBExecutePostgre" graphFile="graph/graphDBExecutePostgre.grf">
        <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/cities.txt" supposedFile="supposed-out/cities.DBExecutePostgre.txt"/>	                                                                    
	     <DeleteFile file="seq/seq.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteSybase" graphFile="graph/graphDBExecuteSybase.grf">
        <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/sybase.out" supposedFile="supposed-out/sybase.DBExecuteSybase.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBJoin" graphFile="graph/graphDBJoin.grf" absoluteProjectPath="true">
	 	  <FlatFile outputFile="data-out/joined.txt" supposedFile="supposed-out/joined.DBJoin.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/rejected.txt" supposedFile="supposed-out/rejected.DBJoin.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBLoad" graphFile="graph/graphDBLoad.grf" absoluteProjectPath="true">
	 	  <FlatFile outputFile="data-out/rejected.txt" supposedFile="supposed-out/rejected.DBLoad.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBLoad5" graphFile="graph/graphDBLoad5.grf" absoluteProjectPath="true">
	<!-- <DBTableToXMLFile outputTable="employee_tmp" supposedTable="employee_names" outputTableConnection="derby" supposedXMLFile="supposed-out/employee.DBLoad5.xml"/>
	      <DBTableToTable
	      	 outputTable="employee_tmp" 
	      	 outputTableConnection="postgre_foodmart"
	      	 supposedTable="employee_names"
	      	 supposedTableConnection="postgre_foodmart"
	      /> 
      	<DeleteTable connection="derby" name="employee_tmp"/> -->
      	<FlatFile outputFile="data-out/employee.DBLoad5.xml" supposedFile="supposed-out/employee.DBLoad5.xml"/>
	</FunctionalTest>

	<FunctionalTest ident="DBLoad6" graphFile="graph/graphDBLoad6.grf" absoluteProjectPath="true">
	<!-- <DBTableToXMLFile outputTable="employee_tmp" supposedTable="employee_names_dates" outputTableConnection="derby" supposedXMLFile="supposed-out/employee.DBLoad6.xml"/>
	      <DBTableToTable
	      	 outputTable="employee_tmp" 
	      	 outputTableConnection="postgre_foodmart"
	      	 supposedTable="employee_names_dates"
	      	 supposedTableConnection="postgre_foodmart"
	      />
      	<DeleteTable connection="derby" name="employee_tmp"/> -->
      	<FlatFile outputFile="data-out/employee.DBLoad6.xml" supposedFile="supposed-out/employee.DBLoad6.xml"/>
	</FunctionalTest>

	<FunctionalTest ident="DBLookup" graphFile="graph/graphDBLookup.grf" absoluteProjectPath="true">
 	 	  <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.DBLookup.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/log.err" supposedFile="supposed-out/log.DBLookup.err"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBRead" graphFile="graph/graphDBRead.grf" absoluteProjectPath="true">
	 	  <FlatFile outputFile="data-out/customer.out" supposedFile="supposed-out/customer.DBRead.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/intersection_customer_employee.txt" supposedFile="supposed-out/intersection_customer_employee.DBRead.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/employee.out" supposedFile="supposed-out/employee.DBRead.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBUnload" graphFile="graph/graphDBUnload.grf" absoluteProjectPath="true">
	 	  <FlatFile outputFile="data-out/employees.list.out" supposedFile="supposed-out/employees.list.DBUnload.out"/>	                                                                    
	     <DeleteFile file="dbInc.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="DBUnload_Mysql" graphFile="graph/graphDBUnload.grf" absoluteProjectPath="true">
          <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
          <Property name="connection" value="mysql.cfg" />
	 	  <FlatFile outputFile="data-out/employees.list.out" supposedFile="supposed-out/employees.list.DBUnload.out"/>	                                                                    
	     <DeleteFile file="dbInc.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="DBUnload2" graphFile="graph/graphDBUnload2.grf" absoluteProjectPath="true">
	 	  <FlatFile outputFile="data-out/employees.txt" supposedFile="supposed-out/employees.DBUnload2.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBUnloadParametrized" graphFile="graph/graphDBUnloadParametrized.grf" absoluteProjectPath="true">
 	  <XlsFile outputFile="data-out/employees.xls" supposedFile="supposed-out/employees.DBUnloadParametrized.xls"/>	          
	     <DeleteFile file="data-out/employees.xls"/>
	</FunctionalTest>

	<FunctionalTest ident="DBUnloadUniversal" graphFile="graph/graphDBUnloadUniversal.grf" absoluteProjectPath="true">
 	 	  <FlatFile outputFile="data-out/employee.output" supposedFile="supposed-out/employee.DBUnloadUniversal.output"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="LDAPReaderWriter" graphFile="graph/graphLdapReaderWriter.grf">
          <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/persons.txt" supposedFile="data-out/ldap_persons.txt"/>	                                                                 
	</FunctionalTest>

	<FunctionalTest ident="JMS" graphFile="graph/graphJms.grf">
         <Property name="LIB_DIR" value="examples/extExamples/lib" />
	 	  <FlatFile outputFile="data-out/jms.out" supposedFile="supposed-out/jms.JMS.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="JmsSingleXmlField" graphFile="graph/graphJmsSingleXmlField.grf">
         <Property name="LIB_DIR" value="examples/extExamples/lib" />
	 	  <FlatFile outputFile="data-out/customers.out" supposedFile="supposed-out/customers.JmsSingleXmlField.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/orders.out" supposedFile="supposed-out/orders.JmsSingleXmlField.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="LookupJoin" graphFile="graph/graphLookupJoin.grf" absoluteProjectPath="true">
	 	  <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.LookupJoin.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="InfobrightDataWriterRemote" graphFile="graph/graphInfobrightDataWriterRemote.grf">
        <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
      	<SQLStatement connection="infobright">drop table test</SQLStatement>
	 	  <FlatFile outputFile="data-tmp/infobright_out.xml" supposedFile="supposed-out/infobright_out.InfobrightDataWriter.xml"/>	                                                                    
     	 <DBTableToXMLFile outputTable="test" supposedTable="test" outputTableConnection="infobright" supposedXMLFile="supposed-out/infobright_out.InfobrightDataWriter.xml"/>
	</FunctionalTest>
	
	<FunctionalTest ident="InfobrightDataWriterRemote_3_5" graphFile="graph/graphInfobrightDataWriterRemote.grf">
        <Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
         <Property name="connection_cfg" value="infobright-3-5.cfg" />
      	<SQLStatement connection="infobright_3_5">drop table test</SQLStatement>
	 	  <FlatFile outputFile="data-tmp/infobright_out.xml" supposedFile="supposed-out/infobright_out.InfobrightDataWriter.xml"/>	                                                                    
     	 <DBTableToXMLFile outputTable="test" supposedTable="test" outputTableConnection="infobright_3_5" supposedXMLFile="supposed-out/infobright_out.InfobrightDataWriter.xml"/>
	</FunctionalTest>
	
</TestScenario>
