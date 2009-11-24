<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="ext-examples" description="Engine extended examples" useJMX="true">

	<DBConnection ident="postgre_foodmart" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/foodmart" driver="org.postgresql.Driver" />

	<FunctionalTest ident="LDAPReaderWriter" graphFile="graph/graphLdapReaderWriter.grf">
          <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/persons.txt" supposedFile="data-out/ldap_persons.txt"/>	                                                                 
	</FunctionalTest>

	<FunctionalTest ident="ApproximativeJoin" graphFile="graph/graphApproximativeJoin.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/conformingKeyGenerator.txt" supposedFile="supposed-out/conformingKeyGenerator.AproximativeJoin.txt"/>	                                                                 
	 	  <FlatFile outputFile="data-out/conformingMetaphone.txt" supposedFile="supposed-out/conformingMetaphone.AproximativeJoin.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/conformingNYSIIS.txt" supposedFile="supposed-out/conformingNYSIIS.AproximativeJoin.txt"/>	                                                                      
	 	  <FlatFile outputFile="data-out/conformingSoundex.txt" supposedFile="supposed-out/conformingSoundex.AproximativeJoin.txt"/>	                                                                 
	 	  <FlatFile outputFile="data-out/suspicoiusKeyGenerator.txt" supposedFile="supposed-out/suspicoiusKeyGenerator.AproximativeJoin.txt"/>                                                                    
	 	  <FlatFile outputFile="data-out/suspicoiusMetaphone.txt" supposedFile="supposed-out/suspicoiusMetaphone.AproximativeJoin.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/suspicoiusNYSIIS.txt" supposedFile="supposed-out/suspicoiusNYSIIS.AproximativeJoin.txt"/>	                                                                     
	 	  <FlatFile outputFile="data-out/suspicoiusSoundex.txt" supposedFile="supposed-out/suspicoiusSoundex.AproximativeJoin.txt"/>	                                                                  
	 	  <FlatFile outputFile="data-out/customersSoundex.out" supposedFile="supposed-out/empty.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/customersMetaphone.out" supposedFile="supposed-out/empty.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/customersKeyGenerator.out" supposedFile="supposed-out/empty.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/customersNYSIIS.out" supposedFile="supposed-out/empty.txt"/>	                                                                  
	 	  <FlatFile outputFile="data-out/employeeKeyGenerator.out" supposedFile="supposed-out/employeeKeyGenerator.AproximativeJoin.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/employeeMetaphone.out" supposedFile="supposed-out/employeeMetaphone.AproximativeJoin.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/employeeNYSIIS.out" supposedFile="supposed-out/employeeNYSIIS.AproximativeJoin.out"/>	                                                                        
	 	  <FlatFile outputFile="data-out/employeeSoundex.out" supposedFile="supposed-out/employeeSoundex.AproximativeJoin.out"/>	                                                               
	     <DeleteFile file="seq/id0.seq"/>
	     <DeleteFile file="seq/id1.seq"/>
	     <DeleteFile file="seq/id2.seq"/>
	     <DeleteFile file="seq/id3.seq"/>
	</FunctionalTest>

	<FunctionalTest ident="CheckForeignKey" graphFile="graph/graphCheckForeignKey.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/wrongKey.out" supposedFile="supposed-out/wrongKey.CheckForeignKey.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteMsSql" graphFile="graph/graphDBExecuteMsSql.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/mssql.out" supposedFile="supposed-out/mssql.DBExecuteMsSql.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteMySql" graphFile="graph/graphDBExecuteMySql.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/foundClients.txt" supposedFile="supposed-out/foundClients.DBExecuteMySql.txt"/>	                                                                    
	     <DeleteFile file="seq/id.seq"/>
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteOracle" graphFile="graph/graphDBExecuteOracle.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/countries.txt" supposedFile="supposed-out/countries.DBExecuteOracle.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBExecutePostgre" graphFile="graph/graphDBExecutePostgre.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/cities.txt" supposedFile="supposed-out/cities.DBExecutePostgre.txt"/>	                                                                    
	     <DeleteFile file="seq/seq.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="DBExecuteSybase" graphFile="graph/graphDBExecuteSybase.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/sybase.out" supposedFile="supposed-out/sybase.DBExecuteSybase.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBJoin" graphFile="graph/graphDBJoin.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/joined.txt" supposedFile="supposed-out/joined.DBJoin.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBLoad" graphFile="graph/graphDBLoad.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/rejected.txt" supposedFile="supposed-out/rejected.DBLoad.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBLoad5" graphFile="graph/graphDBLoad5.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
      	<DBTableToXMLFile outputTable="employee_tmp" supposedTable="employee_names" outputTableConnection="postgre_foodmart" supposedXMLFile="supposed-out/employee.DBLoad5.xml"/>
<!--	      <DBTableToTable
	      	 outputTable="employee_tmp" 
	      	 outputTableConnection="postgre_foodmart"
	      	 supposedTable="employee_names"
	      	 supposedTableConnection="postgre_foodmart"
	      /> -->
      	<DeleteTable connection="postgre_foodmart" name="employee_tmp"/>
	</FunctionalTest>

	<FunctionalTest ident="DBLoad6" graphFile="graph/graphDBLoad6.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
      	<DBTableToXMLFile outputTable="employee_tmp" supposedTable="employee_names_dates" outputTableConnection="postgre_foodmart" supposedXMLFile="supposed-out/employee.DBLoad6.xml"/>
<!--	      <DBTableToTable
	      	 outputTable="employee_tmp" 
	      	 outputTableConnection="postgre_foodmart"
	      	 supposedTable="employee_names_dates"
	      	 supposedTableConnection="postgre_foodmart"
	      />--> 
      	<DeleteTable connection="postgre_foodmart" name="employee_tmp"/>
	</FunctionalTest>

	<FunctionalTest ident="DBLookup" graphFile="graph/graphDBLookup.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.DBLookup.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/log.err" supposedFile="supposed-out/log.DBLookup.err"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBRead" graphFile="graph/graphDBRead.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/customer.out" supposedFile="supposed-out/customer.DBRead.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/intersection_customer_employee.txt" supposedFile="supposed-out/intersection_customer_employee.DBRead.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/employee.out" supposedFile="supposed-out/employee.DBRead.out"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBUnload" graphFile="graph/graphDBUnload.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/employees.list.out" supposedFile="supposed-out/employees.list.DBUnload.out"/>	                                                                    
	     <DeleteFile file="dbInc.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="DBUnload2" graphFile="graph/graphDBUnload2.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/employees.txt" supposedFile="supposed-out/employees.DBUnload2.txt"/>	                                                                    
	</FunctionalTest>

	<FunctionalTest ident="DBUnloadParametrized" graphFile="graph/graphDBUnloadParametrized.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
<!-- We are not able to compare xls files now. This comparison test should be uncommented after issue 2981 is resolved.
	 	  <FlatFile outputFile="data-out/employees.xls" supposedFile="supposed-out/employees.DBUnloadParametrized.xls"/>	          
	     <DeleteFile file="data-out/employees.xls"/>
-->                                                          
	</FunctionalTest>

	<FunctionalTest ident="DBUnloadUniversal" graphFile="graph/graphDBUnloadUniversal.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/employee.output" supposedFile="supposed-out/employee.DBUnloadUniversal.output"/>	                                                                    
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

	<FunctionalTest ident="LookupJoin" graphFile="graph/graphLookupJoin.grf">
         <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
	 	  <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.DBLookup.out"/>	                                                                    
	</FunctionalTest>

</TestScenario>
