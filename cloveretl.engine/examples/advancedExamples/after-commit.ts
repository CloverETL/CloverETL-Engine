<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="advanced-examples" description="Engine advanced examples" useJMX="true">

	<DBConnection ident="postgre_foodmart" type="POSTGRE" user="test" password="test" URL="jdbc:postgresql://koule/foodmart" driver="org.postgresql.Driver" />

    <FunctionalTest ident="AccessLogParsing" graphFile="graph/AccessLogParsing.grf">
        <FlatFile outputFile="data-out/irrelevant.txt" supposedFile="supposed-out/irrelevant.txt"/>
        <FlatFile outputFile="data-out/statistics.xls" supposedFile="supposed-out/statistics.xls"/>     
        <DeleteFile file="data-out/irrelevant.txt"/>
        <DeleteFile file="data-out/statistics.xls"/>
    </FunctionalTest>
	
	<FunctionalTest ident="Revenues" graphFile="graph/graphRevenues.grf">
        <Property name="CONN_DIR" value="../../../cloveretl.test.scenarios/conn" />
      	<DBTableToXMLFile outputTable="halfyear_aggr_revenues" supposedTable="pololeti_agr_vynosy" outputTableConnection="postgre_foodmart" 
      		supposedXMLFile="supposed-out/revenues.xml"/>
      	<DBTableToXMLFile outputTable="Clients_without_revenues" supposedTable="Pololeti_zakaznici_bez_vynosu" outputTableConnection="postgre_foodmart" 
      		supposedXMLFile="supposed-out/revenues.xml"/>
      	<DeleteTable connection="postgre_foodmart" name="halfyear_aggr_revenues"/>
      	<DeleteTable connection="postgre_foodmart" name="Clients_without_revenues"/>
	</FunctionalTest>

    <FunctionalTest ident="SOAP_EXAMPLE" graphFile="graph/SOAP_EXAMPLE.grf">
<!-- We are not able to compare xls files now. This comparison test should be uncommented after issue 2981 is resolved.
        <FlatFile outputFile="data-out/extract.xls" supposedFile="supposed-out/extract.xls"/> -->   
        <DeleteFile file="data-out/extract.xls"/>
         <DeleteFile file="seq/temp_seq.seq"/>
         <DeleteFile file="seq/time_seq.seq"/>
    </FunctionalTest>
	
</TestScenario>
