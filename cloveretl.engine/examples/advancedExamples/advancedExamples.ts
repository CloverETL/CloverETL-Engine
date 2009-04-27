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
      	<DBTableToXMLFile outputTable="pololeti_agr_vynosy" supposedTable="pololeti_agr_vynosy" outputTableConnection="postgre_foodmart" 
      		supposedXMLFile="supposed-out/revenues.xml"/>
      	<DBTableToXMLFile outputTable="Pololeti_zakaznici_bez_vynosu" supposedTable="Pololeti_zakaznici_bez_vynosu" outputTableConnection="postgre_foodmart" 
      		supposedXMLFile="supposed-out/revenues.xml"/>
      	<DeleteTable connection="postgre_foodmart" name="pololeti_agr_vynosy"/>
      	<DeleteTable connection="postgre_foodmart" name="Pololeti_zakaznici_bez_vynosu"/>
	</FunctionalTest>

    <FunctionalTest ident="SCDType2_example1" graphFile="graph/SCDType2_example1.grf">
        <FlatFile outputFile="data-out/exchange_rates_DIM_insert.txt" supposedFile="supposed-out/exchange_rates_DIM_insert.txt"/>
        <FlatFile outputFile="data-out/exchange_rates_DIM_update.txt" supposedFile="supposed-out/exchange_rates_DIM_update.txt"/>
        <FlatFile outputFile="data-out/exchange_rates_matched.txt" supposedFile="supposed-out/exchange_rates_matched.txt"/>
        <FlatFile outputFile="data-out/onlyInRates.txt" supposedFile="supposed-out/onlyInRates.txt"/>
        <FlatFile outputFile="data-out/onlyInRatesByCode.txt" supposedFile="supposed-out/onlyInRatesByCode.txt"/>
         <DeleteFile file="seq/exchange_rates_ID.seq"/>
    </FunctionalTest>
	
    <FunctionalTest ident="SCDType2_example2" graphFile="graph/SCDType2_example2.grf">
        <FlatFile outputFile="data-out/new.txt" supposedFile="supposed-out/empty.txt"/>
        <FlatFile outputFile="data-out/NewProductsWithNewDescription.txt" supposedFile="supposed-out/NewProductsWithNewDescription.txt"/>
        <FlatFile outputFile="data-out/OriginalProductsWithNewDescription.txt" supposedFile="supposed-out/OriginalProductsWithNewDescription.txt"/>
        <FlatFile outputFile="data-out/OriginalProductsWithOriginalDescription.txt" supposedFile="supposed-out/OriginalProductsWithOriginalDescription.txt"/>
        <FlatFile outputFile="data-out/OriginalProductsWithTerminatedDescription.txt" supposedFile="supposed-out/OriginalProductsWithTerminatedDescription.txt"/>
         <DeleteFile file="seq/Product_id_seq.dat"/>
         <DeleteFile file="seq/Internal_key_seq.dat"/>
    </FunctionalTest>

    <FunctionalTest ident="SOAP_EXAMPLE" graphFile="graph/SOAP_EXAMPLE.grf">
        <FlatFile outputFile="data-out/extract.xls" supposedFile="supposed-out/extract.xls"/>
        <DeleteFile file="data-out/extract.xls"/>
         <DeleteFile file="seq/temp_seq.seq"/>
         <DeleteFile file="seq/time_seq.seq"/>
    </FunctionalTest>
	
</TestScenario>
