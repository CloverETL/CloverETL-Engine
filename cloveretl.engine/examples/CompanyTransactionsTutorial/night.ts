<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="Company Transactions Tutorial" description="Company Transactions Tutorial examples" useJMX="true">    

	<FunctionalTest ident="A01" graphFile="graph/A01_SplittingTransactions.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="/data/bigfiles/tmp/Amounts.dat" supposedFile="supposed-out/A01.Amounts.dat"/>
	 	<FlatFile outputFile="/data/bigfiles/tmp/Customers.dat" supposedFile="supposed-out/A01.Customers.dat"/>
	 	<FlatFile outputFile="/data/bigfiles/tmp/Employees.dat" supposedFile="supposed-out/A01.Employees.dat"/>
	 	<FlatFile outputFile="/data/bigfiles/tmp/Ids.dat" supposedFile="supposed-out/A01.Ids.dat"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A02" graphFile="graph/A02_CreatingXLSEmployeesWithFamily.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="/data/bigfiles/tmp/EmployeesWithFamily.xls" supposedFile="supposed-out/A02.EmployeesWithFamily.xls"/>
	</FunctionalTest>

	<FunctionalTest ident="A03" graphFile="graph/A03_ConvertingCustomersFromDelimitedToFixed.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="/data/bigfiles/tmp/CustomersFixed.txt" supposedFile="supposed-out/A03.CustomersFixed.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="A04" graphFile="graph/A04_SortingTransactions.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/CustomersForEmployees.txt" supposedFile="supposed-out/A04.CustomersForEmployees.txt"/>
	 	<FlatFile outputFile="data-out/CustomersForStates.txt" supposedFile="supposed-out/A04.CustomersForStates.txt"/>
	 	<FlatFile outputFile="data-out/EmployeesForCustomers.txt" supposedFile="supposed-out/A04.EmployeesForCustomers.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForCustomers.txt" supposedFile="supposed-out/A04.TransactionsForCustomers.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForEmployees.txt" supposedFile="supposed-out/A04.TransactionsForEmployees.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForStates.txt" supposedFile="supposed-out/A04.TransactionsForStates.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForStatesWithinEmployees.txt" supposedFile="supposed-out/A04.TransactionsForStatesWithinEmployees.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A05" graphFile="graph/A05_CreatingXMLEmplFamCustAm.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A06" graphFile="graph/A06_CreatingXMLCustEmplFamAm.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A07" graphFile="graph/A07_CreatingXMLAmCustEmplFam.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A08" graphFile="graph/A08_CreatingXMLTransactionsFamily.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A09" graphFile="graph/A09_XMLExtractEmplFamCustAm.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLExtractWithEmplID_WithCustID.txt" supposedFile="/data/bigfiles/supposed-out-night/A09.amountsXMLExtractWithEmplID_WithCustID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A09.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A09.customersXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A09.employeesXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A09.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A10" graphFile="graph/A10_XMLExtractCustEmplFamAm.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLExtractWithCustID_WithEmplID.txt" supposedFile="/data/bigfiles/supposed-out-night/A10.amountsXMLExtractWithCustID_WithEmplID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A10.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A10.customersXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A10.employeesXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A10.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A11" graphFile="graph/A11_XMLExtractAmCustEmplFam.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A11.amountsXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A11.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtractWithAmID.txt" supposedFile="/data/bigfiles/supposed-out-night/A11.customersXMLExtractWithAmID.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtractWithAmountID.txt" supposedFile="/data/bigfiles/supposed-out-night/A11.employeesXMLExtractWithAmountID.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A11.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A12" graphFile="graph/A12_XMLExtractTransactionsFamily.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A12.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/transactionsXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A12.transactionsXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="/data/bigfiles/supposed-out-night/A12.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A13" graphFile="graph/A13_XMLXPathEmplFamCustAm.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPathWithEmplID_WithCustID.txt" supposedFile="/data/bigfiles/supposed-out-night/A13.amountsXMLXPathWithEmplID_WithCustID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A13.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A13.customersXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A13.employeesXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A13.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A14" graphFile="graph/A14_XMLXPathCustEmplFamAm.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPathWithCustID_WithEmplID.txt" supposedFile="/data/bigfiles/supposed-out-night/A14.amountsXMLXPathWithCustID_WithEmplID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A14.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A14.customersXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A14.employeesXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A14.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A15" graphFile="graph/A15_XMLXPathAmCustEmplFam.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/customersXMLXPathWithAmID.txt" supposedFile="/data/bigfiles/supposed-out-night/A15.customersXMLXPathWithAmID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A15.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A15.amountsXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPathWithAmountID.txt" supposedFile="/data/bigfiles/supposed-out-night/A15.employeesXMLXPathWithAmountID.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A15.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A16" graphFile="graph/A16_XMLXPathTransactionsFamily.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
	 	<FlatFile outputFile="data-out/transactionsXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A16.transactionsXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A16.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="/data/bigfiles/supposed-out-night/A16.spousesXMLXPath.txt"/>
        <DeleteFile file="/data/bigfiles/tmp/EmployeesWithFamily.xls"/>
	</FunctionalTest>
	
	<!--
	<FunctionalTest ident="allGraphs" graphFile="graph/runAllGraphs.grf">
		<Property name="DATATMP_DIR" value="/data/bigfiles/tmp"/>
		<RegEx expression="Some graph\(s\) finished with error" occurences="0"/>
		<RegEx expression="Processing finished successfully" occurences="16"/>
        <DeleteFile file="/data/bigfiles/tmp/*"/>
        <DeleteFile file="data-out/*"/>
	</FunctionalTest>
	-->
</TestScenario>