<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="Company Transactions Tutorial" description="Company Transactions Tutorial examples" useJMX="true">    

	<FunctionalTest ident="A01" graphFile="graph/A01_SplittingTransactions.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/Amounts.dat" supposedFile="supposed-out/A01.Amounts.dat"/>
	 	<FlatFile outputFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/Customers.dat" supposedFile="supposed-out/A01.Customers.dat"/>
	 	<FlatFile outputFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/Employees.dat" supposedFile="supposed-out/A01.Employees.dat"/>
	 	<FlatFile outputFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/Ids.dat" supposedFile="supposed-out/A01.Ids.dat"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A02" graphFile="graph/A02_CreatingXLSEmployeesWithFamily.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/EmployeesWithFamily.xls" supposedFile="supposed-out/A02.EmployeesWithFamily.xls"/>
	</FunctionalTest>

	<FunctionalTest ident="A03" graphFile="graph/A03_ConvertingCustomersFromDelimitedToFixed.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/CustomersFixed.txt" supposedFile="supposed-out/A03.CustomersFixed.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="A04" graphFile="graph/A04_SortingTransactions.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/CustomersForEmployees.txt" supposedFile="supposed-out/A04.CustomersForEmployees.txt"/>
	 	<FlatFile outputFile="data-out/CustomersForStates.txt" supposedFile="supposed-out/A04.CustomersForStates.txt"/>
	 	<FlatFile outputFile="data-out/EmployeesForCustomers.txt" supposedFile="supposed-out/A04.EmployeesForCustomers.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForCustomers.txt" supposedFile="supposed-out/A04.TransactionsForCustomers.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForEmployees.txt" supposedFile="supposed-out/A04.TransactionsForEmployees.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForStates.txt" supposedFile="supposed-out/A04.TransactionsForStates.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForStatesWithinEmployees.txt" supposedFile="supposed-out/A04.TransactionsForStatesWithinEmployees.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A05" graphFile="graph/A05_CreatingXMLEmplFamCustAm.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A06" graphFile="graph/A06_CreatingXMLCustEmplFamAm.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A07" graphFile="graph/A07_CreatingXMLAmCustEmplFam.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A08" graphFile="graph/A08_CreatingXMLTransactionsFamily.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
		<Property name="MAPPINGS" value=""/>
	</FunctionalTest>

	<FunctionalTest ident="A09" graphFile="graph/A09_XMLExtractEmplFamCustAm.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLExtractWithEmplID_WithCustID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A09.amountsXMLExtractWithEmplID_WithCustID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A09.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A09.customersXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A09.employeesXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A09.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A10" graphFile="graph/A10_XMLExtractCustEmplFamAm.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLExtractWithCustID_WithEmplID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A10.amountsXMLExtractWithCustID_WithEmplID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A10.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A10.customersXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A10.employeesXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A10.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A11" graphFile="graph/A11_XMLExtractAmCustEmplFam.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A11.amountsXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A11.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtractWithAmID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A11.customersXMLExtractWithAmID.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtractWithAmountID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A11.employeesXMLExtractWithAmountID.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A11.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A12" graphFile="graph/A12_XMLExtractTransactionsFamily.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A12.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/transactionsXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A12.transactionsXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A12.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A13" graphFile="graph/A13_XMLXPathEmplFamCustAm.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPathWithEmplID_WithCustID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A13.amountsXMLXPathWithEmplID_WithCustID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A13.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A13.customersXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A13.employeesXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A13.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A14" graphFile="graph/A14_XMLXPathCustEmplFamAm.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPathWithCustID_WithEmplID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A14.amountsXMLXPathWithCustID_WithEmplID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A14.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A14.customersXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A14.employeesXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A14.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A15" graphFile="graph/A15_XMLXPathAmCustEmplFam.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/customersXMLXPathWithAmID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A15.customersXMLXPathWithAmID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A15.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A15.amountsXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPathWithAmountID.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A15.employeesXMLXPathWithAmountID.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A15.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A16" graphFile="graph/A16_XMLXPathTransactionsFamily.grf">
		<Property name="DATATMP_DIR" value="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp"/>
	 	<FlatFile outputFile="data-out/transactionsXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A16.transactionsXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A16.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/supposed-out/A16.spousesXMLXPath.txt"/>
        <DeleteFile file="../../../cloveretl.engine.examples.CompanyTransactionsTutorial/data-tmp/EmployeesWithFamily.xls"/>
	</FunctionalTest>
	
</TestScenario>