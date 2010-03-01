<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="Company Transactions Tutorial" description="Engine simple examples" useJMX="true">    

	<FunctionalTest ident="A01" graphFile="graph/A01_SplittingTransactions.grf">
	 	<FlatFile outputFile="data-tmp/Amounts.dat" supposedFile="supposed-out/A01.Amounts.dat"/>
	 	<FlatFile outputFile="data-tmp/Customers.dat" supposedFile="supposed-out/A01.Customers.dat"/>
	 	<FlatFile outputFile="data-tmp/Employees.dat" supposedFile="supposed-out/A01.Employees.dat"/>
	 	<FlatFile outputFile="data-tmp/Ids.dat" supposedFile="supposed-out/A01.Ids.dat"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A02" graphFile="graph/A02_CreatingXLSEmployeesWithFamily.grf">
	 	<XlsFile outputFile="data-tmp/EmployeesWithFamily.xls" supposedFile="supposed-out/A02.EmployeesWithFamily.xls"/>
	</FunctionalTest>

	<FunctionalTest ident="A03" graphFile="graph/A03_ConvertingCustomersFromDelimitedToFixed.grf">
	 	<FlatFile outputFile="data-tmp/CustomersFixed.txt" supposedFile="supposed-out/A03.CustomersFixed.txt"/>
	</FunctionalTest>

	<FunctionalTest ident="A04" graphFile="graph/A04_SortingTransactions.grf">
	 	<FlatFile outputFile="data-out/CustomersForEmployees.txt" supposedFile="supposed-out/A04.CustomersForEmployees.txt"/>
	 	<FlatFile outputFile="data-out/CustomersForStates.txt" supposedFile="supposed-out/A04.CustomersForStates.txt"/>
	 	<FlatFile outputFile="data-out/EmployeesForCustomers.txt" supposedFile="supposed-out/A04.EmployeesForCustomers.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForCustomers.txt" supposedFile="supposed-out/A04.TransactionsForCustomers.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForEmployees.txt" supposedFile="supposed-out/A04.TransactionsForEmployees.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForStates.txt" supposedFile="supposed-out/A04.TransactionsForStates.txt"/>
	 	<FlatFile outputFile="data-out/TransactionsForStatesWithinEmployees.txt" supposedFile="supposed-out/A04.TransactionsForStatesWithinEmployees.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A05" graphFile="graph/A05_CreatingXMLEmplFamCustAm.grf">
<!--	 	<FlatFile outputFile="data-tmp/EmplFamCustAm000.xml" supposedFile="supposed-out/A05.EmplFamCustAm000.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm001.xml" supposedFile="supposed-out/A05.EmplFamCustAm001.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm002.xml" supposedFile="supposed-out/A05.EmplFamCustAm002.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm003.xml" supposedFile="supposed-out/A05.EmplFamCustAm003.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm004.xml" supposedFile="supposed-out/A05.EmplFamCustAm004.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm005.xml" supposedFile="supposed-out/A05.EmplFamCustAm005.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm006.xml" supposedFile="supposed-out/A05.EmplFamCustAm006.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm007.xml" supposedFile="supposed-out/A05.EmplFamCustAm007.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm008.xml" supposedFile="supposed-out/A05.EmplFamCustAm008.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm009.xml" supposedFile="supposed-out/A05.EmplFamCustAm009.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm010.xml" supposedFile="supposed-out/A05.EmplFamCustAm010.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm011.xml" supposedFile="supposed-out/A05.EmplFamCustAm011.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm012.xml" supposedFile="supposed-out/A05.EmplFamCustAm012.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm013.xml" supposedFile="supposed-out/A05.EmplFamCustAm013.xml"/>
	 	<FlatFile outputFile="data-tmp/EmplFamCustAm014.xml" supposedFile="supposed-out/A05.EmplFamCustAm014.xml"/>
-->
	</FunctionalTest>

	<FunctionalTest ident="A06" graphFile="graph/A06_CreatingXMLCustEmplFamAm.grf">
<!--	 	<FlatFile outputFile="data-tmp/CustEmplFamAm000.xml" supposedFile="supposed-out/A06.CustEmplFamAm000.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm001.xml" supposedFile="supposed-out/A06.CustEmplFamAm001.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm002.xml" supposedFile="supposed-out/A06.CustEmplFamAm002.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm003.xml" supposedFile="supposed-out/A06.CustEmplFamAm003.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm004.xml" supposedFile="supposed-out/A06.CustEmplFamAm004.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm005.xml" supposedFile="supposed-out/A06.CustEmplFamAm005.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm006.xml" supposedFile="supposed-out/A06.CustEmplFamAm006.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm007.xml" supposedFile="supposed-out/A06.CustEmplFamAm007.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm008.xml" supposedFile="supposed-out/A06.CustEmplFamAm008.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm009.xml" supposedFile="supposed-out/A06.CustEmplFamAm009.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm010.xml" supposedFile="supposed-out/A06.CustEmplFamAm010.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm011.xml" supposedFile="supposed-out/A06.CustEmplFamAm011.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm012.xml" supposedFile="supposed-out/A06.CustEmplFamAm012.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm013.xml" supposedFile="supposed-out/A06.CustEmplFamAm013.xml"/>
	 	<FlatFile outputFile="data-tmp/CustEmplFamAm014.xml" supposedFile="supposed-out/A06.CustEmplFamAm014.xml"/>
-->
	</FunctionalTest>

	<FunctionalTest ident="A07" graphFile="graph/A07_CreatingXMLAmCustEmplFam.grf">
<!--	 	<FlatFile outputFile="data-tmp/AmCustEmplFam000.xml" supposedFile="supposed-out/A07.AmCustEmplFam000.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam001.xml" supposedFile="supposed-out/A07.AmCustEmplFam001.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam002.xml" supposedFile="supposed-out/A07.AmCustEmplFam002.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam003.xml" supposedFile="supposed-out/A07.AmCustEmplFam003.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam004.xml" supposedFile="supposed-out/A07.AmCustEmplFam004.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam005.xml" supposedFile="supposed-out/A07.AmCustEmplFam005.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam006.xml" supposedFile="supposed-out/A07.AmCustEmplFam006.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam007.xml" supposedFile="supposed-out/A07.AmCustEmplFam007.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam008.xml" supposedFile="supposed-out/A07.AmCustEmplFam008.xml"/>
	 	<FlatFile outputFile="data-tmp/AmCustEmplFam009.xml" supposedFile="supposed-out/A07.AmCustEmplFam009.xml"/>
-->
	</FunctionalTest>

	<FunctionalTest ident="A08" graphFile="graph/A08_CreatingXMLTransactionsFamily.grf">
<!--	 	<FlatFile outputFile="data-tmp/TransactionsFamily000.xml" supposedFile="supposed-out/A08.TransactionsFamily000.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily001.xml" supposedFile="supposed-out/A08.TransactionsFamily001.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily002.xml" supposedFile="supposed-out/A08.TransactionsFamily002.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily003.xml" supposedFile="supposed-out/A08.TransactionsFamily003.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily004.xml" supposedFile="supposed-out/A08.TransactionsFamily004.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily005.xml" supposedFile="supposed-out/A08.TransactionsFamily005.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily006.xml" supposedFile="supposed-out/A08.TransactionsFamily006.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily007.xml" supposedFile="supposed-out/A08.TransactionsFamily007.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily008.xml" supposedFile="supposed-out/A08.TransactionsFamily008.xml"/>
	 	<FlatFile outputFile="data-tmp/TransactionsFamily009.xml" supposedFile="supposed-out/A08.TransactionsFamily009.xml"/>
-->
	</FunctionalTest>

	<FunctionalTest ident="A09" graphFile="graph/A09_XMLExtractEmplFamCustAm.grf">
	 	<FlatFile outputFile="data-out/amountsXMLExtractWithEmplID_WithCustID.txt" supposedFile="supposed-out/A09.amountsXMLExtractWithEmplID_WithCustID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="supposed-out/A09.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtract.txt" supposedFile="supposed-out/A09.customersXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtract.txt" supposedFile="supposed-out/A09.employeesXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="supposed-out/A09.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A10" graphFile="graph/A10_XMLExtractCustEmplFamAm.grf">
	 	<FlatFile outputFile="data-out/amountsXMLExtractWithCustID_WithEmplID.txt" supposedFile="supposed-out/A10.amountsXMLExtractWithCustID_WithEmplID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="supposed-out/A10.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtract.txt" supposedFile="supposed-out/A10.customersXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtract.txt" supposedFile="supposed-out/A10.employeesXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="supposed-out/A10.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A11" graphFile="graph/A11_XMLExtractAmCustEmplFam.grf">
	 	<FlatFile outputFile="data-out/amountsXMLExtract.txt" supposedFile="supposed-out/A11.amountsXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="supposed-out/A11.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLExtractWithAmID.txt" supposedFile="supposed-out/A11.customersXMLExtractWithAmID.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLExtractWithAmountID.txt" supposedFile="supposed-out/A11.employeesXMLExtractWithAmountID.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="supposed-out/A11.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A12" graphFile="graph/A12_XMLExtractTransactionsFamily.grf">
	 	<FlatFile outputFile="data-out/childrenXMLExtract.txt" supposedFile="supposed-out/A12.childrenXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/transactionsXMLExtract.txt" supposedFile="supposed-out/A12.transactionsXMLExtract.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLExtract.txt" supposedFile="supposed-out/A12.spousesXMLExtract.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A13" graphFile="graph/A13_XMLXPathEmplFamCustAm.grf">
	 	<FlatFile outputFile="data-out/amountsXMLXPathWithEmplID_WithCustID.txt" supposedFile="supposed-out/A13.amountsXMLXPathWithEmplID_WithCustID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="supposed-out/A13.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLXPath.txt" supposedFile="supposed-out/A13.customersXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPath.txt" supposedFile="supposed-out/A13.employeesXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="supposed-out/A13.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A14" graphFile="graph/A14_XMLXPathCustEmplFamAm.grf">
	 	<FlatFile outputFile="data-out/amountsXMLXPathWithCustID_WithEmplID.txt" supposedFile="supposed-out/A14.amountsXMLXPathWithCustID_WithEmplID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="supposed-out/A14.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/customersXMLXPath.txt" supposedFile="supposed-out/A14.customersXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPath.txt" supposedFile="supposed-out/A14.employeesXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="supposed-out/A14.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A15" graphFile="graph/A15_XMLXPathAmCustEmplFam.grf">
	 	<FlatFile outputFile="data-out/customersXMLXPathWithAmID.txt" supposedFile="supposed-out/A15.customersXMLXPathWithAmID.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="supposed-out/A15.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/amountsXMLXPath.txt" supposedFile="supposed-out/A15.amountsXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/employeesXMLXPathWithAmountID.txt" supposedFile="supposed-out/A15.employeesXMLXPathWithAmountID.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="supposed-out/A15.spousesXMLXPath.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="A16" graphFile="graph/A16_XMLXPathTransactionsFamily.grf">
	 	<FlatFile outputFile="data-out/transactionsXMLXPath.txt" supposedFile="supposed-out/A16.transactionsXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/childrenXMLXPath.txt" supposedFile="supposed-out/A16.childrenXMLXPath.txt"/>
	 	<FlatFile outputFile="data-out/spousesXMLXPath.txt" supposedFile="supposed-out/A16.spousesXMLXPath.txt"/>
        <DeleteFile file="data-tmp/EmployeesWithFamily.xls"/>
	</FunctionalTest>
</TestScenario>