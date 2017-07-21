<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="Examples" description="Examples distributed with Designer" useJMX="true">
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />
	
	<FunctionalTest ident="DataSelectionAdvanced" graphFile="graph/DataSelectionAdvanced.grf" excludedContainers="websphere7">
		<FlatFile outputFile="data-out/NumberOfCustomers.out" supposedFile="supposed-out/NumberOfCustomers.out"/>
		<FlatFile outputFile="data-tmp/had_duplicate_records.txt" supposedFile="supposed-out/had_duplicate_records.txt"/>
		<RegEx expression="# 2 *\|Argentina *\|16" occurences="1"/>
		<RegEx expression="# 4 *\|Spain *\|23" occurences="1"/>
		<RegEx expression="# 1 *\|Brazil *\|83" occurences="1"/>
		<RegEx expression="# 3 *\|Venezuela *\|46" occurences="1"/>
	</FunctionalTest>
	
	<FunctionalTest ident="DebuggingGraph" graphFile="graph/DebuggingGraph.grf" assertion="false">
		<ExcludeRegEx ident="error"/>
		<RegEx expression="Exception raised by user: O is not a valid gender\. Go to record No\. 5820 and replace with \'M\' or \'F\'\." occurences="4"/>
	</FunctionalTest>
	
	<FunctionalTest ident="ExecutingExternal" graphFile="graph/ExecutingExternal.grf" />
	
	<FunctionalTest ident="FiltersLookups" graphFile="graph/FiltersLookups.grf">
		<FlatFile outputFile="data-out/orders_late.txt" supposedFile="supposed-out/orders_late.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="JoiningAggregating" graphFile="graph/JoiningAggregating.grf" limit="225">
		<FlatFile outputFile="data-out/customers_without_order_region1.csv" supposedFile="supposed-out/customers_without_order_region1.csv"/>
		<FlatFile outputFile="data-out/customers_without_order_region2.csv" supposedFile="supposed-out/customers_without_order_region2.csv"/>
	</FunctionalTest>
	
	<FunctionalTest ident="ParsingTextData" graphFile="graph/ParsingTextData.grf">
		<FlatFile outputFile="data-out/people.xml" supposedFile="supposed-out/people.xml"/>
		<FlatFile outputFile="data-out/people1.xml" supposedFile="supposed-out/people1.xml"/>
	</FunctionalTest>
	
	<FunctionalTest ident="ReadingComplexData" graphFile="graph/ReadingComplexData.grf" />
	
	<FunctionalTest ident="SpreadsheetReadWrite" graphFile="graph/SpreadsheetReadWrite.grf">
		<FlatFile outputFile="data-out/orders_delimited.txt" supposedFile="supposed-out/orders_delimited.txt"/>
		<FlatFile outputFile="data-out/tax_form_data.txt" supposedFile="supposed-out/tax_form_data.txt"/>
		<DeleteFile file="data-out/orders.xlsx"/>
		<DeleteFile file="data-out/sortedByTotalTax.xlsx"/>
	</FunctionalTest>
	
	<FunctionalTest ident="WebServicesHTTP" graphFile="graph/WebServicesHTTP.grf">
		<RegEx expression="Returned code for http request" occurences="0"/>
	</FunctionalTest>
	
	<FunctionalTest ident="WebServicesWSClient" graphFile="graph/WebServicesWSClient.grf" />
	
	<FunctionalTest ident="WritingTextData" graphFile="graph/WritingTextData.grf">
		<FlatFile outputFile="data-out/customers/AK.txt" supposedFile="supposed-out/AK.txt"/>
		<FlatFile outputFile="data-out/customers/WY.txt" supposedFile="supposed-out/WY.txt"/>
		<FlatFile outputFile="data-out/customers2/customers3.xml" supposedFile="supposed-out/customers3.xml"/>
	</FunctionalTest>
	
	<FunctionalTest ident="XMLProcessing" graphFile="graph/XMLProcessing.grf">
		<FlatFile outputFile="data-out/Actors.xml" supposedFile="supposed-out/Actors.xml"/>
		<FlatFile outputFile="data-out/Movies_list.html" supposedFile="supposed-out/Movies_list.html"/>
	</FunctionalTest>

	<FunctionalTest ident="JSONProcessing" graphFile="graph/JSONProcessing.grf">
		<FlatFile outputFile="data-out/Actors.json" supposedFile="supposed-out/Actors.json"/>
	</FunctionalTest>
	
	<FunctionalTest ident="_Introduction" graphFile="graph/_Introduction.grf" />
	
	<FunctionalTest ident="CreditCardFraudDetection" graphFile="graph/CreditCardFraudDetection.grf" />
	
	<!-- runs too long on cluster; fails on websphere because of different order in outputs -->
	<FunctionalTest ident="EmailValidation" graphFile="graph/EmailValidation.grf" excludedEtlEnvironment="cluster" excludedContainers="websphere7">
		<FlatFile outputFile="data-out/emails_domain_accept.txt" supposedFile="supposed-out/emails_domain_accept.txt"/>
		<FlatFile outputFile="data-out/emails_domain_reject.txt" supposedFile="supposed-out/emails_domain_reject.txt"/>
		<FlatFile outputFile="data-out/emails_smtp_accept.txt" supposedFile="supposed-out/emails_smtp_accept.txt"/>
		<FlatFile outputFile="data-out/emails_syntax_accept.txt" supposedFile="supposed-out/emails_syntax_accept.txt"/>
		<FlatFile outputFile="data-out/emails_syntax_reject.txt" supposedFile="supposed-out/emails_syntax_reject.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="OrdersLookup" graphFile="graph/OrdersLookup.grf">
		<FlatFile outputFile="data-out/orders_late.txt" supposedFile="supposed-out/orders_late.txt"/>
	</FunctionalTest>
			
	<FunctionalTest ident="MetadataWriting" graphFile="graph/MetadataWriting.grf" excludedEtlEnvironment="engine">
		<FlatFile outputFile="data-out/employees_txt.fmt" supposedFile="supposed-out/employees_txt.fmt"/>
		<FlatFile outputFile="data-out/departments_txt.fmt" supposedFile="supposed-out/departments_txt.fmt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="IssuesReport" graphFile="graph/IssuesReport.grf" excludedEtlEnvironment="engine" />
	
	<FunctionalTest ident="IssuesSearch" graphFile="graph/IssuesSearch.grf" excludedEtlEnvironment="engine" />
	
	<!-- fails on weblogic12 (CLO-5071) -->
	<FunctionalTest ident="Twitter" graphFile="graph/Twitter.grf" excludedEtlEnvironment="engine" excludedContainers="weblogic12" />

</TestScenario>
