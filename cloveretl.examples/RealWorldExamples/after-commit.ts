<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="RealWorldExamples" description="RealWorldExamples distributed with Designer" useJMX="true">
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />
	
	<FunctionalTest ident="DataSelectionAdvanced" graphFile="graph/DataSelectionAdvanced.grf" excludedContainers="websphere7">
		<FlatFile outputFile="data-out/NumberOfCustomers.out" supposedFile="supposed-out/NumberOfCustomers.out"/>
		<FlatFile outputFile="data-tmp/had_duplicate_records.txt" supposedFile="supposed-out/had_duplicate_records.txt"/>
		<RegEx expression="# 2 *\|Argentina *\|16" occurences="1"/>
		<RegEx expression="# 4 *\|Spain *\|23" occurences="1"/>
		<RegEx expression="# 1 *\|Brazil *\|83" occurences="1"/>
		<RegEx expression="# 3 *\|Venezuela *\|46" occurences="1"/>
	</FunctionalTest>
	
	<!-- excluded on websphere85 because of CLO-3267 -->
	<FunctionalTest ident="DebuggingGraph" graphFile="graph/DebuggingGraph.grf" assertion="false" excludedContainers="websphere85">
		<ExcludeRegEx ident="error"/>
		<RegEx expression="Exception raised by user: O is not a valid gender\. Go to record No\. 5820 and replace with \'M\' or \'F\'\." occurences="4"/>
	</FunctionalTest>
	
	<FunctionalTest ident="ExecutingExternal" graphFile="graph/ExecutingExternal.grf" />
	
	<FunctionalTest ident="FiltersLookups" graphFile="graph/FiltersLookups.grf">
		<FlatFile outputFile="data-out/orders_late.txt" supposedFile="supposed-out/orders_late.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="JoiningAggregating" graphFile="graph/JoiningAggregating.grf">
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

	<FunctionalTest ident="ValidateData" graphFile="graph/ValidateData.grf">
		<FlatFile outputFile="data-out/invalid_contacts.csv" supposedFile="supposed-out/invalid_contacts.csv"/>
	</FunctionalTest>
	
	<FunctionalTest ident="Twitter" graphFile="graph/Twitter.grf">
		<Property name="OAUTH_API_KEY" value="r0h8VjKBinQUaEhAksjEjUw3v" />
		<Property name="OAUTH_API_KEY_SECRET" value="KrNSU4lT9Po5Lam4X7adf57CXFFpZc8fPljTo0OrhcbattKWOU" />
		<Property name="OAUTH_ACCESS_TOKEN" value="2492653806-5XIsyYAfv289fUcN3vIsCIbXAgsgKqfM2KnXawU" />
		<Property name="OAUTH_ACCESS_TOKEN_SECRET" value="Mpnb4Vbwt2BH9XcNL4EogKct7huGTDgvtDRZeheQeDSKB" />
	</FunctionalTest>

</TestScenario>
