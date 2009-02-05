<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="simple-examples" description="Engine simple examples" useJMX="true">    


 
<FunctionalTest ident="AggregateSorted" graphFile="graph/graphAggregateSorted.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/orders.aggregated.AggregateSorted"/>
</FunctionalTest>


<FunctionalTest ident="AggregateUnsorted" graphFile="graph/graphAggregateUnsorted.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/order.aggregated.AggregateUnsorted"/>	                                                                    
</FunctionalTest>


<FunctionalTest ident="AspellLookup" graphFile="graph/graphAspellLookup.grf">
	 <FlatFile outputFile="data-out/street-lookup.dat" supposedFile="supposed-out/street-lookup.AspellLookup.dat"/>
</FunctionalTest>


<FunctionalTest ident="CloverData" graphFile="graph/graphCloverData.grf">
	 <FlatFile outputFile="data-out/strucured_customers.txt" supposedFile="supposed-out/strucured_customers.CloverData.txt"/>
</FunctionalTest>


<FunctionalTest ident="CompressedByteTest" graphFile="graph/graphCompressedByteTest.grf">

</FunctionalTest>


<FunctionalTest ident="DataPolicy" graphFile="graph/graphDataPolicy.grf">
	 <FlatFile outputFile="data-out/correctCustomers.txt" supposedFile="supposed-out/correctCustomers.DataPolicy.txt"/>
</FunctionalTest>


<FunctionalTest ident="DBFJoin" graphFile="graph/graphDBFJoin.grf">
	<FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoin.out"/>
	<FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoin.out"/>
</FunctionalTest>

<FunctionalTest ident="DBFJoinTL" graphFile="graph/graphDBFJoinTL.grf">
	 <FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoinTL.out"/>
	 <FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoinTL.out"/>
</FunctionalTest>


<FunctionalTest ident="DBFLoad" graphFile="graph/graphDBFLoad.grf">

</FunctionalTest>



<FunctionalTest ident="Dedup" graphFile="graph/graphDedup.grf">
	 <FlatFile outputFile="data-out/out.txt" supposedFile="supposed-out/out.Dedup.txt"/>
	 <FlatFile outputFile="data-out/reject.txt" supposedFile="supposed-out/reject.Dedup.txt"/>
</FunctionalTest>


<FunctionalTest ident="DenormalizeInline" graphFile="graph/graphDenormalizeInline.grf">
	 <FlatFile outputFile="data-out/denormalized.out" supposedFile="supposed-out/denormalized.DenormalizeInline.out"/>
</FunctionalTest>


<FunctionalTest ident="DenormalizeTL" graphFile="graph/graphDenormalizeTL.grf">
	 <FlatFile outputFile="data-out/denormalizedTL.out" supposedFile="supposed-out/denormalizedTL.DenormalizeTL.out"/>
</FunctionalTest>


<FunctionalTest ident="ExtFilter" graphFile="graph/graphExtFilter.grf">
	 <FlatFile outputFile="data-out/memoFluent.dat" supposedFile="supposed-out/memoFluent.ExtFilter.dat"/>
	 <FlatFile outputFile="data-out/HireDateGT19931231" supposedFile="supposed-out/HireDateGT19931231.ExtFilter"/>
	 <FlatFile outputFile="data-out/HireDateLT19931231" supposedFile="supposed-out/HireDateLT19931231.ExtFilter"/>
	 <FlatFile outputFile="data-out/fluentAndHireDateLT19931231" supposedFile="supposed-out/fluentAndHireDateLT19931231.ExtFilter"/>	
</FunctionalTest>


<FunctionalTest ident="ExtFilter2" graphFile="graph/graphExtFilter2.grf">
	 <FlatFile outputFile="data-out/employees.filtered_1.dat" supposedFile="supposed-out/employees.filtered_1.ExtFilter2.dat"/>
</FunctionalTest>


<FunctionalTest ident="GenerateData" graphFile="graph/graphGenerateData.grf">	 
</FunctionalTest>


<FunctionalTest ident="IntersectData" graphFile="graph/graphIntersectData.grf">
	 <FlatFile outputFile="data-out/intersect_1_2_data.out" supposedFile="supposed-out/intersect_1_2_data.IntersectData.out"/>
	 <FlatFile outputFile="data-out/intersect_1_data.out" supposedFile="supposed-out/intersect_1_data.IntersectData.out"/>
	 <FlatFile outputFile="data-out/intersect_2_data.out" supposedFile="supposed-out/intersect_2_data.IntersectData.out"/>
</FunctionalTest>


<FunctionalTest ident="JavaExecute" graphFile="graph/graphJavaExecute.grf">
</FunctionalTest>



<FunctionalTest ident="JoinData" graphFile="graph/graphJoinData.grf">
	 <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.JoinData.out"/>
</FunctionalTest>


<FunctionalTest ident="JoinHash" graphFile="graph/graphJoinHash.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHash.out"/>
</FunctionalTest>


<FunctionalTest ident="JoinHashInline" graphFile="graph/graphJoinHashInline.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHashInline.out"/>
</FunctionalTest>


<FunctionalTest ident="JoinHashUsingTransformLanguage" graphFile="graph/graphJoinHashUsingTransformLanguage.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHashUsingTransformLanguage.out"/>
</FunctionalTest>




<FunctionalTest ident="JoinMergeInline" graphFile="graph/graphJoinMergeInline.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.JoinMergeInline.out"/>
</FunctionalTest>


<FunctionalTest ident="LookupReader" graphFile="graph/graphLookupReader.grf">
	 <FlatFile outputFile="data-out/employees.out" supposedFile="supposed-out//employees.LookupReader.out"/>
</FunctionalTest>


<FunctionalTest ident="MergeData" graphFile="graph/graphMergeData.grf">
	 <FlatFile outputFile="data-out/orders.merged" supposedFile="supposed-out//orders.merged.MergeData"/>
</FunctionalTest>


<FunctionalTest ident="NormalizeInline" graphFile="graph/graphNormalizeInline.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeInline.out"/>
</FunctionalTest>


<FunctionalTest ident="NormalizeTL" graphFile="graph/graphNormalizeTL.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeTL.out"/>
</FunctionalTest>


<FunctionalTest ident="OrdersReformat" graphFile="graph/graphOrdersReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformat.out"/>
</FunctionalTest>


<FunctionalTest ident="OrdersReformatExternTransform" graphFile="graph/graphOrdersReformatExternTransform.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformatExternTransform.out"/>
</FunctionalTest>


<FunctionalTest ident="OrdersReformatInline" graphFile="graph/graphOrdersReformatInline.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformatInline.out"/>
</FunctionalTest>


<FunctionalTest ident="OrdersTLReformat" graphFile="graph/graphOrdersTLReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersTLReformat.out"/>
</FunctionalTest>


<FunctionalTest ident="ParametrizedLookup" graphFile="graph/graphParametrizedLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.ParametrizedLookup.out"/>
</FunctionalTest>

<FunctionalTest ident="Partition" graphFile="graph/graphPartition.grf">
</FunctionalTest>


<FunctionalTest ident="PersistentLookup" graphFile="graph/graphPersistentLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash2.out" supposedFile="supposed-out//joined_data_hash2.PersistentLookup.out"/>
	 <FlatFile outputFile="data-out/joined_data_hash3.out" supposedFile="supposed-out//joined_data_hash3.PersistentLookup.out"/>
</FunctionalTest>


<FunctionalTest ident="PersistentLookup2" graphFile="graph/graphPersistentLookup2.grf">
	 <FlatFile outputFile="data-out/employees.in" supposedFile="supposed-out//employees.PersistentLookup2.in"/>
	 <FlatFile outputFile="data-out/employees.out" supposedFile="supposed-out//employees.PersistentLookup2.out"/>	 
</FunctionalTest>


<FunctionalTest ident="PhasesDemo" graphFile="graph/graphPhasesDemo.grf">
	 <FlatFile outputFile="data-out/orders.phases.merged" supposedFile="supposed-out//orders.phases.merged.PhasesDemo"/>
</FunctionalTest>


<FunctionalTest ident="RangeLookup" graphFile="graph/graphRangeLookup.grf">
	 <FlatFile outputFile="data-out/peopleAtTour.out" supposedFile="supposed-out//peopleAtTour.RangeLookup.out"/>
</FunctionalTest>


<FunctionalTest ident="Sequence" graphFile="graph/graphSequence.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.Sequence.dat.out"/>
</FunctionalTest>


<FunctionalTest ident="SequenceChecker" graphFile="graph/graphSequenceChecker.grf">
</FunctionalTest>


<FunctionalTest ident="SimpleCopy" graphFile="graph/graphSimpleCopy.grf">
</FunctionalTest>

<FunctionalTest ident="SimpleCopyEmbeddedMetadata" graphFile="graph/graphSimpleCopyEmbeddedMetadata.grf">
</FunctionalTest>


<FunctionalTest ident="SimpleCopyLocale" graphFile="graph/graphSimpleCopyLocale.grf">
	 <FlatFile outputFile="data-out/employees_locale.dat" supposedFile="supposed-out//employees_locale.SimpleCopyLocale.dat"/>
</FunctionalTest>


<FunctionalTest ident="SimpleLookup" graphFile="graph/graphSimpleLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.SimpleLookup.out"/>
</FunctionalTest>


<FunctionalTest ident="SortData" graphFile="graph/graphSortData.grf">
	 <FlatFile outputFile="data-out/orders.sorted" supposedFile="supposed-out//orders.sorted.SortData"/>
</FunctionalTest>


<FunctionalTest ident="SortUniversal" graphFile="graph/graphSortUniversal.grf">
	 <FlatFile outputFile="data-out/customers.sorted" supposedFile="supposed-out//customers.sorted.SortUniversal"/>
</FunctionalTest>

<FunctionalTest ident="SortWithinGroups" graphFile="graph/graphSortWithinGroups.grf">
	 <FlatFile outputFile="data-out/friends-country-town-name.dat" supposedFile="supposed-out//friends-country-town-name.SortWithinGroups.dat"/>
	 <FlatFile outputFile="data-out/friends-country-town-age.dat" supposedFile="supposed-out//friends-country-town-age.SortWithinGroups.dat"/>
</FunctionalTest>

<FunctionalTest ident="ViewData" graphFile="graph/graphViewData.grf">
	 <FlatFile outputFile="data-out/data.out" supposedFile="supposed-out//data.ViewData.out"/>
</FunctionalTest>

<FunctionalTest ident="XLSReadWrite" graphFile="graph/graphXLSReadWrite.grf">
	 <FlatFile outputFile="data-out/ordersByCountry.xls" supposedFile="supposed-out//ordersByCountry.XLSReadWrite.xls"/>
</FunctionalTest>

<FunctionalTest ident="XMLExtract" graphFile="graph/graphXMLExtract.grf">
	 <FlatFile outputFile="data-out/XMLoutputCHILD.txt" supposedFile="supposed-out//XMLoutputCHILD.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputCUSTOM.txt" supposedFile="supposed-out//XMLoutputCUSTOM.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputEMPL.txt" supposedFile="supposed-out//XMLoutputEMPL.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputPROJ.txt" supposedFile="supposed-out//XMLoutputPROJ.XMLExtract.txt"/>	 
</FunctionalTest>

<FunctionalTest ident="XmlWriter" graphFile="graph/graphXmlWriter.grf">
	 <FlatFile outputFile="data-out/xmlOut_00.zip" supposedFile="supposed-out//xmlOut_00.XmlWriter.zip"/>
	 <FlatFile outputFile="data-out/xmlOut_01.zip" supposedFile="supposed-out//xmlOut_01.XmlWriter.zip"/>
	 <FlatFile outputFile="data-out/xmlOut_02.zip" supposedFile="supposed-out//xmlOut_02.XmlWriter.zip"/>
	 <FlatFile outputFile="data-out/xmlOut_03.zip" supposedFile="supposed-out//xmlOut_03.XmlWriter.zip"/>	 
	 <FlatFile outputFile="data-out/xmlOut_04.zip" supposedFile="supposed-out//xmlOut_04.XmlWriter.zip"/>	 	 
</FunctionalTest>

<FunctionalTest ident="XPathReader" graphFile="graph/graphXPathReader.grf">
	 <FlatFile outputFile="data-out/XMLoutputCHILD.txt" supposedFile="supposed-out//XMLoutputCHILD.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputCUSTOM.txt" supposedFile="supposed-out//XMLoutputCUSTOM.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputEMPL.txt" supposedFile="supposed-out//XMLoutputEMPL.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputPROJ.txt" supposedFile="supposed-out//XMLoutputPROJ.XPathReader.txt"/>	 
</FunctionalTest>




</TestScenario>
