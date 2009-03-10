<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="simple-examples" description="Engine simple examples" useJMX="true">    


 
<FunctionalTest ident="AggregateSorted" graphFile="graph/graphAggregateSorted.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/orders.aggregated.AggregateSorted"/>
	 <Wildcard>
     	<DeleteFile file="data-out/orders.aggregate"/>
     </Wildcard>
</FunctionalTest>


<FunctionalTest ident="AggregateUnsorted" graphFile="graph/graphAggregateUnsorted.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/order.aggregated.AggregateUnsorted"/>	                                                                    
	 <Wildcard>
	     <DeleteFile file="data-out/orders.aggregated"/>
	 </Wildcard>
</FunctionalTest>


<FunctionalTest ident="AspellLookup" graphFile="graph/graphAspellLookup.grf">
	 <FlatFile outputFile="data-out/street-lookup.dat" supposedFile="supposed-out/street-lookup.AspellLookup.dat"/>
     <Wildcard>
<DeleteFile file="data-out/street-lookup.dat"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="CloverData" graphFile="graph/graphCloverData.grf">
	 <FlatFile outputFile="data-out/strucured_customers.txt" supposedFile="supposed-out/strucured_customers.CloverData.txt"/>
     <Wildcard>
<DeleteFile file="data-out/strucured_customers.txt"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="CompressedByteTest" graphFile="graph/graphCompressedByteTest.grf">

</FunctionalTest>


<FunctionalTest ident="DataPolicy" graphFile="graph/graphDataPolicy.grf">
	 <FlatFile outputFile="data-out/correctCustomers.txt" supposedFile="supposed-out/correctCustomers.DataPolicy.txt"/>
     <Wildcard>
<DeleteFile file="data-out/correctCustomers.txt"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="DBFJoin" graphFile="graph/graphDBFJoin.grf">
	<FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoin.out"/>
	<FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoin.out"/>
     <Wildcard>
<DeleteFile file="data-out/joinedDBForders.out"/>
<DeleteFile file="data-out/joinedDBFordersNA.out"/>
</Wildcard>
</FunctionalTest>

<FunctionalTest ident="DBFJoinTL" graphFile="graph/graphDBFJoinTL.grf">
	 <FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoinTL.out"/>
	 <FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoinTL.out"/>
     <Wildcard>
<DeleteFile file="data-out/joinedDBForders.out"/>
<DeleteFile file="data-out/joinedDBFordersNA.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="DBFLoad" graphFile="graph/graphDBFLoad.grf">
	 <FlatFile outputFile="data-out/Trash1_output.txt" supposedFile="supposed-out/Trash1_output.DBFLoad.txt"/>
     <Wildcard>
<DeleteFile file="data-out/Trash1_output.txt"/>
</Wildcard>
</FunctionalTest>



<FunctionalTest ident="Dedup" graphFile="graph/graphDedup.grf">
	 <FlatFile outputFile="data-out/out.txt" supposedFile="supposed-out/out.Dedup.txt"/>
	 <FlatFile outputFile="data-out/reject.txt" supposedFile="supposed-out/reject.Dedup.txt"/>
     <Wildcard>
<DeleteFile file="data-out/out.txt"/>
<DeleteFile file="data-out/reject.txt"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="DenormalizeInline" graphFile="graph/graphDenormalizeInline.grf">
	 <FlatFile outputFile="data-out/denormalized.out" supposedFile="supposed-out/denormalized.DenormalizeInline.out"/>
     <Wildcard>
<DeleteFile file="data-out/denormalized.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="DenormalizeTL" graphFile="graph/graphDenormalizeTL.grf">
	 <FlatFile outputFile="data-out/denormalizedTL.out" supposedFile="supposed-out/denormalizedTL.DenormalizeTL.out"/>
	 <FlatFile outputFile="data-out/denormalizedTLwithoutOrder.out" supposedFile="supposed-out/denormalizedTLwithoutOrder.DenormalizeTL.out"/>
     <Wildcard>
		<DeleteFile file="data-out/denormalizedTL.out"/>
		<DeleteFile file="data-out/denormalizedTLwithoutOrder.out"/>
	</Wildcard>
</FunctionalTest>


<FunctionalTest ident="ExtFilter" graphFile="graph/graphExtFilter.grf">
	 <FlatFile outputFile="data-out/memoFluent.dat" supposedFile="supposed-out/memoFluent.ExtFilter.dat"/>
	 <FlatFile outputFile="data-out/HireDateGT19931231" supposedFile="supposed-out/HireDateGT19931231.ExtFilter"/>
	 <FlatFile outputFile="data-out/HireDateLT19931231" supposedFile="supposed-out/HireDateLT19931231.ExtFilter"/>
	 <FlatFile outputFile="data-out/fluentAndHireDateLT19931231" supposedFile="supposed-out/fluentAndHireDateLT19931231.ExtFilter"/>	
     <Wildcard>
<DeleteFile file="data-out/HireDateGT19931231"/>
<DeleteFile file="data-out/HireDateLT19931231"/>
<DeleteFile file="data-out/fluentAndHireDateLT19931231"/>
<DeleteFile file="data-out/memoFluent.dat"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="ExtFilter2" graphFile="graph/graphExtFilter2.grf">
	 <FlatFile outputFile="data-out/employees.filtered.dat" supposedFile="supposed-out/employees.filtered.ExtFilter2.dat"/>
      <Wildcard>
<DeleteFile file="data-out/employees.filtered.dat"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="GenerateData" graphFile="graph/graphGenerateData.grf">	 
      <Wildcard>
<DeleteFile file="data-out/orders.fix"/>
<DeleteFile file="seq/seq.seq"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="IntersectData" graphFile="graph/graphIntersectData.grf">
	 <FlatFile outputFile="data-out/intersect_1_2_data.out" supposedFile="supposed-out/intersect_1_2_data.IntersectData.out"/>
	 <FlatFile outputFile="data-out/intersect_1_data.out" supposedFile="supposed-out/intersect_1_data.IntersectData.out"/>
	 <FlatFile outputFile="data-out/intersect_2_data.out" supposedFile="supposed-out/intersect_2_data.IntersectData.out"/>
     <Wildcard>
		<DeleteFile file="data-out/intersect_1_2_data.out"/>
		<DeleteFile file="data-out/intersect_1_data.out"/>
		<DeleteFile file="data-out/intersect_2_data.out"/>
	</Wildcard>
</FunctionalTest>


<FunctionalTest ident="JavaExecute" graphFile="graph/graphJavaExecute.grf">
</FunctionalTest>



<FunctionalTest ident="JoinData" graphFile="graph/graphJoinData.grf">
	 <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.JoinData.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="JoinHash" graphFile="graph/graphJoinHash.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHash.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="JoinHashInline" graphFile="graph/graphJoinHashInline.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHashInline.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="JoinHashUsingTransformLanguage" graphFile="graph/graphJoinHashUsingTransformLanguage.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHashUsingTransformLanguage.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="JoinMergeInline" graphFile="graph/graphJoinMergeInline.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.JoinMergeInline.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="LookupReader" graphFile="graph/graphLookupReader.grf">
	 <FlatFile outputFile="data-out/employees.out" supposedFile="supposed-out//employees.LookupReader.out"/>
     <Wildcard>
<DeleteFile file="data-out/employees.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="MergeData" graphFile="graph/graphMergeData.grf">
	 <FlatFile outputFile="data-out/orders.merged" supposedFile="supposed-out//orders.merged.MergeData"/>
     <Wildcard>
<DeleteFile file="data-out/orders.merged"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="NormalizeInline" graphFile="graph/graphNormalizeInline.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeInline.out"/>
     <Wildcard>
<DeleteFile file="data-out/normalized.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="NormalizeTL" graphFile="graph/graphNormalizeTL.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeTL.out"/>
     <Wildcard>
<DeleteFile file="data-out/normalized.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="OrdersReformat" graphFile="graph/graphOrdersReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformat.out"/>
     <Wildcard>
<DeleteFile file="data-out/orders.dat.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="OrdersReformatExternTransform" graphFile="graph/graphOrdersReformatExternTransform.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformatExternTransform.out"/>
     <Wildcard>
<DeleteFile file="data-out/orders.dat.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="OrdersReformatInline" graphFile="graph/graphOrdersReformatInline.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformatInline.out"/>
     <Wildcard>
<DeleteFile file="data-out/orders.dat.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="OrdersTLReformat" graphFile="graph/graphOrdersTLReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersTLReformat.out"/>
     <Wildcard>
<DeleteFile file="data-out/orders.dat.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="ParametrizedLookup" graphFile="graph/graphParametrizedLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.ParametrizedLookup.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash.out"/>
</Wildcard>
</FunctionalTest>

<FunctionalTest ident="Partition" graphFile="graph/graphPartition.grf">
	 <FlatFile outputFile="data-out/smallIdOlder.txt" supposedFile="supposed-out//smallIdOlder.Partition.txt"/>
	 <FlatFile outputFile="data-out/bigIdOlder.txt" supposedFile="supposed-out//bigIdOlder.Partition.txt"/>
	 <FlatFile outputFile="data-out/smallIdYounger.txt" supposedFile="supposed-out//smallIdYounger.Partition.txt"/>
	 <FlatFile outputFile="data-out/bigIdYounger.txt" supposedFile="supposed-out//bigIdYounger.Partition.txt"/>
	 <FlatFile outputFile="data-out/rejectedId.txt" supposedFile="supposed-out//rejectedId.Partition.txt"/>
     <Wildcard>
<DeleteFile file="data-out/smallIdOlder.txt"/>
<DeleteFile file="data-out/bigIdOlder.txt"/>
<DeleteFile file="data-out/smallIdYounger.txt"/>
<DeleteFile file="data-out/bigIdYounger.txt"/>
<DeleteFile file="data-out/rejectedId.txt"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="PersistentLookup" graphFile="graph/graphPersistentLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash2.out" supposedFile="supposed-out/joined_data_hash2.PersistentLookup.out"/>
	 <FlatFile outputFile="data-out/joined_data_hash3.out" supposedFile="supposed-out/joined_data_hash3.PersistentLookup.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash2.out"/>
<DeleteFile file="data-out/joined_data_hash3.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="PersistentLookup2" graphFile="graph/graphPersistentLookup2.grf">
	 <FlatFile outputFile="data-out/employees.in" supposedFile="supposed-out/employees.PersistentLookup2.in"/>
	 <FlatFile outputFile="data-out/employees.out" supposedFile="supposed-out/employees.PersistentLookup2.out"/>	 
     <Wildcard>
<DeleteFile file="data-out/employees.in"/>
<DeleteFile file="data-out/employees.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="PhasesDemo" graphFile="graph/graphPhasesDemo.grf">
	 <FlatFile outputFile="data-out/orders.phases.merged" supposedFile="supposed-out//orders.phases.merged.PhasesDemo"/>
     <Wildcard>
<DeleteFile file="data-out/orders.phases.merged"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="RangeLookup" graphFile="graph/graphRangeLookup.grf">
	 <FlatFile outputFile="data-out/peopleAtTour.out" supposedFile="supposed-out//peopleAtTour.RangeLookup.out"/>
     <Wildcard>
<DeleteFile file="data-out/peopleAtTour.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="Sequence" graphFile="graph/graphSequence.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.Sequence.dat.out"/>
     <Wildcard>
<DeleteFile file="data-out/orders.dat.out"/>
<DeleteFile file="seq/sequence.dat"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="SequenceChecker" graphFile="graph/graphSequenceChecker.grf">
</FunctionalTest>


<FunctionalTest ident="SimpleCopy" graphFile="graph/graphSimpleCopy.grf">
	 <FlatFile outputFile="data-out/employees.copy.dat" supposedFile="supposed-out/employees.copy.SimpleCopy.dat"/>
	 <FlatFile outputFile="data-out/Trash1_output.txt" supposedFile="supposed-out/Trash1_output.SimpleCopy.txt"/>
     <Wildcard>
		<DeleteFile file="data-out/employees.copy.dat"/>
		<DeleteFile file="data-out/Trash1_output.txt"/>
	</Wildcard>
</FunctionalTest>

<FunctionalTest ident="SimpleCopyEmbeddedMetadata" graphFile="graph/graphSimpleCopyEmbeddedMetadata.grf">
</FunctionalTest>


<FunctionalTest ident="SimpleCopyLocale" graphFile="graph/graphSimpleCopyLocale.grf">
	 <FlatFile outputFile="data-out/employees_locale.dat" supposedFile="supposed-out//employees_locale.SimpleCopyLocale.dat"/>
     <Wildcard>
<DeleteFile file="data-out/employees_locale.dat"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="SimpleLookup" graphFile="graph/graphSimpleLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.SimpleLookup.out"/>
     <Wildcard>
<DeleteFile file="data-out/joined_data_hash.out"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="SortData" graphFile="graph/graphSortData.grf">
	 <FlatFile outputFile="data-out/orders.sorted" supposedFile="supposed-out//orders.sorted.SortData"/>
     <Wildcard>
<DeleteFile file="data-out/orders.sorted"/>
</Wildcard>
</FunctionalTest>


<FunctionalTest ident="SortUniversal" graphFile="graph/graphSortUniversal.grf">
	 <FlatFile outputFile="data-out/customers.sorted" supposedFile="supposed-out//customers.sorted.SortUniversal"/>
     <Wildcard>
<DeleteFile file="data-out/customers.sorted"/>
</Wildcard>
</FunctionalTest>

<FunctionalTest ident="SortWithinGroups" graphFile="graph/graphSortWithinGroups.grf">
	 <FlatFile outputFile="data-out/friends-country-town-name.dat" supposedFile="supposed-out//friends-country-town-name.SortWithinGroups.dat"/>
	 <FlatFile outputFile="data-out/friends-country-town-age.dat" supposedFile="supposed-out//friends-country-town-age.SortWithinGroups.dat"/>
     <Wildcard>
<DeleteFile file="data-out/friends-country-town-name.dat"/>
<DeleteFile file="data-out/friends-country-town-age.dat"/>
</Wildcard>
</FunctionalTest>

<FunctionalTest ident="ViewData" graphFile="graph/graphViewData.grf">
	 <FlatFile outputFile="data-out/data.out" supposedFile="supposed-out//data.ViewData.out"/>
     <Wildcard>
<DeleteFile file="data-out/data.out"/>
</Wildcard>
</FunctionalTest>

<FunctionalTest ident="XLSReadWrite" graphFile="graph/graphXLSReadWrite.grf">
	 <FlatFile outputFile="data-out/ordersByCountry.xls" supposedFile="supposed-out//ordersByCountry.XLSReadWrite.xls"/>
     <Wildcard>
<DeleteFile file="data-out/ordersByCountry.xls"/>
</Wildcard>
</FunctionalTest>

<FunctionalTest ident="XPathReader" graphFile="graph/graphXPathReader.grf">
	 <FlatFile outputFile="data-out/XMLoutputCHILD.txt" supposedFile="supposed-out//XMLoutputCHILD.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputCUSTOM.txt" supposedFile="supposed-out//XMLoutputCUSTOM.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputEMPL.txt" supposedFile="supposed-out//XMLoutputEMPL.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputPROJ.txt" supposedFile="supposed-out//XMLoutputPROJ.XPathReader.txt"/>	 
	 <FlatFile outputFile="data-out/XMLoutputBENEF.txt" supposedFile="supposed-out//XMLoutputBENEF.XPathReader.txt"/>	 
     <Wildcard>
		<DeleteFile file="data-out/XMLoutputCHILD.txt"/>
		<DeleteFile file="data-out/XMLoutputCUSTOM.txt"/>
		<DeleteFile file="data-out/XMLoutputEMPL.txt"/>
		<DeleteFile file="data-out/XMLoutputPROJ.txt"/>
		<DeleteFile file="data-out/XMLoutputBENEF.txt"/>
		<DeleteFile file="seq/xpathsequence.seq"/>
	</Wildcard>
</FunctionalTest>

<FunctionalTest ident="XMLExtract" graphFile="graph/graphXMLExtract.grf">
	 <FlatFile outputFile="data-out/XMLoutputCHILD.txt" supposedFile="supposed-out//XMLoutputCHILD.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputCUSTOM.txt" supposedFile="supposed-out//XMLoutputCUSTOM.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputEMPL.txt" supposedFile="supposed-out//XMLoutputEMPL.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputPROJ.txt" supposedFile="supposed-out//XMLoutputPROJ.XMLExtract.txt"/>	 
	 <FlatFile outputFile="data-out/XMLoutputBENEF.txt" supposedFile="supposed-out//XMLoutputBENEF.XMLExtract.txt"/>	 
     <Wildcard>
		<DeleteFile file="data-out/XMLoutputCHILD.txt"/>
		<DeleteFile file="data-out/XMLoutputCUSTOM.txt"/>
		<DeleteFile file="data-out/XMLoutputEMPL.txt"/>
		<DeleteFile file="data-out/XMLoutputPROJ.txt"/>
		<DeleteFile file="data-out/XMLoutputBENEF.txt"/>
		<DeleteFile file="seq/seqkey.seq"/>
	</Wildcard>
</FunctionalTest>

<FunctionalTest ident="XmlWriter" graphFile="graph/graphXmlWriter.grf">
     <Wildcard>
		<DeleteFile file="data-out/xmlOut_00.zip"/>
		<DeleteFile file="data-out/xmlOut_01.zip"/>
		<DeleteFile file="data-out/xmlOut_02.zip"/>
		<DeleteFile file="data-out/xmlOut_03.zip"/>
		<DeleteFile file="data-out/xmlOut_04.zip"/>
		<DeleteFile file="seq/seqkey.seq"/>
	</Wildcard>
</FunctionalTest>


</TestScenario>
