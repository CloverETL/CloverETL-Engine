<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="simple-examples" description="Engine simple examples" useJMX="true">    


<FunctionalTest ident="HttpConnector" graphFile="graph/graphHTTPConnector.grf">	 
     <Property name="-P:DEFAULT_INTERNAL_IO_BUFFER_SIZE" value="89152" />
	 <FlatFile outputFile="data-out/city_overview.txt" supposedFile="supposed-out/city_overview.HttpConnector.txt"/>
	 <!-- next two are disabled until the service works again (currently returns IOException) -->
	 <FlatFile outputFile="data-tmp/CzechRepublic.xml" supposedFile="supposed-out/CzechRepublic.HttpConnector.xml"/>
	 <FlatFile outputFile="data-tmp/Poland.xml" supposedFile="supposed-out/Poland.HttpConnector.xml"/>
</FunctionalTest>
 
<FunctionalTest ident="AggregateSorted" graphFile="graph/graphAggregateSorted.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/orders.aggregated.AggregateSorted"/>
	 
     	<DeleteFile file="data-out/orders.aggregate"/>
     
</FunctionalTest>


<FunctionalTest ident="AggregateUnsorted" graphFile="graph/graphAggregateUnsorted.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/order.aggregated.AggregateUnsorted"/>	                                                                    
	 
	     <DeleteFile file="data-out/orders.aggregated"/>
	 
</FunctionalTest>

	<FunctionalTest ident="ApproximativeJoin" graphFile="graph/graphApproximativeJoin.grf">
	 	  <FlatFile outputFile="data-out/conformingSubstring.txt" supposedFile="supposed-out/conformingSubstring.AproximativeJoin.txt"/>	                                                                 
	 	  <FlatFile outputFile="data-out/conformingMetaphone.txt" supposedFile="supposed-out/conformingMetaphone.AproximativeJoin.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/conformingNYSIIS.txt" supposedFile="supposed-out/conformingNYSIIS.AproximativeJoin.txt"/>	                                                                      
	 	  <FlatFile outputFile="data-out/conformingSoundex.txt" supposedFile="supposed-out/conformingSoundex.AproximativeJoin.txt"/>	                                                                 
	 	  <FlatFile outputFile="data-out/suspiciousSubstring.txt" supposedFile="supposed-out/suspiciousSubstring.AproximativeJoin.txt"/>                                                                    
	 	  <FlatFile outputFile="data-out/suspiciousMetaphone.txt" supposedFile="supposed-out/suspiciousMetaphone.AproximativeJoin.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/suspiciousNYSIIS.txt" supposedFile="supposed-out/suspiciousNYSIIS.AproximativeJoin.txt"/>	                                                                     
	 	  <FlatFile outputFile="data-out/suspiciousSoundex.txt" supposedFile="supposed-out/suspiciousSoundex.AproximativeJoin.txt"/>	                                                                  
	 	  <FlatFile outputFile="data-out/customersSoundex.out" supposedFile="supposed-out/empty.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/customersMetaphone.out" supposedFile="supposed-out/empty.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/customersSubstring.out" supposedFile="supposed-out/empty.txt"/>	                                                                    
	 	  <FlatFile outputFile="data-out/customersNYSIIS.out" supposedFile="supposed-out/empty.txt"/>	                                                                  
	 	  <FlatFile outputFile="data-out/employeeSubstring.out" supposedFile="supposed-out/employeeSubstring.AproximativeJoin.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/employeeMetaphone.out" supposedFile="supposed-out/employeeMetaphone.AproximativeJoin.out"/>	                                                                    
	 	  <FlatFile outputFile="data-out/employeeNYSIIS.out" supposedFile="supposed-out/employeeNYSIIS.AproximativeJoin.out"/>	                                                                        
	 	  <FlatFile outputFile="data-out/employeeSoundex.out" supposedFile="supposed-out/employeeSoundex.AproximativeJoin.out"/>	                                                               
	     <DeleteFile file="seq/id0.seq"/>
	     <DeleteFile file="seq/id1.seq"/>
	     <DeleteFile file="seq/id2.seq"/>
	     <DeleteFile file="seq/id3.seq"/>
	</FunctionalTest>

	<FunctionalTest ident="CheckForeignKey" graphFile="graph/graphCheckForeignKey.grf">
	 	  <FlatFile outputFile="data-out/wrongKey.out" supposedFile="supposed-out/wrongKey.CheckForeignKey.out"/>	                                                                    
		  <FlatFile outputFile="data-out/fixedKey.out" supposedFile="supposed-out/fixedKey.CheckForeignKey.out"/>
	</FunctionalTest>

<FunctionalTest ident="CloverData" graphFile="graph/graphCloverData.grf">
	 <FlatFile outputFile="data-out/strucured_customers.txt" supposedFile="supposed-out/strucured_customers.CloverData.txt"/>
     
<DeleteFile file="data-out/strucured_customers.txt"/>

</FunctionalTest>

<FunctionalTest ident="DataPolicy" graphFile="graph/graphDataPolicy.grf" assertion="false">
	 <FlatFile outputFile="data-out/correctCustomersControlled.txt" supposedFile="supposed-out/correctCustomers.DataPolicy.Controlled.txt"/>
	 <FlatFile outputFile="data-out/correctCustomersLenient.txt" supposedFile="supposed-out/correctCustomers.DataPolicy.Lenient.txt"/>
	 <FlatFile outputFile="data-out/allCustomersStrict.txt" supposedFile="supposed-out/allCustomers.DataPolicy.Strict.txt"/>
		<RegEx expression="org.jetel.exception.BadDataFormatException: Field account_num\(long\) cannot be set to value &quot;8747577056D&quot;; doesn't match the specified format &quot;&quot; with reason &quot;Incomplete parsing&quot;; in record 3, field 2" occurences="1"/>
</FunctionalTest>

<FunctionalTest ident="DBFJoin" graphFile="graph/graphDBFJoin.grf">
	<FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoin.out"/>
	<FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoin.out"/>
</FunctionalTest>

<FunctionalTest ident="DBFJoinTL" graphFile="graph/graphDBFJoinTL.grf">
	 <FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoin.out"/>
	 <FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoin.out"/>
</FunctionalTest>

<FunctionalTest ident="DBFJoinTLNew" graphFile="graph/graphDBFJoinTLNew.grf">
	 <FlatFile outputFile="data-out/joinedDBForders.out" supposedFile="supposed-out/joinedDBForders.DBFJoin.New.out"/>
	 <FlatFile outputFile="data-out/joinedDBFordersNA.out" supposedFile="supposed-out/joinedDBFordersNA.DBFJoin.New.out"/>
     
<DeleteFile file="data-out/joinedDBForders.out"/>
<DeleteFile file="data-out/joinedDBFordersNA.out"/>

</FunctionalTest>

<FunctionalTest ident="DBFLoad" graphFile="graph/graphDBFLoad.grf">
	 <FlatFile outputFile="data-out/Trash1_output.txt" supposedFile="supposed-out/Trash1_output.DBFLoad.txt"/>
</FunctionalTest>

<FunctionalTest ident="Dedup" graphFile="graph/graphDedup.grf">
	 <FlatFile outputFile="data-out/out.txt" supposedFile="supposed-out/out.Dedup.txt"/>
	 <FlatFile outputFile="data-out/reject.txt" supposedFile="supposed-out/reject.Dedup.txt"/>
     
<DeleteFile file="data-out/out.txt"/>
<DeleteFile file="data-out/reject.txt"/>

</FunctionalTest>


<FunctionalTest ident="DenormalizeInline" graphFile="graph/graphDenormalizeInline.grf">
	 <FlatFile outputFile="data-out/denormalized.out" supposedFile="supposed-out/denormalized.DenormalizeInline.out"/>
     
<DeleteFile file="data-out/denormalized.out"/>

</FunctionalTest>


<FunctionalTest ident="DenormalizeTL" graphFile="graph/graphDenormalizeTL.grf">
	 <FlatFile outputFile="data-out/denormalizedTL.out" supposedFile="supposed-out/denormalizedTL.DenormalizeTL.out"/>
	 <FlatFile outputFile="data-out/denormalizedTLwithoutOrder.out" supposedFile="supposed-out/denormalizedTLwithoutOrder.DenormalizeTL.out"/>
	<DeleteFile file="data-out/denormalizedTL.out"/>
	<DeleteFile file="data-out/denormalizedTLwithoutOrder.out"/>
</FunctionalTest>

<FunctionalTest ident="DictionaryJava" graphFile="graph/graphDictionaryJava.grf">
	 <FlatFile outputFile="data-out/smaller_half.txt" supposedFile="supposed-out/smaller_half.DictionaryJava.txt"/>
	 <FlatFile outputFile="data-out/greater_half.txt" supposedFile="supposed-out/greater_half.DictionaryJava.txt"/>
<!-- Trash output is not logged on	 
	 <RegEx expression="TRASH_MIN \|# 1    \|153" occurences="1"/>
	 <RegEx expression="TRASH_MAX \|# 1    \|9978" occurences="1"/> -->
<!-- Test environment doesn't register the logged when graph finished (issue 3510)
	 <RegEx expression="min:153" occurences="1"/>
	 <RegEx expression="median:4607.0" occurences="1"/>
	 <RegEx expression="max:9978" occurences="1"/> -->
</FunctionalTest>

<FunctionalTest ident="DictionaryTL" graphFile="graph/graphDictionaryTL.grf">
	 <FlatFile outputFile="data-out/smaller_half.txt" supposedFile="supposed-out/smaller_half.DictionaryTL.txt"/>
	 <FlatFile outputFile="data-out/greater_half.txt" supposedFile="supposed-out/greater_half.DictionaryTL.txt"/>
	 <RegEx expression="TRASH_MIN \|# 1    \|82" occurences="1"/>
	 <RegEx expression="TRASH_MAX \|# 1    \|9986" occurences="1"/> 
	 <RegEx expression="DictEntry:min:integer:\{value=82\}" occurences="1"/>
	 <RegEx expression="DictEntry:median:number:\{value=4710.5\}" occurences="1"/>
	 <RegEx expression="DictEntry:max:integer:\{value=9986\}" occurences="1"/>
</FunctionalTest>

<FunctionalTest ident="ExtFilter" graphFile="graph/graphExtFilter.grf">
	 <FlatFile outputFile="data-out/memoFluent.dat" supposedFile="supposed-out/memoFluent.ExtFilter.dat"/>
	 <FlatFile outputFile="data-out/HireDateGT19931231" supposedFile="supposed-out/HireDateGT19931231.ExtFilter"/>
	 <FlatFile outputFile="data-out/HireDateLT19931231" supposedFile="supposed-out/HireDateLT19931231.ExtFilter"/>
	 <FlatFile outputFile="data-out/fluentAndHireDateLT19931231" supposedFile="supposed-out/fluentAndHireDateLT19931231.ExtFilter"/>	
     
<DeleteFile file="data-out/HireDateGT19931231"/>
<DeleteFile file="data-out/HireDateLT19931231"/>
<DeleteFile file="data-out/fluentAndHireDateLT19931231"/>
<DeleteFile file="data-out/memoFluent.dat"/>

</FunctionalTest>


<FunctionalTest ident="ExtFilter2" graphFile="graph/graphExtFilter2.grf">
	 <FlatFile outputFile="data-out/employees.filtered.dat" supposedFile="supposed-out/employees.filtered.ExtFilter2.dat"/>
	<DeleteFile file="data-out/employees.filtered.dat"/>
</FunctionalTest>

<FunctionalTest ident="GenerateData" graphFile="graph/graphGenerateData.grf">	 
	 <FlatFile outputFile="data-out/orders.mix" supposedFile="supposed-out/orders.GenerateData.mix"/>
	<DeleteFile file="seq/seq.seq"/>
</FunctionalTest>

<FunctionalTest ident="IntersectData" graphFile="graph/graphIntersectData.grf">
	 <FlatFile outputFile="data-out/intersect_1_2_data.out" supposedFile="supposed-out/intersect_1_2_data.IntersectData.out"/>
	 <FlatFile outputFile="data-out/intersect_1_data.out" supposedFile="supposed-out/intersect_1_data.IntersectData.out"/>
	 <FlatFile outputFile="data-out/intersect_2_data.out" supposedFile="supposed-out/intersect_2_data.IntersectData.out"/>
     
		<DeleteFile file="data-out/intersect_1_2_data.out"/>
		<DeleteFile file="data-out/intersect_1_data.out"/>
		<DeleteFile file="data-out/intersect_2_data.out"/>
	
</FunctionalTest>


<FunctionalTest ident="JavaExecute" graphFile="graph/graphJavaExecute.grf">
		<RegEx expression="Basic example of Java Execute, value of testingProperty = Testing output string!!!" occurences="1"/>
</FunctionalTest>

<FunctionalTest ident="JoinData" graphFile="graph/graphJoinData.grf">
	 <FlatFile outputFile="data-out/joined_data.out" supposedFile="supposed-out/joined_data.JoinData.out"/>
     
<DeleteFile file="data-out/joined_data.out"/>

</FunctionalTest>


<FunctionalTest ident="JoinHash" graphFile="graph/graphJoinHash.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHash.out"/>
     
<DeleteFile file="data-out/joined_data_hash.out"/>

</FunctionalTest>


<FunctionalTest ident="JoinHashInline" graphFile="graph/graphJoinHashInline.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHashInline.out"/>
     
<DeleteFile file="data-out/joined_data_hash.out"/>

</FunctionalTest>

<FunctionalTest ident="JoinHashUsingTransformLanguage" graphFile="graph/graphJoinHashUsingTransformLanguage.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out/joined_data_hash.JoinHashUsingTransformLanguage.out"/>
     
<DeleteFile file="data-out/joined_data_hash.out"/>

</FunctionalTest>

<FunctionalTest ident="JoinMergeInline" graphFile="graph/graphJoinMergeInline.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.JoinMergeInline.out"/>
     
<DeleteFile file="data-out/joined_data_hash.out"/>

</FunctionalTest>


<FunctionalTest ident="LookupReader" graphFile="graph/graphLookupReader.grf">
	 <FlatFile outputFile="data-out/employees_without_duplicates.out" supposedFile="supposed-out/employees_without_duplicates.LookupReader.out"/>
	 <FlatFile outputFile="data-out/employees_with_duplicates.out" supposedFile="supposed-out/employees_with_duplicates.LookupReader.out"/>
</FunctionalTest>


<FunctionalTest ident="MergeData" graphFile="graph/graphMergeData.grf">
	 <FlatFile outputFile="data-out/orders.merged" supposedFile="supposed-out//orders.merged.MergeData"/>
     
<DeleteFile file="data-out/orders.merged"/>

</FunctionalTest>


<FunctionalTest ident="NormalizeInline" graphFile="graph/graphNormalizeInline.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeInline.out"/>
     
<DeleteFile file="data-out/normalized.out"/>

</FunctionalTest>


<FunctionalTest ident="NormalizeTL" graphFile="graph/graphNormalizeTL.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeTL.out"/>
     
<DeleteFile file="data-out/normalized.out"/>

</FunctionalTest>

<FunctionalTest ident="NormalizeTLNew" graphFile="graph/graphNormalizeTLNew.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out//normalized.NormalizeTL.out"/>
     
<DeleteFile file="data-out/normalized.out"/>

</FunctionalTest>

<FunctionalTest ident="OrdersReformat" graphFile="graph/graphOrdersReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out/orders.dat.OrdersReformat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>

</FunctionalTest>


<FunctionalTest ident="OrdersReformatExternTransform" graphFile="graph/graphOrdersReformatExternTransform.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out/orders.dat.OrdersReformat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>

</FunctionalTest>


<FunctionalTest ident="OrdersReformatInline" graphFile="graph/graphOrdersReformatInline.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>

</FunctionalTest>

<FunctionalTest ident="OrdersTLNewReformat" graphFile="graph/graphOrdersTLNewReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out/orders.dat.OrdersReformat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>

</FunctionalTest>

<FunctionalTest ident="OrdersTLReformat" graphFile="graph/graphOrdersTLReformat.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>

</FunctionalTest>

<FunctionalTest ident="OrdersTLReformatExtern" graphFile="graph/graphOrdersTLReformatExtern.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.dat.OrdersReformat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>

</FunctionalTest>

<FunctionalTest ident="ParametrizedLookup" graphFile="graph/graphParametrizedLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.ParametrizedLookup.out"/>
     
<DeleteFile file="data-out/joined_data_hash.out"/>

</FunctionalTest>

<FunctionalTest ident="Partition" graphFile="graph/graphPartition.grf">
	 <FlatFile outputFile="data-out/smallIdOlder.txt" supposedFile="supposed-out//smallIdOlder.Partition.txt"/>
	 <FlatFile outputFile="data-out/bigIdOlder.txt" supposedFile="supposed-out//bigIdOlder.Partition.txt"/>
	 <FlatFile outputFile="data-out/smallIdYounger.txt" supposedFile="supposed-out//smallIdYounger.Partition.txt"/>
	 <FlatFile outputFile="data-out/bigIdYounger.txt" supposedFile="supposed-out//bigIdYounger.Partition.txt"/>
	 <FlatFile outputFile="data-out/rejectedId.txt" supposedFile="supposed-out//rejectedId.Partition.txt"/>
     
<DeleteFile file="data-out/smallIdOlder.txt"/>
<DeleteFile file="data-out/bigIdOlder.txt"/>
<DeleteFile file="data-out/smallIdYounger.txt"/>
<DeleteFile file="data-out/bigIdYounger.txt"/>
<DeleteFile file="data-out/rejectedId.txt"/>

</FunctionalTest>

<FunctionalTest ident="Partition_Transform" graphFile="graph/graphPartition_Transform.grf">
	 <FlatFile outputFile="data-out/smallIdOlder.txt" supposedFile="supposed-out//smallIdOlder.Partition.txt"/>
	 <FlatFile outputFile="data-out/bigIdOlder.txt" supposedFile="supposed-out//bigIdOlder.Partition.txt"/>
	 <FlatFile outputFile="data-out/smallIdYounger.txt" supposedFile="supposed-out//smallIdYounger.Partition.txt"/>
	 <FlatFile outputFile="data-out/bigIdYounger.txt" supposedFile="supposed-out//bigIdYounger.Partition.txt"/>
	 <FlatFile outputFile="data-out/rejectedId.txt" supposedFile="supposed-out//rejectedId.Partition.txt"/>
     
<DeleteFile file="data-out/smallIdOlder.txt"/>
<DeleteFile file="data-out/bigIdOlder.txt"/>
<DeleteFile file="data-out/smallIdYounger.txt"/>
<DeleteFile file="data-out/bigIdYounger.txt"/>
<DeleteFile file="data-out/rejectedId.txt"/>

</FunctionalTest>

<FunctionalTest ident="Partition_KeyHashCode" graphFile="graph/graphPartition_KeyHashCode.grf">
	 <FlatFile outputFile="data-out/hashcode0.txt" supposedFile="supposed-out//hashcode0.Partition.txt"/>
	 <FlatFile outputFile="data-out/hashcode1.txt" supposedFile="supposed-out//hashcode1.Partition.txt"/>
	 <FlatFile outputFile="data-out/hashcode2.txt" supposedFile="supposed-out//hashcode2.Partition.txt"/>
	 <FlatFile outputFile="data-out/hashcode3.txt" supposedFile="supposed-out//hashcode3.Partition.txt"/>
	 <FlatFile outputFile="data-out/hashcode4.txt" supposedFile="supposed-out//hashcode4.Partition.txt"/>
     
<DeleteFile file="data-out/hashcode0.txt"/>
<DeleteFile file="data-out/hashcode1.txt"/>
<DeleteFile file="data-out/hashcode2.txt"/>
<DeleteFile file="data-out/hashcode3.txt"/>
<DeleteFile file="data-out/hashcode4.txt"/>

</FunctionalTest>

<FunctionalTest ident="Partition_RoundRobin" graphFile="graph/graphPartition_RoundRobin.grf">
	 <FlatFile outputFile="data-out/output0.txt" supposedFile="supposed-out//output0.Partition.txt"/>
	 <FlatFile outputFile="data-out/output1.txt" supposedFile="supposed-out//output1.Partition.txt"/>
	 <FlatFile outputFile="data-out/output2.txt" supposedFile="supposed-out//output2.Partition.txt"/>
	 <FlatFile outputFile="data-out/output3.txt" supposedFile="supposed-out//output3.Partition.txt"/>
	 <FlatFile outputFile="data-out/output4.txt" supposedFile="supposed-out//output4.Partition.txt"/>
     
<DeleteFile file="data-out/output0.txt"/>
<DeleteFile file="data-out/output1.txt"/>
<DeleteFile file="data-out/output2.txt"/>
<DeleteFile file="data-out/output3.txt"/>
<DeleteFile file="data-out/output4.txt"/>

</FunctionalTest>

<FunctionalTest ident="PhasesDemo" graphFile="graph/graphPhasesDemo.grf">
	 <FlatFile outputFile="data-out/orders.phases.merged" supposedFile="supposed-out//orders.phases.merged.PhasesDemo"/>
     
<DeleteFile file="data-out/orders.phases.merged"/>

</FunctionalTest>


<FunctionalTest ident="RangeLookup" graphFile="graph/graphRangeLookup.grf">
	 <FlatFile outputFile="data-out/peopleAtTour.out" supposedFile="supposed-out//peopleAtTour.RangeLookup.out"/>
     
<DeleteFile file="data-out/peopleAtTour.out"/>

</FunctionalTest>

<FunctionalTest ident="RollupTL_AsAggregatorAndDedup" graphFile="graph/graphRollupTL_AsAggregatorAndDedup.grf">
	 <FlatFile outputFile="data-out/orders.aggregated" supposedFile="supposed-out/orders.aggregated.AggregateSorted"/>
	 <FlatFile outputFile="data-out/orders.aggregated_Rollup" supposedFile="supposed-out/orders.aggregated.AggregateSorted"/>
</FunctionalTest>

<FunctionalTest ident="RollupTL_AsDenormalizer" graphFile="graph/graphRollupTL_AsDenormalizer.grf">
	 <FlatFile outputFile="data-out/denormalizedTL.out" supposedFile="supposed-out/denormalizedTL.DenormalizeTL.out"/>
	 <FlatFile outputFile="data-out/denormalizedTLwithoutOrder.out" supposedFile="supposed-out/denormalizedTLwithoutOrder.DenormalizeTL.out"/>
	 <FlatFile outputFile="data-out/denormalizedTLwithoutOrder_Rollup.out" supposedFile="supposed-out/denormalizedTL.DenormalizeTL.out"/>
</FunctionalTest>

<FunctionalTest ident="RollupTL_AsNormalizer" graphFile="graph/graphRollupTL_AsNormalizer.grf">
	 <FlatFile outputFile="data-out/normalized.out" supposedFile="supposed-out/normalized.NormalizeTL.out"/>
	 <FlatFile outputFile="data-out/normalized_Rollup.out" supposedFile="supposed-out/normalized.NormalizeTL.out"/>
</FunctionalTest>

<FunctionalTest ident="RunGraph" graphFile="graph/graphRunGraph.grf">
		<!--
		<RegEx expression="DEDUP0\s+FINISHED_OK" occurences="2"/>
		<RegEx expression="DENORMALIZE\s+FINISHED_OK" occurences="2"/>
		<RegEx expression="EXT_SORT0\s+FINISHED_OK" occurences="4"/>
		-->
</FunctionalTest>


<FunctionalTest ident="Sequence" graphFile="graph/graphSequence.grf">
	 <FlatFile outputFile="data-out/orders.dat.out" supposedFile="supposed-out//orders.Sequence.dat.out"/>
     
<DeleteFile file="data-out/orders.dat.out"/>
<DeleteFile file="seq/sequence.dat"/>

</FunctionalTest>


<FunctionalTest ident="SequenceChecker" graphFile="graph/graphSequenceChecker.grf" assertion="false">
		<RegEx expression="SEQUENCE_CHECKER0\s+FINISHED_OK" occurences="1"/>
		<RegEx expression="The sequence checker fails at row '6'." occurences="3"/>
</FunctionalTest>


<FunctionalTest ident="SimpleCopy" graphFile="graph/graphSimpleCopy.grf">
	 <FlatFile outputFile="data-out/employees.copy.dat" supposedFile="supposed-out/employees.copy.SimpleCopy.dat"/>
	 <FlatFile outputFile="data-out/Trash1_output.txt" supposedFile="supposed-out/Trash1_output.SimpleCopy.txt"/>
	<DeleteFile file="data-out/employees.copy.dat"/>
	<DeleteFile file="data-out/Trash1_output.txt"/>
</FunctionalTest>

<FunctionalTest ident="SimpleCopyEmbeddedMetadata" graphFile="graph/graphSimpleCopyEmbeddedMetadata.grf">
</FunctionalTest>


<FunctionalTest ident="SimpleCopyLocale" graphFile="graph/graphSimpleCopyLocale.grf">
	 <FlatFile outputFile="data-out/employees_locale.dat" supposedFile="supposed-out//employees_locale.SimpleCopyLocale.dat"/>
     
<DeleteFile file="data-out/employees_locale.dat"/>

</FunctionalTest>


<FunctionalTest ident="SimpleLookup" graphFile="graph/graphSimpleLookup.grf">
	 <FlatFile outputFile="data-out/joined_data_hash.out" supposedFile="supposed-out//joined_data_hash.SimpleLookup.out"/>
     
<DeleteFile file="data-out/joined_data_hash.out"/>

</FunctionalTest>


<FunctionalTest ident="SortData" graphFile="graph/graphSortData.grf">
	 <FlatFile outputFile="data-out/orders.sorted" supposedFile="supposed-out//orders.sorted.SortData"/>
     
<DeleteFile file="data-out/orders.sorted"/>

</FunctionalTest>


<FunctionalTest ident="SortUniversal" graphFile="graph/graphSortUniversal.grf">
	 <FlatFile outputFile="data-out/customers.sorted" supposedFile="supposed-out//customers.sorted.SortUniversal"/>
     
<DeleteFile file="data-out/customers.sorted"/>

</FunctionalTest>

<FunctionalTest ident="SortWithinGroups" graphFile="graph/graphSortWithinGroups.grf">
	 <FlatFile outputFile="data-out/friends-country-town-name.dat" supposedFile="supposed-out//friends-country-town-name.SortWithinGroups.dat"/>
	 <FlatFile outputFile="data-out/friends-country-town-age.dat" supposedFile="supposed-out//friends-country-town-age.SortWithinGroups.dat"/>
     
<DeleteFile file="data-out/friends-country-town-name.dat"/>
<DeleteFile file="data-out/friends-country-town-age.dat"/>

</FunctionalTest>

<FunctionalTest ident="SystemExecute" graphFile="graph/graphSystemExecute.grf">
	 <FlatFile outputFile="data-out/command.out" supposedFile="supposed-out/command.SystemExecute.out"/>
</FunctionalTest>

<FunctionalTest ident="ViewData" graphFile="graph/graphViewData.grf">
	 <FlatFile outputFile="data-out/data.out" supposedFile="supposed-out//data.ViewData.out"/>
     
<DeleteFile file="data-out/data.out"/>

</FunctionalTest>

<FunctionalTest ident="XLSReadWrite" graphFile="graph/graphXLSReadWrite.grf">
	 <XlsFile outputFile="data-out/ordersByCountry.xls" supposedFile="supposed-out//ordersByCountry.XLSReadWrite.xls"/>
	 <FlatFile outputFile="data-out/orders_delimited.txt" supposedFile="supposed-out/orders_delimited.XLSReadWrite.txt"/>
<DeleteFile file="data-out/ordersByCountry.xls"/>

</FunctionalTest>

<FunctionalTest ident="XPathReader" graphFile="graph/graphXPathReader.grf">
	 <FlatFile outputFile="data-out/XMLoutputCHILD.txt" supposedFile="supposed-out//XMLoutputCHILD.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputCUSTOM.txt" supposedFile="supposed-out//XMLoutputCUSTOM.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputEMPL.txt" supposedFile="supposed-out//XMLoutputEMPL.XPathReader.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputPROJ.txt" supposedFile="supposed-out//XMLoutputPROJ.XPathReader.txt"/>	 
	 <FlatFile outputFile="data-out/XMLoutputBENEF.txt" supposedFile="supposed-out//XMLoutputBENEF.XPathReader.txt"/>	 
     
		<DeleteFile file="seq/xpathsequence.seq"/>
	
</FunctionalTest>

<FunctionalTest ident="XMLExtract" graphFile="graph/graphXMLExtract.grf">
	 <FlatFile outputFile="data-out/XMLoutputCHILD.txt" supposedFile="supposed-out//XMLoutputCHILD.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputCUSTOM.txt" supposedFile="supposed-out//XMLoutputCUSTOM.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputEMPL.txt" supposedFile="supposed-out//XMLoutputEMPL.XMLExtract.txt"/>
	 <FlatFile outputFile="data-out/XMLoutputPROJ.txt" supposedFile="supposed-out//XMLoutputPROJ.XMLExtract.txt"/>	 
	 <FlatFile outputFile="data-out/XMLoutputBENEF.txt" supposedFile="supposed-out//XMLoutputBENEF.XMLExtract.txt"/>	 
     
		<DeleteFile file="data-out/XMLoutputCHILD.txt"/>
		<DeleteFile file="data-out/XMLoutputCUSTOM.txt"/>
		<DeleteFile file="data-out/XMLoutputEMPL.txt"/>
		<DeleteFile file="data-out/XMLoutputPROJ.txt"/>
		<DeleteFile file="data-out/XMLoutputBENEF.txt"/>
		<DeleteFile file="seq/seqkey.seq"/>
	
</FunctionalTest>

<FunctionalTest ident="XmlWriter" graphFile="graph/graphXmlWriter.grf">

   <FlatFile outputFile="data-out/xmlOut.xml" supposedFile="supposed-out/xmlOut.xml"/>

<!--     
		<DeleteFile file="data-out/xmlOut_00.zip"/>
		<DeleteFile file="data-out/xmlOut_01.zip"/>
		<DeleteFile file="data-out/xmlOut_02.zip"/>
		<DeleteFile file="data-out/xmlOut_03.zip"/>
		<DeleteFile file="data-out/xmlOut_04.zip"/>
		<DeleteFile file="seq/seqkey.seq"/>
-->	

</FunctionalTest>

<FunctionalTest ident="XMLExtractXsd" graphFile="graph/graphXMLExtractXsd.grf">
	 <FlatFile outputFile="data-out/persons.txt" supposedFile="supposed-out//persons.XMLExtractXsd.txt"/>
	 <FlatFile outputFile="data-out/items.txt" supposedFile="supposed-out//items.XMLExtractXsd.txt"/>
     
		<DeleteFile file="data-out/persons.txt"/>
		<DeleteFile file="data-out/items.txt"/>
	
</FunctionalTest>

<FunctionalTest ident="DataGeneratorExt" graphFile="graph/graphDataGeneratorExt.grf">
	 <FlatFile outputFile="data-out/random.txt" supposedFile="supposed-out/random.DataGeneratorExt.txt"/>
	 <FlatFile outputFile="data-out/random1.txt" supposedFile="supposed-out/random1.DataGeneratorExt.txt"/>
 </FunctionalTest>
 
 <FunctionalTest ident="DBFWrite" graphFile="graph/graphDBFWrite.grf">
	 <FlatFile outputFile="data-out/DBFWrite_out.txt" supposedFile="supposed-out/DBFWrite_out.txt"/>
 </FunctionalTest>

</TestScenario>
