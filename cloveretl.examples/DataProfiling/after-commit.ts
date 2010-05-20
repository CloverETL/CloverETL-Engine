<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="Data profiling" description="Data profiling" useJMX="true">    

	<FunctionalTest ident="BasicStatisticXLS" graphFile="graph/BasicStatistic.grf">
<!-- bug 3491 - do not merge it with higher versions!!!! 
		<FlatFile outputFile="data-out/summary.html" supposedFile="supposed-out/summary_xls.html"/>
-->
	</FunctionalTest>
	
	<FunctionalTest ident="AdvancedStatisticXLS" graphFile="graph/AdvancedStatistic.grf">
<!-- bug 3491 - do not merge it with higher versions!!!! 
	 	<FlatFile outputFile="data-out/histogram.html" supposedFile="supposed-out/histogram_xls.html"/>
-->
	</FunctionalTest>

	<FunctionalTest ident="BasicStatisticFlat" graphFile="graph/BasicStatistic.grf">
		<Property name="READER_TYPE" value="DATA_READER" />
		<Property name="input_file" value="data-in/employees.list.dat" />
		<Property name="metadata" value="meta/employees.fmt" />
		<Property name="WRITER_TYPE" value="XLS_WRITER" />
		<Property name="output_file" value="data-out/summary.xls" />
	 	<XlsFile outputFile="data-out/summary.xls" supposedFile="supposed-out/summary_flat.xls"/>
<!--	 	<DeleteFile file="data-out/summary.xls"/>-->
	</FunctionalTest>
	
	<FunctionalTest ident="AdvancedStatisticFlat" graphFile="graph/AdvancedStatistic.grf">
		<Property name="READER_TYPE" value="DATA_READER" />
		<Property name="input_file" value="data-in/employees.list.dat" />
		<Property name="metadata" value="meta/employees.fmt" />
		<Property name="WRITER_TYPE" value="XLS_WRITER" />
		<Property name="output_file" value="data-out/histogram.xls" />
	 	<XlsFile outputFile="data-out/histogram.xls" supposedFile="supposed-out/histogram_flat.xls"/>
<!--	 	<DeleteFile file="data-out/histogram.xls"/>-->
	</FunctionalTest>

</TestScenario>