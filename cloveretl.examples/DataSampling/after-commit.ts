<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="Data sampling" description="Data sampling" useJMX="true">    

	<FunctionalTest ident="DefaultSampling" graphFile="graph/CompareSamples.grf">
	</FunctionalTest>
	
	<FunctionalTest ident="DefaultSampling_small_sample" graphFile="graph/CompareSamples.grf">
		<Property name="sampling_size" value="0.001" />
	</FunctionalTest>
	
	<FunctionalTest ident="DefaultSampling_disable_sorting" graphFile="graph/CompareSamples.grf">
		<Property name="sort_data" value="passThrough" />
	</FunctionalTest>
	
	<FunctionalTest ident="JOB_COUNTRY_Sampling" graphFile="graph/CompareSamples.grf">
		<Property name="sampling_field" value="JOB_COUNTRY" />
	</FunctionalTest>

	<FunctionalTest ident="HIRE_DATE_Sampling" graphFile="graph/CompareSamples.grf">
		<Property name="sampling_field" value="HIRE_DATE" />
	</FunctionalTest>

</TestScenario>