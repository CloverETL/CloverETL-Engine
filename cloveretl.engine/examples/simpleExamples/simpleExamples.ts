<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "./testscenario.dtd">
<TestScenario ident="simple-examples" description="Engine simple examples scenario" useJMX="true">    

    <FunctionalTest ident="simpleCopy" graphFile="graph/graphSimpleCopy.grf">
      <FlatFile outputFile="data-out/friends-country+town-age.dat" supposedFile="supposed-out/friends-country+town-age.dat"/>
      <FlatFile outputFile="data-out/friends-country-town+name.dat" supposedFile="supposed-out/friends-country-town+name.dat"/>
    </FunctionalTest>

</TestScenario>
