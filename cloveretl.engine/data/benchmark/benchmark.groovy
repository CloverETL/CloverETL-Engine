import java.util.regex.Matcher;
import java.util.regex.Pattern;

class EngineBenchmark {

  def REPEAT_EXECUTION = 3
  
  def config = [
    LANGUAGE: [
      "CTL", 
      "CTLCOMPILED", 
      "GROOVY", 
      "SCALA"],
    SERVER_FLAG: [
      true, 
      false],
    GRAPH: [
      "big_decimal.grf"/*,
      "big_records.grf", 
      "dates.grf",
      "strings_unicode.grf", 
      "strings_long.grf", 
      "strings_basics.grf",
      "regex_dynamic.grf",
      "regex_static.grf"*/]
    ]
    
  def records = [
      "big_decimal.grf" : 1, 
      "big_records.grf" : 1, 
      "dates.grf" : 1, 
      "regex_dynamic.grf" : 1, 
      "regex_static.grf" : 1, 
      "strings_unicode.grf" : 1, 
      "strings_long.grf" : 1, 
      "strings_basics.grf" : 1]
    
  //TODO this is sick - done just because ANT build cannot be done (because of SCALA)
  def libs = [
    "bin",
    "lib/commons-logging-1.1.1.jar",
    "lib/jms.jar",
    "lib/javolution.jar",
    "lib/jxl.jar",
    "lib/log4j-1.2.15.jar",
    "lib/jsch-0.1.42.jar",
    "lib/commons-cli-1.1.jar",
    "lib/annotations.jar",
    "lib/icu4j-normalizer_transliterator-4.8.1.1.jar",
    "lib/jakarta-oro-2.0.8.jar",
    "lib/tar.jar",
    "lib/joda-time-1.6.jar",
    "lib/openxml4j-1.0-beta.jar",
    "lib/xmlbeans-2.3.0.jar",
    "lib/jsr173_1.0_api.jar",
    "lib/jaxb-api.jar",
    "lib/jaxb-impl.jar",
    "lib/dom4j-1.6.1.jar",
    "lib/commons-net-2.0.jar",
    "lib/commons-codec-1.4.jar",
    "lib/ftp4j-1.4.3.jar",
    "lib/XmlSchema-1.4.3.jar",
    "lib/commons-io-1.4.jar",
    "lib/truezip-6.8.1.jar",
    "lib/sardine.jar",
    "lib/jets3t-0.8.0.jar",
    "lib/commons-httpclient-3.1.jar",
    "lib/httpclient-4.1.1.jar",
    "lib/httpcore-4.1.1.jar",
    "lib/poi-3.8-beta4-20110826.jar",
    "lib/poi-ooxml-3.8-beta4-20110826.jar",
    "lib/poi-ooxml-schemas-3.8-beta4-20110826.jar",
    "lib/scala-compiler.jar",
    "lib/scala-dbc.jar",
    "lib/scala-library.jar",
    "lib/scala-swing.jar",
    "lib/groovy-all-1.8.2.jar",
    "lib/groovypp-all-0.9.0_1.8.2.jar",
    "resources"
  ]
  
  def execRootPath() {
    if (isWindows()) {
      return "d:/kubosj/workspaces/issue-CLO-115"
    } else {
      return "/home/jkubos/issue-CLO-115"
    }
  }
  
  def isWindows() {
    return System.properties['os.name'].toLowerCase().contains('windows')
  }
  
  def execute() {
    println "Benchmarking..."
    
    def parameters = [:]
    def scope = config.keySet() as List<String>
    
    combinate(parameters, scope)
  }
  
  def combinate(parameters, scope) {
    if (scope.size()==0) {
      executeCombination(parameters)
    } else {
      def myScope = scope.pop()

      config[myScope].each { value ->
        parameters[myScope] = value
        combinate(parameters, scope.clone())  
      }
    }
  }
  
  def executeCombination(parameters) {
    parameters.each { k, v -> print "${v} " }
    
    def command = buildCommand(parameters)

    for (int i=0;i<REPEAT_EXECUTION;++i) {
      def output = launchEngine(command)
      def runTime = parseRunTime(output)
      storeLog(i, parameters, output)
      
      print " ${runTime}"
    }
    
    print "\n"
  }
  
  def storeLog(runId, parameters, output) {
  
    def path = "logs/log"
    
    parameters.each { k, v -> path+="_${v}" }
  
    path += "_${runId}.log"
  
    new File(path) << output
  }
  
  def parseRunTime(output) {
    def m = output =~ /FINISHED_OK\s+(\d+)\s+/
    
    if (m) {
      return m[0][1]
    } else {
      println output
      throw new Exception("Something went wrong during parsing!")
    }
  }
  
  def launchEngine(command) {
    def proc = command.execute()
    
    StringBuffer outStream = new StringBuffer()
    StringBuffer errStream = new StringBuffer()
    
    proc.consumeProcessOutput(outStream, errStream)

    proc.waitFor()
    
    if (proc.exitValue()!=0) {
      storeLog("ERROR", [:], outStream.toString()+"\n"+errStream.toString())
      throw new Exception("Something went wrong during launching!")
    }
    
    return outStream.toString()
  }
  
  def buildCommand(parameters) {
    def launch_array = ["java"]
    
    if (parameters['SERVER_FLAG']) {
      launch_array.push("-server")
    }          
    
    def classPath = ""
    
    def sepChar = isWindows() ? ";" : ":"
    
    libs.each { lib -> classPath+="${execRootPath()}/cloveretl.engine/${lib}${sepChar}" }

    launch_array.push("-classpath")
    launch_array.push(classPath)

    launch_array.push("org.jetel.main.runGraph")
    
    launch_array.push("-plugins")
    launch_array.push(execRootPath()) 
    
    launch_array.push("-config")
    launch_array.push("config.properties")
    
    config["LANGUAGE"].each {lang -> launch_array.push("-P:${lang}=${parameters['LANGUAGE']==lang?"enabled":"disabled"}") }
    
    if (records[parameters["GRAPH"]]==null) {
      throw new Exception("Please provide count of records for graph ${parameters["GRAPH"]}")
    }
    
    launch_array.push("-P:RECORDS_COUNT=${records[parameters["GRAPH"]]}")
    
    launch_array.push("graph/${parameters["GRAPH"]}")
    
    //println launch_array
    
    return launch_array  
  }
}

engine = new EngineBenchmark()
engine.execute()

