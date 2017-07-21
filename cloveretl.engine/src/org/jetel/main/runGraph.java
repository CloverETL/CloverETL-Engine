/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.main;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.dictionary.SerializedDictionaryValue;
import org.jetel.graph.dictionary.UnsupportedDictionaryOperation;
import org.jetel.graph.runtime.AuthorityProxyFactory;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.graph.runtime.WatchDogFuture;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.JetelVersion;
import org.jetel.util.file.FileUtils;

/*
 *  - runGraph.main()
 *      - runGraph.initEngine()
 *      - runGraph.loadGraph()
 *          - ..., Nodes.fromXML(), ...
 *      - TransformationGraph.init()
 *          - validate Connections (init(), free())
 *          - validate Sequences (init(), free())
 *          - analyze graph topology
 *      - TransformationGraph.checkConfig()
 *          - Phases.checkConfig()
 *              - Nodes.checkConfig()
 *      - TransfomrationGraph.run()
 *          - create and start WatchDog thread
 *              - WatchDog.runPhase()
 *                  - Phase.init()
 *                      - Edges.init()
 *                      - Nodes.init()
 *                  - start all node threads in phase (Nodes.execute())
 *                  - watching them 
 *                  - Phase.free()
 *                      - Edges.free()
 *                      - Nodes.free()             
 */

/**
 *  class for executing transformations described in XML layout file<br><br>
 *  The graph layout is read from specified XML file and the whole transformation is executed.<br>
 *  <tt><pre>
 *  Program parameters:
 *  <table>
 *  <tr><td nowrap>-v</td><td>be verbose - print even graph layout</td></tr>
 *  <tr><td nowrap>-P:<i>properyName</i>=<i>propertyValue</i></td><td>add definition of property to global graph's property list</td></tr>
 *  <tr><td nowrap>-cfg <i>filename</i></td><td>load definitions of properties from specified file</td></tr>
 *  <tr><td nowrap>-logcfg <i>filename</i></td><td>load log4j properties from specified file; if not specified, \"log4j.properties\" should be in classpath</td></tr>
 *  <tr><td nowrap>-loglevel <i>ALL | TRACE | DEBUG | INFO | WARN | ERROR | FATAL | OFF</i></td><td>overrides log4j configuration and sets specified logging level for rootLogger</td></tr>
 *  <tr><td nowrap>-tracking <i>seconds</i></td><td>how frequently output the processing status</td></tr>
 *  <tr><td nowrap>-info</td><td>print info about Clover library version</td></tr>
 *  <tr><td nowrap>-plugins <i>filename</i></td><td>directory where to look for plugins/components</td></tr>
 *  <tr><td nowrap>-pass <i>password</i></td><td>password for decrypting of hidden connections passwords</td></tr>
 *  <tr><td nowrap>-stdin</td><td>load graph layout from STDIN</td></tr>
 *  <tr><td nowrap>-loghost</td><td>define host and port number for socket appender of log4j (log4j library is required); i.e. localhost:4445</td></tr>
 *  <tr><td nowrap>-skipcheckconfig</td><td>skip checking of graph configuration</td></tr>
 *  <tr><td nowrap>-noJMX</td><td>this switch turns off sending graph tracking information; this switch is recommended if the tracking information are not necessary</td></tr>
 *  <tr><td nowrap>-config <i>filename</i></td><td>load default engine properties from specified file</td></tr>
 *  <tr><td nowrap>-nodebug</td><td>turns off all runtime debugging e.g edge debugging</td></tr>
 *  <tr><td nowrap>-debugdirectory <i>foldername</i></td><td>directory dedicated to store temporary debug data; default is java's temporary folder</td></tr>
 *  <tr><td nowrap>-contexturl <i>foldername</i></td><td>all relative paths in graph xml are relative to this folder</td></tr>
 *  <tr><td nowrap><b>filename</b></td><td>filename or URL of the file (even remote) containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  </table>
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 */
public class runGraph {
    private static Logger logger = Logger.getLogger(runGraph.class);

	public final static String VERBOSE_SWITCH = "-v";
	public final static String PROPERTY_FILE_SWITCH = "-cfg";
	public final static String LOG4J_PROPERTY_FILE_SWITCH = "-logcfg";
	public final static String LOG4J_LOG_LEVEL_SWITCH = "-loglevel";
	public final static String PROPERTY_DEFINITION_SWITCH = "-P:";
	public final static String TRACKING_INTERVAL_SWITCH = "-tracking";
	public final static String INFO_SWITCH = "-info";
    public final static String PLUGINS_SWITCH = "-plugins";
    public final static String LICENSES_SWITCH = "-licenses";
    public final static String PASSWORD_SWITCH = "-pass";
    public final static String LOAD_FROM_STDIN_SWITCH = "-stdin";
    public final static String LOG_HOST_SWITCH = "-loghost";
    public final static String SKIP_CHECK_CONFIG_SWITCH = "-skipcheckconfig";
    public final static String NO_JMX = "-noJMX";
    public final static String CONFIG_SWITCH = "-config";
    public final static String NO_DEBUG_SWITCH = "-nodebug";
    public final static String NO_TOKEN_TRACKING_SWITCH = "-notokentracking";
    public final static String DEBUG_DIRECTORY_SWITCH = "-debugdirectory";
    public final static String CONTEXT_URL_SWITCH = "-contexturl";
    //private command line options
    public final static String WAIT_FOR_JMX_CLIENT_SWITCH = "-waitForJMXClient";
    public final static String MBEAN_NAME = "-mbean";
    public final static String DICTIONARY_VALUE_DEFINITION_SWITCH = "-V:";
    public final static String CLOVER_CLASS_PATH = "-classpath";
    public final static String CLOVER_COMPILE_CLASS_PATH = "-compileclasspath";
    public final static String INIT_PROXY_AUTHENTICATOR_SWITCH = "-proxyauth";
    public final static String LOCALE_SWITCH = "-locale";
    public final static String TIME_ZONE_SWITCH = "-timezone";
	
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
        boolean loadFromSTDIN = false;
        String pluginsRootDirectory = null;
        String licenseLocations = null;
        String logHost = null;
        String graphFileName = null;
        String configFileName = null;
        
        //the runtime context values parsed from a command line
        boolean isVerboseMode = GraphRuntimeContext.DEFAULT_VERBOSE_MODE;
        Properties additionalProperties = new Properties();
        int trackingInterval = 0;
        String password = null;
        boolean waitForJMXClient = GraphRuntimeContext.DEFAULT_WAIT_FOR_JMX_CLIENT;
        boolean useJMX = GraphRuntimeContext.DEFAULT_USE_JMX;
        boolean debugMode = GraphRuntimeContext.DEFAULT_DEBUG_MODE;
        boolean tokenTracking = GraphRuntimeContext.DEFAULT_TOKEN_TRACKING;
        boolean skipCheckConfig = GraphRuntimeContext.DEFAULT_SKIP_CHECK_CONFIG;
        String debugDirectory = null;
        URL contextURL = null;
        String classPathString = null;
        String compileClassPathString = null;
        String locale = null;
        String timeZone = null;
        
        List<SerializedDictionaryValue> dictionaryValues = new ArrayList<SerializedDictionaryValue>();
        
        Logger.getLogger(runGraph.class); // make log4j to init itself
        String log4jPropertiesFile = null;
        Level logLevel = null;
        
        // process command line arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith(VERBOSE_SWITCH)) {
                isVerboseMode = true;
            } else if (args[i].startsWith(PROPERTY_FILE_SWITCH)) {
                i++;
                try {
                    InputStream inStream = new BufferedInputStream(
                            new FileInputStream(args[i]));
                    Properties properties = new Properties();
                    properties.load(inStream);
                    additionalProperties.putAll(properties);
                } catch (IOException ex) {
                    logger.error(ex);
                    System.exit(-1);
                }
            } else if (args[i].startsWith(LOG4J_PROPERTY_FILE_SWITCH)) {
                i++;
                log4jPropertiesFile = args[i];
                File test = new File(log4jPropertiesFile);
                if (!test.canRead()){
                    System.err.println("Cannot read file: \"" + log4jPropertiesFile + "\"");
                    System.exit(-1);
                }
            } else if (args[i].startsWith(LOG4J_LOG_LEVEL_SWITCH)){
                i++;
                String logLevelString = args[i];
                logLevel = Level.toLevel(logLevelString, Level.DEBUG);
                
            } else if (args[i].startsWith(CLOVER_CLASS_PATH)){
                i++;
                classPathString = args[i];
            } else if (args[i].startsWith(CLOVER_COMPILE_CLASS_PATH)){
                i++;
                compileClassPathString = args[i];
            } else if (args[i].startsWith(PROPERTY_DEFINITION_SWITCH)) {
                // String[]
                // nameValue=args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"").split("=");
                // properties.setProperty(nameValue[0],nameValue[1]);
                String tmp = args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH, "");
                additionalProperties.put(tmp.substring(0, tmp.indexOf("=")), tmp.substring(tmp.indexOf("=") + 1));
            } else if (args[i].startsWith(TRACKING_INTERVAL_SWITCH)) {
                i++;
                try {
                    trackingInterval = Integer.parseInt(args[i]) * 1000;
                } catch (NumberFormatException ex) {
                    System.err.println("Invalid tracking parameter: \"" + args[i] + "\"");
                    System.exit(-1);
                }
            } else if (args[i].startsWith(INFO_SWITCH)) {
                printInfo();
                System.exit(0);
            } else if (args[i].startsWith(PLUGINS_SWITCH)) {
                i++;
                pluginsRootDirectory = args[i];
            } else if (args[i].startsWith(LICENSES_SWITCH)) {
                i++;
                licenseLocations = args[i];
            } else if (args[i].startsWith(PASSWORD_SWITCH)) {
                i++;
                password = args[i];
            } else if (args[i].startsWith(LOAD_FROM_STDIN_SWITCH)) {
                loadFromSTDIN = true;
            } else if (args[i].startsWith(LOG_HOST_SWITCH)) {
                i++;
                logHost = args[i];
            } else if (args[i].startsWith(SKIP_CHECK_CONFIG_SWITCH)) {
                skipCheckConfig = true;
            } else if (args[i].startsWith(WAIT_FOR_JMX_CLIENT_SWITCH)) {
                waitForJMXClient = true;
            } else if (args[i].startsWith(MBEAN_NAME)){
                i++;
                //TODO
                //runtimeContext.set  --> mbeanName = args[i];
            } else if (args[i].startsWith(NO_JMX)){
                useJMX = false;
            } else if (args[i].startsWith(CONFIG_SWITCH)) {
                i++;
                configFileName = args[i];
            } else if (args[i].startsWith(NO_TOKEN_TRACKING_SWITCH)){
            	tokenTracking = false;
            } else if (args[i].startsWith(NO_DEBUG_SWITCH)) {
                debugMode = false;
            } else if (args[i].startsWith(DEBUG_DIRECTORY_SWITCH)) {
                i++;
                debugDirectory = args[i]; 
            } else if (args[i].startsWith(CONTEXT_URL_SWITCH)) {
                i++;
                try {
					contextURL = FileUtils.getFileURL(FileUtils.appendSlash(args[i]));
				} catch (MalformedURLException e) {
                    System.err.println("Invalid contextURL command line parameter: " + args[i]);
                    System.exit(-1);
				} 
            } else if (args[i].startsWith(DICTIONARY_VALUE_DEFINITION_SWITCH)) {
            	String value = args[i].replaceFirst(DICTIONARY_VALUE_DEFINITION_SWITCH, "");
            	try {
            		dictionaryValues.add(SerializedDictionaryValue.fromString(value));
            	} catch (IllegalArgumentException e) {
                    System.err.println("Invalid dictionary value format: " + value);
                    System.exit(-1);
            	}
            } else if (args[i].equals(INIT_PROXY_AUTHENTICATOR_SWITCH)) {
            	// Java ignores http.proxyUser. Here come's the workaround.
            	Authenticator.setDefault(new Authenticator() {
            	    @Override
            	    protected PasswordAuthentication getPasswordAuthentication() {
            	        if (getRequestorType() == RequestorType.PROXY) {
            	            String prot = getRequestingURL().getProtocol().toLowerCase();
            	            String host = System.getProperty(prot + ".proxyHost", "");
            	            String port = System.getProperty(prot + ".proxyPort", "");
            	            String user = System.getProperty(prot + ".proxyUser", "");
            	            String password = System.getProperty(prot + ".proxyPassword", "");

            	            if (getRequestingHost().equalsIgnoreCase(host)) {
            	                if (Integer.parseInt(port) == getRequestingPort()) {
            	                    // Seems to be OK.
            	                    return new PasswordAuthentication(user, password.toCharArray());
            	                }
            	            }
            	        }
            	        return null;
            	    }
            	});            
            } else if (args[i].startsWith(LOCALE_SWITCH)){
                i++;
                locale = args[i];
            } else if (args[i].startsWith(TIME_ZONE_SWITCH)){
                i++;
                timeZone = args[i];
            } else if (args[i].startsWith("-")) {
                System.err.println("Unknown option: " + args[i]);
                System.exit(-1);
            } else {
                graphFileName = args[i];
            }
        }

        if (graphFileName == null) {
            printHelp();
            System.exit(-1);
        }

        if (log4jPropertiesFile != null) {
			PropertyConfigurator.configure(log4jPropertiesFile);
		}
        
        if (logLevel != null) {
        	setLogLevel(logLevel);
        }

        EngineInitializer.initLicenses(licenseLocations);
        // engine initialization - should be called only once
        EngineInitializer.initEngine(pluginsRootDirectory, configFileName, logHost);
        
        //prepare runtime context
        GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
        runtimeContext.setVerboseMode(isVerboseMode);
        runtimeContext.addAdditionalProperties(additionalProperties);
        if (trackingInterval > 0) runtimeContext.setTrackingInterval(trackingInterval);
        runtimeContext.setPassword(password);
        runtimeContext.setWaitForJMXClient(waitForJMXClient);
        runtimeContext.setSkipCheckConfig(skipCheckConfig);
        runtimeContext.setUseJMX(useJMX);
        runtimeContext.setTokenTracking(tokenTracking);
        runtimeContext.setDebugMode(debugMode);
        runtimeContext.setDebugDirectory(debugDirectory);
        runtimeContext.setContextURL(contextURL);
        try {
			runtimeContext.setJobUrl(FileUtils.getFileURL(contextURL, graphFileName).toString());
		} catch (MalformedURLException e1) {
			ExceptionUtils.logException(logger, "Given graph path cannot form a valid URL", e1);
			ExceptionUtils.logHighlightedException(logger, "Given graph path cannot form a valid URL", e1);
			System.exit(-1);
		}
        runtimeContext.setLocale(locale);
        runtimeContext.setTimeZone(timeZone);
    	if (classPathString != null) {
    		try {
				runtimeContext.setRuntimeClassPath(FileUtils.getFileUrls(contextURL, classPathString.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX)));
			} catch (MalformedURLException e) {
				ExceptionUtils.logException(logger, "Given classpath is not valid URL.", e);
				ExceptionUtils.logHighlightedException(logger, "Given classpath is not valid URL.", e);
				System.exit(-1);
			}
    	}
    	if (compileClassPathString != null) {
    		try {
				runtimeContext.setCompileClassPath(FileUtils.getFileUrls(contextURL, compileClassPathString.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX)));
			} catch (MalformedURLException e) {
				ExceptionUtils.logException(logger, "Given compile classpath is not valid URL.", e);
				ExceptionUtils.logHighlightedException(logger, "Given compile classpath is not valid URL.", e);
				System.exit(-1);
			}
    	}
        
        // prepare input stream with XML graph definition
        InputStream in = null;
        if (loadFromSTDIN) {
            logger.info("Graph definition loaded from STDIN");
            in = System.in;
        } else {
        	logger.info("Graph definition file: " + graphFileName);

        	try {
            	in = FileUtils.getInputStream(contextURL, graphFileName);
            } catch (IOException e) {
            	ExceptionUtils.logException(logger, "Error - graph definition file can't be read", e);
            	ExceptionUtils.logHighlightedException(logger, "Error - graph definition file can't be read", e);
                System.exit(-1);
            }
        }

        TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext);
			logger.info("Graph revision: " + graph.getRevision() + " Modified by: " + graph.getModifiedBy() + " Modified: " + graph.getModified());
			initializeDictionary(dictionaryValues, graph);
	        runGraph(graph);
        } catch (XMLConfigurationException e) {
            ExceptionUtils.logException(logger, "Error in reading graph from XML !", e);
            ExceptionUtils.logHighlightedException(logger, "Error in reading graph from XML !", e);
            System.exit(-1);
        } catch (GraphConfigurationException e) {
            ExceptionUtils.logException(logger, "Error - graph's configuration invalid !", e);
            ExceptionUtils.logHighlightedException(logger, "Error - graph's configuration invalid !", e);
            System.exit(-1);
		} 
    }
	
	private static void initializeDictionary(List<SerializedDictionaryValue> dictionaryValues, TransformationGraph graph)
	throws XMLConfigurationException {
		for (SerializedDictionaryValue serializedDictionaryValue : dictionaryValues) {
	        try {
	        	String key = serializedDictionaryValue.getKey();
	        	String type = serializedDictionaryValue.getType();
	        	Properties properties = serializedDictionaryValue.getProperties();
	        	graph.getDictionary().setValueFromProperties(key, type, properties);
	        } catch (ComponentNotReadyException e) {
	            throw new XMLConfigurationException("Dictionary initialization problem.", e);
			} catch (UnsupportedDictionaryOperation e) {
	            throw new XMLConfigurationException("Dictionary initialization problem.", e);
			}
		}
	}
	
	private static void runGraph(TransformationGraph graph) {
        WatchDogFuture watchDogFuture = null;
		try {
			if (!graph.isInitialized()) {
				EngineInitializer.initGraph(graph);
			}
			watchDogFuture = executeGraph(graph, graph.getRuntimeContext());			
        } catch (Exception e) {
			ExceptionUtils.logException(logger, "Error during graph initialization !", e);
            ExceptionUtils.logHighlightedException(logger, "Error during graph initialization !", e);
            System.exit(-1);
		} 
        
        Result result = Result.N_A;
		try {
			result = watchDogFuture.get();
		} catch (InterruptedException e) {
			ExceptionUtils.logException(logger, "Graph was unexpectedly interrupted !", e);
			ExceptionUtils.logHighlightedException(logger, "Graph was unexpectedly interrupted !", e);
            System.exit(-1);
		} catch (ExecutionException e) {
			ExceptionUtils.logException(logger, "Error during graph processing !", e);
			ExceptionUtils.logHighlightedException(logger, "Error during graph processing !", e);
            System.exit(-1);
		}
        
        logger.info("Freeing graph resources.");
		graph.free();
		
        switch (result) {

        case FINISHED_OK:
            // everything O.K.
            logger.info("Execution of graph successful !");
            System.exit(0);
            break;
        case ABORTED:
            // execution was ABORTED !!
            logger.warn("Execution of graph aborted !");
            System.exit(result.code());
            break;
        default:
            ExceptionUtils.logHighlightedException(logger, null,
            		watchDogFuture.getWatchDog().getCauseException());
            logger.error("Execution of graph failed !");
            System.exit(result.code());
        }
    }

	public static WatchDogFuture executeGraph(TransformationGraph graph, GraphRuntimeContext runtimeContext) throws ComponentNotReadyException {
		if (!graph.isInitialized()) {
			EngineInitializer.initGraph(graph);
		}

		//load dictionary content from runtime context
		DictionaryValuesContainer dictContainer = runtimeContext.getDictionaryContent();
		for (String key : runtimeContext.getDictionaryContent().getKeys()) {
			graph.getDictionary().setValue(key, dictContainer.getValue(key));
		}
		
		IAuthorityProxy authorityProxy = AuthorityProxyFactory.createDefaultAuthorityProxy();
		runtimeContext.setAuthorityProxy(authorityProxy);
        IThreadManager threadManager = new SimpleThreadManager();
        WatchDog watchDog = new WatchDog(graph, runtimeContext);
        threadManager.initWatchDog(watchDog);
		return threadManager.executeWatchDog(watchDog);
	}
    
	public static String getInfo(){
		final StringBuilder ret = new StringBuilder();
		ret.append("CloverETL library version ");
		ret.append(JetelVersion.MAJOR_VERSION );
		ret.append(".");
		ret.append(JetelVersion.MINOR_VERSION);
		ret.append(".");
		ret.append(JetelVersion.REVISION_VERSION);
		if( ! "".equals(JetelVersion.VERSION_SUFFIX) ) {
			ret.append(".");
			ret.append(JetelVersion.VERSION_SUFFIX);
	 	}
		if( ! "0".equals(JetelVersion.BUILD_NUMBER) ) {
			ret.append(" build#");
			ret.append(JetelVersion.BUILD_NUMBER);
		}
		if( JetelVersion.LIBRARY_BUILD_DATETIME.length() != 0 ){
			ret.append(" compiled ");
			ret.append( JetelVersion.LIBRARY_BUILD_DATETIME );
		}
		return ret.toString();
	}
	
	public static void printInfo(){
	    System.out.println(getInfo());
	}

	private static void printHelp() {
		System.out.println("Usage: runGraph [-(v|cfg|logcfg|loglevel|P:|tracking|info|plugins|pass)] <graph definition file>");
		System.out.println("Options:");
		System.out.println("-v\t\t\tbe verbose - print even graph layout");
		System.out.println("-P:<key>=<value>\tadd definition of property to global graph's property list");
		System.out.println("-cfg <filename>\t\tload definitions of properties from specified file");
		System.out.println("-logcfg <filename>\tload log4j configuration from specified file; \n\t\t\tif not specified, \"log4j.properties\" should be in classpath");
		System.out.println("-loglevel <level>\toverrides log4j configuration and sets specified logging level for rootLogger; \n\t\t\tpossible levels are: ALL | TRACE | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
		System.out.println("-tracking <seconds>\thow frequently output the graph processing status");
		System.out.println("-info\t\t\tprint info about Clover library version");
        System.out.println("-plugins\t\tdirectory where to look for plugins/components");
        System.out.println("-pass\t\t\tpassword for decrypting of hidden connections passwords");
        System.out.println("-stdin\t\t\tload graph definition from STDIN");
        System.out.println("-loghost\t\tdefine host and port number for socket appender of log4j (log4j library is required); i.e. localhost:4445");
        System.out.println("-checkconfig\t\tonly check graph configuration");
        System.out.println("-skipcheckconfig\t\tskip checking of graph configuration");
        System.out.println("-noJMX\t\t\tturns off sending graph tracking information");
        System.out.println("-config <filename>\t\tload default engine properties from specified file");
        System.out.println("-nodebug\t\tturns off all runtime debugging e.g edge debugging");
        System.out.println("-debugdirectory <foldername>\t\tdirectory dedicated to store temporary debug data; default is java's temporary folder");
        
        System.out.println();
        System.out.println("Note: <graph definition file> can be either local filename or URL of local/remote file");
        
	}

	public static void printRuntimeHeader() {
        logger.info("***  CloverETL framework/transformation graph"
                + ", (c) 2002-" + JetelVersion.LIBRARY_BUILD_YEAR + " Javlin a.s.  ***");
        logger.info("Running with " + getInfo());

        logger.info("Running on " + Runtime.getRuntime().availableProcessors() + " CPU(s), " +
        		"OS " + System.getProperty("os.name") +
        		", architecture " + System.getProperty("os.arch") + 
        		", Java version " + System.getProperty("java.version") +
        		", max available memory for JVM " + Runtime.getRuntime().maxMemory() / 1024 + " KB");
	}
	
	/**
	 * Sets user-defined level of logging.
	 * First of all, level of root logger is set to requested level and
	 * filters to all appenders are added to avoid unintended logging messages
	 * from child loggers with specified log level (see CLO-1682). 
	 * @param logLevel
	 */
	private static void setLogLevel(final Level logLevel) {
    	Logger.getRootLogger().setLevel(logLevel);
    	
    	Filter filter = new Filter() {
			@Override
			public int decide(LoggingEvent event) {
			    if (event.getLevel().isGreaterOrEqual(logLevel)) {
			    	return NEUTRAL;
			    } else {
			    	return DENY;
			    }
			}
		};
    	
    	Enumeration appenders = Logger.getRootLogger().getAllAppenders();
    	while (appenders.hasMoreElements()) {
    		((Appender) appenders.nextElement()).addFilter(filter);
    	}
	}
	
}

