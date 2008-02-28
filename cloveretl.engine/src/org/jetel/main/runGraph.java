/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-03  David Pavlis
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.jetel.main;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
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
 *  <tr><td nowrap>-checkconfig</td><td>only check graph configuration</td></tr>
 *  <tr><td nowrap>-noJMX</td><td>this switch turns off sending graph tracking information; this switch is recommended if the tracking information are not necessary</td></tr>
 *  <tr><td nowrap>-config <i>filename</i></td><td>load default engine properties from specified file</td></tr>
 *  <tr><td nowrap><b>filename</b></td><td>filename or URL of the file (even remote) containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  </table>
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 * @revision    $Revision$
 */
public class runGraph {
    private static Log logger = LogFactory.getLog(runGraph.class);

    //TODO change run graph version
	private final static String RUN_GRAPH_VERSION = JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION;
	public final static String VERBOSE_SWITCH = "-v";
	public final static String PROPERTY_FILE_SWITCH = "-cfg";
	public final static String LOG4J_PROPERTY_FILE_SWITCH = "-logcfg";
	public final static String LOG4J_LOG_LEVEL_SWITCH = "-loglevel";
	public final static String PROPERTY_DEFINITION_SWITCH = "-P:";
	public final static String TRACKING_INTERVAL_SWITCH = "-tracking";
	public final static String INFO_SWITCH = "-info";
    public final static String PLUGINS_SWITCH = "-plugins";
    public final static String PASSWORD_SWITCH = "-pass";
    public final static String LOAD_FROM_STDIN_SWITCH = "-stdin";
    public final static String LOG_HOST_SWITCH = "-loghost";
    public final static String CHECK_CONFIG_SWITCH = "-checkconfig";
    public final static String NO_JMX = "-noJMX";
    public final static String CONFIG_SWITCH = "-config";
    public final static String MBEAN_NAME = "-mbean";
	
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
        boolean loadFromSTDIN = false;
        String pluginsRootDirectory = null;
        boolean onlyCheckConfig = false;
        String logHost = null;
        String graphFileName = null;
        String configFileName = null;
        
        System.out.println("***  CloverETL framework/transformation graph runner ver "
                        + RUN_GRAPH_VERSION
                        + ", (c) 2002-06 D.Pavlis, released under GNU Lesser General Public License  ***");
        System.out.println(" Running with framework version: "
                + JetelVersion.MAJOR_VERSION + "." + JetelVersion.MINOR_VERSION
                + " build#" + JetelVersion.BUILD_NUMBER + " compiled "
                + JetelVersion.LIBRARY_BUILD_DATETIME);
        System.out.println();


        Logger.getLogger(runGraph.class); // make log4j to init itself
        String log4jPropertiesFile = null;
        Level logLevel = null;
        
        // process command line arguments
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith(VERBOSE_SWITCH)) {
                runtimeContext.setVerboseMode(true);
            } else if (args[i].startsWith(PROPERTY_FILE_SWITCH)) {
                i++;
                try {
                    InputStream inStream = new BufferedInputStream(
                            new FileInputStream(args[i]));
                    Properties properties = new Properties();
                    properties.load(inStream);
                    runtimeContext.addAdditionalProperties(properties);
                } catch (IOException ex) {
                    logger.error(ex.getMessage(), ex);
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
            } else if (args[i].startsWith(PROPERTY_DEFINITION_SWITCH)) {
                // String[]
                // nameValue=args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"").split("=");
                // properties.setProperty(nameValue[0],nameValue[1]);
                String tmp = args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH, "");
                runtimeContext.addAdditionalProperty(tmp.substring(0, tmp.indexOf("=")), tmp.substring(tmp.indexOf("=") + 1));
            } else if (args[i].startsWith(TRACKING_INTERVAL_SWITCH)) {
                i++;
                try {
                    runtimeContext.setTrackingInterval(Integer.parseInt(args[i]) * 1000);
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
            } else if (args[i].startsWith(PASSWORD_SWITCH)) {
                i++;
                runtimeContext.setPassword(args[i]);
            } else if (args[i].startsWith(LOAD_FROM_STDIN_SWITCH)) {
                loadFromSTDIN = true;
            } else if (args[i].startsWith(LOG_HOST_SWITCH)) {
                i++;
                logHost = args[i];
            } else if (args[i].startsWith(CHECK_CONFIG_SWITCH)) {
                onlyCheckConfig = true;
            } else if (args[i].startsWith(MBEAN_NAME)){
                i++;
                //TODO
                //runtimeContext.set  --> mbeanName = args[i];
            } else if (args[i].startsWith(NO_JMX)){
                runtimeContext.setUseJMX(false);
            } else if (args[i].startsWith(CONFIG_SWITCH)) {
                i++;
                configFileName = args[i];
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

        if (log4jPropertiesFile != null){
        	PropertyConfigurator.configure( log4jPropertiesFile );
        } else {
        	/*
        	String wdFile = "./log4j.properties";
        	File f = new File(wdFile);
        	if (f.canRead())
            	PropertyConfigurator.configure( wdFile );
        	else
        		BasicConfigurator.configure();
        		*/
        }
        
        if (logLevel != null)
        	Logger.getRootLogger().setLevel(logLevel);

        // engine initialization - should be called only once
        EngineInitializer.initEngine(pluginsRootDirectory, configFileName, logHost);

        //tohle je nutne odstranit - runtimeContext je potreba vytvorit az po initiazlizaci enginu!!! Kokon
        runtimeContext.setTrackingInterval(Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL);
        
        // prepare input stream with XML graph definition
        InputStream in = null;
        if (loadFromSTDIN) {
            System.out.println("Graph definition loaded from STDIN");
            in = System.in;
        } else {
            System.out.println("Graph definition file: " + graphFileName);
            try {
                in = Channels.newInputStream(FileUtils.getReadableChannel(null, graphFileName));
            } catch (IOException e) {
                System.err.println("Error - graph definition file can't be read: " + e.getMessage());
                System.exit(-1);
            }
        }


        TransformationGraph graph = null;
        Future<Result> futureResult = null;;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext.getAdditionalProperties());
	        futureResult = executeGraph(graph, runtimeContext);
        } catch (XMLConfigurationException e) {
            logger.error("Error in reading graph from XML !", e);
            if (runtimeContext.isVerboseMode()) {
                e.printStackTrace(System.err);
            }
            System.exit(-1);
        } catch (GraphConfigurationException e) {
            logger.error("Error - graph's configuration invalid !", e);
            if (runtimeContext.isVerboseMode()) {
                e.printStackTrace(System.err);
            }
            System.exit(-1);
		} catch (ComponentNotReadyException e) {
            logger.error("Error during graph initialization !", e);
            if (runtimeContext.isVerboseMode()) {
                e.printStackTrace(System.err);
            }
            System.exit(-1);
        } catch (RuntimeException e) {
            logger.error("Error during graph initialization !", e);
            if (runtimeContext.isVerboseMode()) {
                e.printStackTrace(System.err);
            }
            System.exit(-1);
		} 
        
        Result result = Result.N_A;
		try {
			result = futureResult.get();
		} catch (InterruptedException e) {
            logger.error("Graph was unexpectedly interrupted !", e);
            if (runtimeContext.isVerboseMode()) {
                e.printStackTrace(System.err);
            }
            System.exit(-1);
		} catch (ExecutionException e) {
            logger.error("Error during graph processing !", e);
            if (runtimeContext.isVerboseMode()) {
                e.printStackTrace(System.err);
            }
            System.exit(-1);
		}
        
        System.out.println("Freeing graph resources.");
		graph.free();
		
        switch (result) {

        case FINISHED_OK:
            // everything O.K.
            System.out.println("Execution of graph successful !");
            System.exit(0);
            break;
        case ABORTED:
            // execution was ABORTED !!
            System.err.println("Execution of graph aborted !");
            System.exit(result.code());
            break;
        default:
            System.err.println("Execution of graph failed !");
            System.exit(result.code());
        }

    }


	public static Future<Result> executeGraph(TransformationGraph graph, GraphRuntimeContext runtimeContext) throws ComponentNotReadyException {
		if (!graph.isInitialized())
			EngineInitializer.initGraph(graph, runtimeContext);

        IThreadManager threadManager = new SimpleThreadManager();
        WatchDog watchDog = new WatchDog(threadManager, graph, runtimeContext);
		return threadManager.executeWatchDog(watchDog);
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
        System.out.println("-pass\t\tpassword for decrypting of hidden connections passwords");
        System.out.println("-stdin\t\tload graph definition from STDIN");
        System.out.println("-loghost\t\tdefine host and port number for socket appender of log4j (log4j library is required); i.e. localhost:4445");
        System.out.println("-checkconfig\t\tonly check graph configuration");
       // System.out.println("-mbean <name>\t\tname under which register Clover's JMXBean");
        System.out.println("-noJMX\t\tturns off sending graph tracking information");
        
        System.out.println();
        System.out.println("Note: <graph definition file> can be either local filename or URL of local/remote file");
        
	}

	private static void printInfo(){
	    System.out.println("CloverETL library version "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
	}
	
}

