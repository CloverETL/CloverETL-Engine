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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.jetel.component.ComponentFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.Plugins;
import org.jetel.util.FileUtils;
import org.jetel.util.JetelVersion;
import org.jetel.util.crypto.Enigma;

/**
 *  class for reading and showing data over input components<br><br>
 *  The graph layout is read from specified XML file and over component id is written data to output.<br>
 *  <tt><pre>
 *  Program parameters:
 *  <table>
 *  <tr><td nowrap>-P<i>properyName</i>=<i>propertyValue</i></td><td>add definition of property to global graph's property list</td></tr>
 *  <tr><td nowrap>--cfg <i>filename</i></td><td>load definitions of properties from specified file</td></tr>
 *  <tr><td nowrap>--tracking <i>seconds</i></td><td>how frequently output the processing status</td></tr>
 *  <tr><td nowrap>--info</td><td>print info about Clover library version</td></tr>
 *  <tr><td nowrap>--plugins <i>filename</i></td><td>directory where to look for plugins/components</td></tr>
 *  <tr><td nowrap>--pass <i>password</i></td><td>password for decrypting of hidden connections passwords</td></tr>
 *  <tr><td nowrap>--stdin</td><td>load graph layout from STDIN</td></tr>
 *  <tr><td nowrap>--mode</td><td>how show data over component {TEXT,HTML,TABLE}</td></tr>
 *  <tr><td nowrap>--delimiter</td><td>delimiter between two fields</td></tr>
 *  <tr><td nowrap>--file</td><td>file url for output. If no file defined, output is set to System.out</td></tr>
 *  <tr><td nowrap>--expFilter</td><td>filter expression for record filtering</td></tr>
 *  <tr><td nowrap>--recFrom</td><td>from where show records</td></tr>
 *  <tr><td nowrap>--recCount</td><td>how many records should be showed</td></tr>
 *  <tr><td nowrap>--fields</td><td>Show only defined fields. If no fields defined, show all fields</td></tr>
 *  <tr><td nowrap>--logLevel</td><td>Log level for logger {all, info, debug, ..}, default is error log level</td></tr>
 *  <tr><td nowrap><b>filename</b></td><td>filename or URL of the file (even remote) containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  <tr><td nowrap><b>component id</b></td><td>over this component will be showed data</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>showData ExampleGraph.grf DELIMITED_DATA_READER0:0</pre>
 *  
 *  <pre>showData --plugins ../cloveretl.engine/plugins --mode TEXT --delimiter "+" --expFilter "1==1" --recFrom 2 --recCount 2 --fields "Field1;Field0" ExampleGraph.grf DELIMITED_DATA_READER0:0</pre>
 *  
 *  </pre></tt>
 * @author      jausperger
 * @since       2007/02/02
 * @revision    $Revision:  $
 */
public class showData {
    private static Log logger = LogFactory.getLog(showData.class);

    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginsRootDirectory directory path, where plugins specification is located 
     *        (can be null, then is used constant from Defaults.DEFAULT_PLUGINS_DIRECTORY)
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections passwordss can be encrypted
     */        int trackingInterval = -1;

    public static void initEngine(String pluginsRootDirectory, String password) {
        
        //init password decryptor
        if(password != null) {
            Enigma.getInstance().init(password);
        }
        
        //init framework constants
        Defaults.init();

        //init clover plugins system
        Plugins.init(pluginsRootDirectory);
    }
    
    
    /**
     * Instantiates transformation graph from a given input stream and presets a given properties.
     * @param inStream
     * @param properties
     * @return
     * @throws XMLConfigurationException
     * @throws GraphConfigurationException
     */
    public static TransformationGraph loadGraph(InputStream inStream, Properties properties) throws XMLConfigurationException, GraphConfigurationException {
        TransformationGraph graph = new TransformationGraph();
        TransformationGraphXMLReaderWriter graphReader = new TransformationGraphXMLReaderWriter(graph);
        if(properties != null) {
            graph.loadGraphProperties(properties);
        }

        graphReader.read(inStream);
        
        return graph;
    }
    
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
        boolean loadFromSTDIN = false;
		Properties properties = new Properties();
        int trackingInterval = -1;
		String pluginsRootDirectory = null;
        String password = null;
        Mode viewMode = Mode.TEXT;
        String delimiter = null;
        String fileUrl = null;
        String filterExpression = null;
        long recordFrom = -1;
        long recordCount = -1;
        String fields = null;
        String logHost = null;
		
		Node extFilter = null;
        
		/*System.out.println("***  CloverETL graph component tester ver "+RUN_GRAPH_VERSION+", (c) 2002-06 D.Pavlis, released under GNU Lesser General Public License  ***");
		System.out.println(" Running with framework version: "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
		System.out.println();
        */
		if (args.length < 1) {
			printHelp();
			System.exit(-1);
		}
        
		Options options = new Options();
    	options.addOption(new Option("g", "cfg", true, "Path to property file"));
    	options.addOption(new Option("P", "propertyDefinition", true, "Property defined by user"));
    	options.addOption(new Option("t", "tracking", false, "Tracking intrnal switch"));
    	options.addOption(new Option("i", "info", false, "Information about program"));
    	options.addOption(new Option("n", "stdin", false, "Load from stdin switch"));
    	options.addOption(new Option("h", "loghost", false, "Log host switch"));
    	options.addOption(new Option("p", "plugins", true, "Path to plugins file."));
    	options.addOption(new Option("s", "pass", true, "Password"));
    	options.addOption(new Option("m", "mode", true, "View mode"));
    	options.addOption(new Option("d", "delimiter", true, "Delimiter between two fields"));
    	options.addOption(new Option("o", "file", true, "File url for output. If no file defined, output is set to System.out"));
    	options.addOption(new Option("e", "expFilter", true, "Filter expression for record filtering"));
    	options.addOption(new Option("f", "recFrom", true, "From where show records"));
    	options.addOption(new Option("c", "recCount", true, "Count of records"));
    	options.addOption(new Option("l", "fields", true, "Show only defined fields. If no fields defined, show all fields"));
    	options.addOption(new Option("x", "logLevel", true, "Log level for logger {all, info, debug, ..}, default is error log level"));

    	PosixParser optParser = new PosixParser();
    	CommandLine cmdLine;
		try {
			cmdLine = optParser.parse(options, args);
		} catch (ParseException e) {
			logger.error(e.getMessage(), e);
			return;
		}
		
		loadFromSTDIN = cmdLine.hasOption("n");
		if (cmdLine.hasOption("h")) {
            String[] hostAndPort = logHost.split(":");
            if (hostAndPort[0].length() == 0 || hostAndPort.length > 2) {
                System.err
                        .println("Invalid log destination, i.e. -loghost localhost:4445");
                System.exit(-1);
            }
            int port = 4445;
            try {
                if (hostAndPort.length == 2) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            } catch (NumberFormatException e) {
                System.err
                        .println("Invalid log destination, i.e. -loghost localhost:4445");
                System.exit(-1);
            }
            Logger.getRootLogger().addAppender(
                    new SocketAppender(hostAndPort[0], port));
		}
		if (cmdLine.hasOption("g")) {
			InputStream inStream;
			try {
				inStream = new BufferedInputStream(new FileInputStream(cmdLine.getOptionValue("g")));
				properties.load(inStream);
			} catch (NullPointerException e) {
				logger.error("cfg file not found: " + e.getMessage(), e);
				System.exit(-1);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
				System.exit(-1);
			}
		}
		if (cmdLine.hasOption("P")) {
	    	String a[] = cmdLine.getOptionValue("P").split(";");
	    	for (String tmp : a) {
	    	    properties.setProperty(tmp.substring(0,tmp.indexOf("=")),tmp.substring(tmp.indexOf("=") +1)); 
	    	}
		}
		if (cmdLine.hasOption("i")) {
		    printInfo();
		    System.exit(0);
		}
		if (cmdLine.hasOption("t")) {
            try {
                trackingInterval = Integer.parseInt(cmdLine.getOptionValue("t"));
            } catch (NumberFormatException ex) {
                System.err.println("Invalid tracking parameter: \""
                        + cmdLine.getOptionValue("t") + "\"");
                System.exit(-1);
            }
		}
		if (cmdLine.hasOption("p")) {
			pluginsRootDirectory = cmdLine.getOptionValue("p");
		}
		if (cmdLine.hasOption("s")) {
			password = cmdLine.getOptionValue("s");
		}
		if (cmdLine.hasOption("m")) {
			viewMode = Mode.valueModeOf(cmdLine.getOptionValue("m"));
	        if (viewMode == null) {
				System.err.println("Unknown mode option: "+cmdLine.getOptionValue("m"));
				System.exit(-1);
	        }
		}
		if (cmdLine.hasOption("d")) {
			delimiter = cmdLine.getOptionValue("d");
		}
		if (cmdLine.hasOption("o")) {
			fileUrl = cmdLine.getOptionValue("o");
		}
		if (cmdLine.hasOption("e")) {
			filterExpression = cmdLine.getOptionValue("e");
		}
		if (cmdLine.hasOption("f")) {
	    	recordFrom = Long.parseLong(cmdLine.getOptionValue("f"));
		}
		if (cmdLine.hasOption("c")) {
	    	recordCount = Long.parseLong(cmdLine.getOptionValue("c"));
		}
		if (cmdLine.hasOption("l")) {
        	fields = cmdLine.getOptionValue("l");
		}
		Logger.getRootLogger().setLevel(cmdLine.hasOption("x")?Level.toLevel(cmdLine.getOptionValue("x")):Level.ERROR);
		
        // setup log4j appenders
        if (logHost != null) {
            String[] hostAndPort = logHost.split(":");
            if (hostAndPort[0].length() == 0 || hostAndPort.length > 2) {
                System.err
                        .println("Invalid log destination, i.e. -loghost localhost:4445");
                System.exit(-1);
            }
            int port = 4445;
            try {
                if (hostAndPort.length == 2) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            } catch (NumberFormatException e) {
                System.err
                        .println("Invalid log destination, i.e. -loghost localhost:4445");
                System.exit(-1);
            }
            Logger.getRootLogger().addAppender(
                    new SocketAppender(hostAndPort[0], port));
        }
        
        // engine initialization - should be called only once
        runGraph.initEngine(pluginsRootDirectory, password);

        // prapere input stream with XML graph definition
        InputStream in = null;
        if (loadFromSTDIN) {
            System.out.println("Graph definition loaded from STDIN");
            in = System.in;
        } else {
            System.out.println("Graph definition file: "
                    + args[args.length - 2]);
            try {
                URL fileURL = FileUtils.getFileURL(null, args[args.length - 2]);
                in = fileURL.openStream();
            } catch (IOException e) {
                System.err
                        .println("Error - graph definition file can't be read: "
                                + e.getMessage());
                System.exit(-1);
            }
        }
        
        System.out.println("Component id: " + args[args.length - 1]);
        String componentID = args[args.length - 1];
        int pos;
        int port = 0;
        if ((pos = componentID.indexOf(':')) != -1) {
        	port = Integer.parseInt(componentID.substring(pos+1));
        	componentID = componentID.substring(0, pos);
        }

        // loading graph from the input stream
        TransformationGraph graph = null;
        try {
            graph = runGraph.loadGraph(in, properties);

            // check graph elements configuration
            logger.info("Checking graph configuration...");
            try {
                ConfigurationStatus status = graph.checkConfig(null);
                status.log();
            } catch(Exception e) {
                logger.error("Checking graph failed! (" + e.getMessage() + ")");
            }

            if (!graph.init()) {
                throw new GraphConfigurationException(
                        "Graph initialization failed.");
            }

            // this can be called only after graph.init()
            graph.dumpGraphConfiguration();
        } catch (XMLConfigurationException ex) {
            logger.error("Error in reading graph from XML !", ex);
            ex.printStackTrace(System.err);
            System.exit(-1);
        } catch (GraphConfigurationException ex) {
            logger.error("Error - graph's configuration invalid !", ex);
            ex.printStackTrace(System.err);
            System.exit(-1);
        } catch (RuntimeException ex) {
            logger.error("Error during graph initialization !", ex);
            ex.printStackTrace(System.err);
            System.exit(-1);
        }
        
        //check graph elements configuration
        ConfigurationStatus status = graph.checkConfig(null);
        status.log();
        
		Map map = graph.getNodes();
		Node node = (Node) map.get(componentID);
		if (node == null) {
			//map = graph.getEdges();
			//Edge edge = (Edge) map.get(componentID);
			//if (edge == null) {
				// error
				logger.error("Component Id '"+ componentID +"' not found!");
				return;
			//}
		}
		if (!node.isRoot()) {
			// not implemented
			System.err.println("Execution is implemented for root node (root node has only output ports connected)!");
			return;
		}

		// create new graph
	    TransformationGraph viewGraph = new TransformationGraph();
		Phase _PHASE_1 = new Phase(1);

	    // add writer component
		DataRecordMetadata dataRecordMetadata = (DataRecordMetadata) node.getOutMetadata().toArray()[port];
		Edge edge0 = new Edge("EDGE0", dataRecordMetadata);
		Edge edge1 = null;
		if (filterExpression != null) {
			edge1 = new Edge("EDGE1", dataRecordMetadata);
		}
		Node writer = null;
		try {
			writer = getWriter(viewGraph, viewMode, dataRecordMetadata, fileUrl, delimiter, recordFrom, recordCount, fields);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		// add Edges & Nodes & Phases to graph
		try {
			viewGraph.addPhase(_PHASE_1);
			viewGraph.addEdge(edge0);
			_PHASE_1.addNode(node);
			_PHASE_1.addNode(writer);
			
			if (filterExpression != null) {
				viewGraph.addEdge(edge1);
				extFilter = ComponentFactory.createComponent(viewGraph, "EXT_FILTER", new Object[] {"ExtFilter0"}, new Class[] {String.class});//new ExtFilter("ExtFilter0");
				Method method = ComponentFactory.getComponentClass("EXT_FILTER").getMethod("setFilterExpression", new Class[] {String.class});
				method.invoke(extFilter, filterExpression);
				_PHASE_1.addNode(extFilter);
			}
		} catch (GraphConfigurationException e) {
			e.printStackTrace();
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		// assign ports (input & output)
		node.addOutputPort(0,edge0);
		if (filterExpression != null) {
			extFilter.addInputPort(0,edge0);
			extFilter.addOutputPort(0,edge1);
			writer.addInputPort(0,edge1); 
		} else {
			writer.addInputPort(0,edge0);
		}

		if(!viewGraph.init()){
			System.err.println("Graph initialization failed !");
			return;
		}
	    
        // set tracking interval
        if (trackingInterval != -1) {
        	viewGraph.setTrackingInterval(trackingInterval * 1000);
        }

        //	start all Nodes (each node is one thread)
		Result result=Result.N_A;
		try {
            result = viewGraph.run();
        } catch (RuntimeException ex) {
            System.err.println("Fatal error during graph run !");
            System.err.println(ex.getCause().getMessage());
            ex.printStackTrace();
            System.exit(-1);
        }
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
    
	/**
	 * @param mode
	 * @param dataRecordMetadata
	 * @param fileUrl
	 * @param delimiter
	 * @param recordFrom
	 * @param recordCount
	 * @param fields
	 * @return
	 * @throws Exception 
	 */
	private static Node getWriter(TransformationGraph graph, Mode mode, DataRecordMetadata dataRecordMetadata, String fileUrl, String delimiter, long recordFrom, long recordCount, String fields) throws Exception {
		if (mode == null) return null;
		Node writer = null;
		String[] aFiealds = fields == null ? null : fields.split(";");
		
		// html formating
		if (mode.equals(Mode.HTML)) {
			StringBuilder maskBuilder = new StringBuilder();
			if (aFiealds == null) {
				maskBuilder.append("<tr>");
				for (int i=0;i<dataRecordMetadata.getNumFields();i++){
					maskBuilder.append("<td>$");
					maskBuilder.append(dataRecordMetadata.getField(i).getName());
					maskBuilder.append("</td>");
				}
				maskBuilder.append("</tr>\n");
			} else if (aFiealds.length > 0) {
				maskBuilder.append("<tr>");
				for (String sfield: aFiealds){
					if (dataRecordMetadata.getField(sfield) == null) {
						System.err.println("Field name '"+ sfield + "' not found.");
						System.exit(-1);
					}
					maskBuilder.append("<td>$");
					maskBuilder.append(sfield);
					maskBuilder.append("</td>");
				}
				maskBuilder.append("</tr>\n");
			} else {
				System.err.println("No field found.");
				System.exit(-1);
			}
			
			Node structureWriter = ComponentFactory.createComponent(graph, "STRUCTURE_WRITER", new Object[] {"STRUCTURE_WRITER0", fileUrl, null, false, maskBuilder.toString()}, new Class[] {String.class, String.class, String.class, boolean.class, String.class});
			Class cStructureWriter = ComponentFactory.getComponentClass("STRUCTURE_WRITER");
			Method method = cStructureWriter.getMethod("setRecordFrom", new Class[] {long.class});
			method.invoke(structureWriter, recordFrom);
			method = cStructureWriter.getMethod("setRecordCount", new Class[] {long.class});
			method.invoke(structureWriter, recordCount);
			StringBuilder sb = new StringBuilder();
			
			sb.append("<table name=\"" + dataRecordMetadata.getName() + "\" border=1>\n");
			sb.append("<thead>\n<tr>");
			if (aFiealds == null ) {
				for (int i=0;i<dataRecordMetadata.getNumFields();i++){
					sb.append("<th>");
					sb.append(dataRecordMetadata.getField(i).getName());
					sb.append("</th>");
				}
			} else {
				for (String sfield: aFiealds){
					sb.append("<th>");
					sb.append(dataRecordMetadata.getField(sfield).getName());
					sb.append("</th>");
				}
			}
			sb.append("</tr>\n</thead>\n<tbody>\n");
			method = cStructureWriter.getMethod("setHeader", new Class[] {String.class});
			method.invoke(structureWriter, sb.toString());
			
			sb = new StringBuilder();
			sb.append("</tbody>\n</table>\n");
			method = cStructureWriter.getMethod("setFooter", new Class[] {String.class});
			method.invoke(structureWriter, sb.toString());
			writer = structureWriter;
			
		// text formating - delimited or fix lenght
		} else if (mode.equals(Mode.TEXT)) {
			Node dataWriter = ComponentFactory.createComponent(graph, "DATA_WRITER", new Object[] {"DATA_WRITER0", fileUrl, dataRecordMetadata.getLocaleStr(), false}, new Class[] {String.class, String.class, String.class, boolean.class});
			//TODO agata dodelat selekci na fieldy
			Class cDataWriter = ComponentFactory.getComponentClass("DATA_WRITER");
			Method method = cDataWriter.getMethod("setRecordFrom", new Class[] {long.class});
			method.invoke(dataWriter, recordFrom);
			method = cDataWriter.getMethod("setRecordCount", new Class[] {long.class});
			method.invoke(dataWriter, recordCount);
			if (delimiter != null) {
				method = cDataWriter.getMethod("setDataDelimiter", new Class[] {String.class});
				method.invoke(dataWriter, delimiter);
			}
			writer = dataWriter;
			
		// table formating
		} else if (mode.equals(Mode.TABLE)) {
			//TextWriter dataWriter = new TextWriter("TEXT_TABLE_WRITER0", fileUrl, null, false, aFiealds);
			Node textWriter = ComponentFactory.createComponent(graph, "TEXT_TABLE_WRITER", new Object[] {"TEXT_TABLE_WRITER0", fileUrl, null, false, aFiealds}, new Class[] {String.class, String.class, String.class, boolean.class, String[].class});
			Class cDataWriter = ComponentFactory.getComponentClass("TEXT_TABLE_WRITER");
			Method method = cDataWriter.getMethod("setRecordFrom", new Class[] {long.class});
			method.invoke(textWriter, recordFrom);
			method = cDataWriter.getMethod("setRecordCount", new Class[] {long.class});
			method.invoke(textWriter, recordCount);
			method = cDataWriter.getMethod("setHeader", new Class[] {boolean.class});
			method.invoke(textWriter, true);
			writer = textWriter;
		}
		return writer;
	}
	
	private static void printHelp() {
		System.out.println("Usage: showData [-P] [--(cfg|tracking|info|plugins|pass|loghost|mode|delimiter|file|expFilter|recFrom|recCount|fields|logLevel)] <graph definition file> <component id>");
		System.out.println("Options:");
		System.out.println("-P:<key>=<value>\tadd definition of property to global graph's property list");
		System.out.println("--cfg <filename>\t\tload definitions of properties from specified file");
		System.out.println("--tracking <seconds>\thow frequently output the graph processing status");
		System.out.println("--info\t\t\tprint info about Clover library version");
        System.out.println("--plugins\t\tdirectory where to look for plugins/components");
        System.out.println("--pass\t\tpassword for decrypting of hidden connections passwords");
        System.out.println("--stdin\t\tload graph definition from STDIN");
        System.out.println("--loghost\t\tdefine host and port number for socket appender of log4j (log4j library is required); i.e. localhost:4445");
        System.out.println("--mode\t\thow show data over component {TEXT,HTML,TABLE}");
        System.out.println("--delimiter\t\tdelimiter between two fields");
        System.out.println("--file\t\tfile url for output. If no file defined, output is set to System.out");
        System.out.println("--expFilter\t\tfilter expression for record filtering");
        System.out.println("--recFrom\t\tfrom where show records");
        System.out.println("--recCount\t\thow many records should be showed");
        System.out.println("--fields\t\tShow only defined fields. If no fields defined, show all fields");
        System.out.println("--logLevel\t\tLog level for logger {all, info, debug, ..}, default is error log level");
        System.out.println();
        System.out.println("Note: <graph definition file> can be either local filename or URL of local/remote file");
        System.out.println("Note: <component id> over this component will be showed data");
	}

	private static void printInfo(){
	    System.out.println("CloverETL library version "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
	}

	public enum Mode {
	    
	    TEXT,
	    HTML,
	    TABLE;

	    public static Mode valueModeOf(String value){
	    	if (value.equalsIgnoreCase(TEXT.name())) {
	    		return TEXT;
	    	}
	    	if (value.equalsIgnoreCase(HTML.name())) {
	    		return HTML;
	    	}
	    	if (value.equalsIgnoreCase(TABLE.name())) {
	    		return TABLE;
	    	}
	    	return null;
	    }
	    
	}
	
}

