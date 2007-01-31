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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.DataWriter;
import org.jetel.component.ExtFilter;
import org.jetel.component.StructureWriter;
import org.jetel.component.TextWriter;
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
 *  class for executing transformations described in XML layout file<br><br>
 *  The graph layout is read from specified XML file and the whole transformation is executed.<br>
 *  <tt><pre>
 *  Program parameters:
 *  <table>
 *  <tr><td nowrap>-v</td><td>be verbose - print even graph layout</td></tr>
 *  <tr><td nowrap>-P:<i>properyName</i>=<i>propertyValue</i></td><td>add definition of property to global graph's property list</td></tr>
 *  <tr><td nowrap>-cfg <i>filename</i></td><td>load definitions of properties from specified file</td></tr>
 *  <tr><td nowrap>-tracking <i>seconds</i></td><td>how frequently output the processing status</td></tr>
 *  <tr><td nowrap>-info</td><td>print info about Clover library version</td></tr>
 *  <tr><td nowrap>-plugins <i>filename</i></td><td>directory where to look for plugins/components</td></tr>
 *  <tr><td nowrap>-pass <i>password</i></td><td>password for decrypting of hidden connections passwords</td></tr>
 *  <tr><td nowrap>-stdin</td><td>load graph layout from STDIN</td></tr>
 *  <tr><td nowrap><b>filename</b></td><td>filename or URL of the file (even remote) containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  </table>
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 * @revision    $Revision: 2080 $
 */
public class DataComponentViewer {
    private static Log logger = LogFactory.getLog(DataComponentViewer.class);

    //TODO change run graph version
	private final static String RUN_GRAPH_VERSION = "2.0";
	public final static String VERBOSE_SWITCH = "-v";
	public final static String PROPERTY_FILE_SWITCH = "-cfg";
	public final static String PROPERTY_DEFINITION_SWITCH = "-P:";
	public final static String INFO_SWITCH = "-info";
    public final static String PLUGINS_SWITCH = "-plugins";
    public final static String PASSWORD_SWITCH = "-pass";
    public final static String VIEW_MODE = "-mode";
    public final static String DELIMITER = "-delimiter";
    public final static String OUT_FILE = "-file";
	public final static String FILTER_EXPRESSION = "-expFilter";
	public final static String RECORD_FROM = "-recFrom";
	public final static String RECORD_COUNT = "-recCount";
	public final static String FIELDS = "-fields";

	
    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginsRootDirectory directory path, where plugins specification is located 
     *        (can be null, then is used constant from Defaults.DEFAULT_PLUGINS_DIRECTORY)
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections passwordss can be encrypted
     */
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
        
        if(!graph.init()) {
            throw new GraphConfigurationException("Graph initialization failed.");
        }
        
        return graph;
    }
    
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
		boolean verbose = false;
		Properties properties = new Properties();
		String pluginsRootDirectory = null;
        String password = null;
        Mode viewMode = Mode.TEXT;
        String delimiter = null;
        String fileUrl = null;
        String filterExpression = null;
        long recordFrom = -1;
        long recordCount = -1;
        String fields = null;
		
		ExtFilter extFilter = null;
        
		System.out.println("***  CloverETL graph component tester ver "+RUN_GRAPH_VERSION+", (c) 2002-06 D.Pavlis, released under GNU Lesser General Public License  ***");
		System.out.println(" Running with framework version: "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
		System.out.println();
        
		if (args.length < 1) {
			printHelp();
			System.exit(-1);
		}
        
		// process command line arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith(VERBOSE_SWITCH)) {
				verbose = true;
			}else if (args[i].startsWith(PROPERTY_FILE_SWITCH)){
				i++;
				try {
					InputStream inStream = new BufferedInputStream(new FileInputStream(args[i]));
					properties.load(inStream);
				} catch (IOException ex) {
					logger.error(ex.getMessage(), ex);
					System.exit(-1);
				}
			}else if (args[i].startsWith(PROPERTY_DEFINITION_SWITCH)){
			   	//String[] nameValue=args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"").split("=");
				//properties.setProperty(nameValue[0],nameValue[1]);
		    	String tmp =  args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"");
        	    properties.setProperty(tmp.substring(0,tmp.indexOf("=")),tmp.substring(tmp.indexOf("=") +1)); 
			}else if (args[i].startsWith(INFO_SWITCH)){
			    printInfo();
			    System.exit(0);
            }else if (args[i].startsWith(PLUGINS_SWITCH)){
                i++;
                pluginsRootDirectory = args[i];
            }else if (args[i].startsWith(PASSWORD_SWITCH)){
                i++;
                password = args[i]; 
            }else if (args[i].startsWith(VIEW_MODE)){
                i++;
                viewMode = Mode.valueModeOf(args[i]);
                if (viewMode == null) {
    				System.err.println("Unknown mode option: "+args[i]);
    				System.exit(-1);
                }
            }else if (args[i].startsWith(DELIMITER)){
            	delimiter = args[i].substring(args[i].indexOf("=") + 1);
            }else if (args[i].startsWith(OUT_FILE)){
            	fileUrl = args[i].substring(args[i].indexOf("=") + 1);
            }else if (args[i].startsWith(FILTER_EXPRESSION)){
            	filterExpression = args[i].substring(args[i].indexOf("=") + 1);
            }else if (args[i].startsWith(RECORD_FROM)){
            	recordFrom = Long.parseLong(args[i].substring(args[i].indexOf("=") + 1));
            }else if (args[i].startsWith(RECORD_COUNT)){
            	recordCount = Long.parseLong(args[i].substring(args[i].indexOf("=") + 1));
            }else if (args[i].startsWith(FIELDS)){
            	fields = args[i].substring(args[i].indexOf("=") + 1);
            }else if (args[i].startsWith("-")) {
				System.err.println("Unknown option: "+args[i]);
				System.exit(-1);
			}
		}
		
        //engine initialization - should be called only once
        DataComponentViewer.initEngine(pluginsRootDirectory, password);
        
		//prapere input stream with XML graph definition
        InputStream in = null;
        System.out.println("Graph definition file: " + args[args.length - 2]);
        URL fileURL = null;
		try {
			fileURL = FileUtils.getFileURL(null, args[args.length - 2]);
		} catch (MalformedURLException e1) {
            System.err.println("Error - graph definition file can't be read.");
            System.exit(-1);
		}
        if(fileURL == null) {
            System.err.println("Error - graph definition file can't be read.");
            System.exit(-1);
        }
        try{
            in=fileURL.openStream();
        } catch (IOException e) {
            System.err.println("Error - graph definition file can't be read: " + e.getMessage());
            System.exit(-1);
        }
        
        System.out.println("Component id: " + args[args.length - 1]);
        String componentID = args[args.length - 1];
        int pos;
        int port = 0;
        if ((pos = componentID.indexOf(':')) != -1) {
        	port = Integer.parseInt(componentID.substring(pos+1));
        	componentID = componentID.substring(0, pos);
        }
        
        //loading graph from the input stream
        TransformationGraph graph = null;
        try {
            graph = DataComponentViewer.loadGraph(in, properties);

            if (verbose) {
                //this can be called only after graph.init()
                graph.dumpGraphConfiguration();
            }
        }catch(XMLConfigurationException ex){
            logger.error("Error in reading graph from XML !", ex);
            if (verbose) {
                ex.printStackTrace(System.err);
            }
            System.exit(-1);
        }catch(GraphConfigurationException ex){
            logger.error("Error - graph's configuration invalid !", ex);
            if (verbose) {
                ex.printStackTrace(System.err);
            }
            System.exit(-1);
        } catch (RuntimeException ex) {
            logger.error("Error during graph initialization !", ex);
            if (verbose) {
                ex.printStackTrace(System.err);
            }
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
				System.err.println("Error");
				return;
			//}
		}
		if (!node.isRoot()) {
			// not implemented
			System.err.println("Execution is implemented for root node (has only output ports connected to id)!");
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
		Node writer = getWriter(viewMode, dataRecordMetadata, fileUrl, delimiter, recordFrom, recordCount, fields);
		
		// add Edges & Nodes & Phases to graph
		try {
			viewGraph.addPhase(_PHASE_1);
			viewGraph.addEdge(edge0);
			_PHASE_1.addNode(node);
			_PHASE_1.addNode(writer);
			
			if (filterExpression != null) {
				viewGraph.addEdge(edge1);
				extFilter = new ExtFilter("ExtFilter0");
				extFilter.setFilterExpression(filterExpression);
				_PHASE_1.addNode(extFilter);
			}
		} catch (GraphConfigurationException e) {
			e.printStackTrace();
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
	    
		//	start all Nodes (each node is one thread)
		Result result=Result.N_A;
		try {
            result = viewGraph.run();
		} catch (RuntimeException ex) {
			System.err.println("Fatal error during graph run !");
			System.err.println(ex.getCause().getMessage());
			if (verbose) {
				ex.printStackTrace();
			}
			System.exit(-1);
		}
		if (result==Result.FINISHED_OK) {
			// everything O.K.
			System.out.println("Execution of graph successful !");
			System.exit(0);
		} else {
			// something FAILED !!
			System.err.println("Execution of graph failed !");
			System.exit(result.code());
		}

	}
    
	private static Node getWriter(Mode mode, DataRecordMetadata dataRecordMetadata, String fileUrl, String delimiter, long recordFrom, long recordCount, String fields) {
		if (mode == null) return null;
		Node writer = null;
		String[] aFiealds = fields == null ? null : fields.split(";");
		
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
			
			StructureWriter structureWriter = new StructureWriter("STRUCTURE_WRITER0", fileUrl, null, false, maskBuilder.toString());
			structureWriter.setRecordFrom(recordFrom);
			structureWriter.setRecordCount(recordCount);
			StringBuilder sb = new StringBuilder();
			
			sb.append("<table name=\"" + dataRecordMetadata.getName() + "\" border=1>\n");
			sb.append("<tr>");
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
			sb.append("</tr>\n");
			structureWriter.setHeader(sb.toString());
			
			sb = new StringBuilder();
			sb.append("</table>\n");
			structureWriter.setFooter(sb.toString());
			
			writer = structureWriter;
			
		} else if (mode.equals(Mode.TEXT)) {
			DataWriter dataWriter = new DataWriter("DATA_WRITER0", fileUrl, dataRecordMetadata.getLocaleStr(), false);
			//TODO agata dodelat selekci na fieldy
			dataWriter.setRecordFrom(recordFrom);
			dataWriter.setRecordCount(recordCount);
			if (delimiter != null) dataWriter.setDataDelimiter(delimiter);
			writer = dataWriter;
			
		} else if (mode.equals(Mode.DELIMITER_TEXT)) {
			TextWriter dataWriter = new TextWriter("TEXT_TABLE_WRITER0", fileUrl, null, false, aFiealds);
			dataWriter.setRecordFrom(recordFrom);
			dataWriter.setRecordCount(recordCount);
			dataWriter.setHeader(true);
			
			writer = dataWriter;
			/*   +--+--+
				 |  |  |
			     +--+--+
			*/
		}
		return writer;
	}
	
	private static void printHelp() {
		System.out.println("Usage: runGraph [-(v|cfg|P:|info|plugins|pass)] <graph definition file> <component id>");
		System.out.println("Options:");
		System.out.println("-v\t\t\tbe verbose - print even graph layout");
		System.out.println("-P:<key>=<value>\tadd definition of property to global graph's property list");
		System.out.println("-cfg <filename>\t\tload definitions of properties from specified file");
		System.out.println("-info\t\t\tprint info about Clover library version");
        System.out.println("-plugins\t\tdirectory where to look for plugins/components");
        System.out.println("-pass\t\tpassword for decrypting of hidden connections passwords");
        System.out.println();
        System.out.println("Note: <graph definition file> can be either local filename or URL of local/remote file");
	}

	private static void printInfo(){
	    System.out.println("CloverETL library version "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
	}
	
	public enum Mode {
	    
	    TEXT,
	    HTML,
	    DELIMITER_TEXT;

	    public static Mode valueModeOf(String value){
	    	if (value.equalsIgnoreCase(TEXT.name())) {
	    		return TEXT;
	    	}
	    	if (value.equalsIgnoreCase(HTML.name())) {
	    		return HTML;
	    	}
	    	if (value.equalsIgnoreCase(DELIMITER_TEXT.name())) {
	    		return DELIMITER_TEXT;
	    	}
	    	return null;
	    }
	    
	}
	
}

