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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.plugin.Plugins;
import org.jetel.util.Enigma;
import org.jetel.util.JetelVersion;

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
 *  <tr><td nowrap><b>filename</b></td><td>name of the file containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  </table>
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 * @revision    $Revision$
 */
public class runGraph {
    private static Log logger = LogFactory.getLog(runGraph.class);

    //TODO change run graph version
	private final static String RUN_GRAPH_VERSION = "1.8";
	public final static String VERBOSE_SWITCH = "-v";
	public final static String PROPERTY_FILE_SWITCH = "-cfg";
	public final static String PROPERTY_DEFINITION_SWITCH = "-P:";
	public final static String TRACKING_INTERVAL_SWITCH = "-tracking";
	public final static String INFO_SWITCH = "-info";
    public final static String PLUGINS_SWITCH = "-plugins";
    public final static String PASSWORD_SWITCH = "-pass";
	
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
		boolean verbose = false;
		Properties properties=new Properties();
		int trackingInterval=-1;
		String pluginsRootDirectory = null;
        Defaults.init();
		
		System.out.println("***  CloverETL framework/transformation graph runner ver "+RUN_GRAPH_VERSION+", (c) 2002-06 D.Pavlis, released under GNU Lesser General Public License  ***");
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
					System.err.println(ex.getMessage());
					System.exit(-1);
				}
			}else if (args[i].startsWith(PROPERTY_DEFINITION_SWITCH)){
			   	//String[] nameValue=args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"").split("=");
				//properties.setProperty(nameValue[0],nameValue[1]);
		    	String tmp =  args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"");
        	    properties.setProperty(tmp.substring(0,tmp.indexOf("=")),tmp.substring(tmp.indexOf("=") +1)); 
			}else if (args[i].startsWith(TRACKING_INTERVAL_SWITCH)) {
				i++;
				trackingInterval = Integer.parseInt(args[i]);
			}else if (args[i].startsWith(INFO_SWITCH)){
			    printInfo();
			    System.exit(0);
            }else if (args[i].startsWith(PLUGINS_SWITCH)){
                i++;
                pluginsRootDirectory = args[i];
            }else if (args[i].startsWith(PASSWORD_SWITCH)){
                i++;
                Enigma.getInstance().init(args[i]);
            }else if (args[i].startsWith("-")) {
				System.err.println("Unknown option: "+args[i]);
				System.exit(-1);
			}
		}
		
        //init clover plugins system
        Plugins.init(pluginsRootDirectory);
        
		FileInputStream in=null;
		System.out.println("Graph definition file: " + args[args.length - 1]);

		try {
			in = new FileInputStream(args[args.length - 1]);
		} catch (FileNotFoundException e) {
			System.err.println("Error - graph definition file not found: "+e.getMessage());
			System.exit(-1);
		}

		TransformationGraph graph = new TransformationGraph();
        TransformationGraphXMLReaderWriter graphReader = new TransformationGraphXMLReaderWriter(graph);
		graph.loadGraphProperties(properties);

		try {
			graphReader.read(in);
			graph.init();
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
		// set tracking interval
		if(trackingInterval!=-1){
			graph.setTrackingInterval(trackingInterval*1000);
		}
		
		//	start all Nodes (each node is one thread)
		boolean finishedOK = false;
		try {
			finishedOK = graph.run();
		} catch (RuntimeException ex) {
			System.err.println("Fatal error during graph run !");
			System.err.println(ex.getCause().getMessage());
			if (verbose) {
				ex.printStackTrace();
			}
			System.exit(-1);
		}
		if (finishedOK) {
			// everything O.K.
			System.out.println("Execution of graph successful !");
			System.exit(0);
		} else {
			// something FAILED !!
			System.err.println("Execution of graph failed !");
			System.exit(-1);
		}

	}
    
    
	private static void printHelp() {
		System.out.println("Usage: runGraph [-(v|cfg|P:|tracking|info|plugins|pass)] <graph definition file>");
		System.out.println("Options:");
		System.out.println("-v\t\t\tbe verbose - print even graph layout");
		System.out.println("-P:<key>=<value>\tadd definition of property to global graph's property list");
		System.out.println("-cfg <filename>\t\tload definitions of properties from specified file");
		System.out.println("-tracking <seconds>\thow frequently output the graph processing status");
		System.out.println("-info\t\t\tprint info about Clover library version");
        System.out.println("-plugins\t\tdirectory where to look for plugins/components");
        System.out.println("-pass\t\tpassword for decrypting of hidden connections passwords");
	}

	private static void printInfo(){
	    System.out.println("CloverETL library version "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
	}
	
}

