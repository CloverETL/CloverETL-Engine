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

import org.jetel.component.ComponentDescriptionReader;
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
import org.jetel.util.JetelVersion;

/**
 *  class for executing transformations described in XML layout file<br><br>
 *  The graph layout is read from specified XML file and the whole transformation is executed.<br>
 *  <tt><pre>
 *  Program parameters:
 *  <table>
 *  <tr><td nowrap>-v</td><td>be verbose - print even graph layout</td></tr>
 *  <tr><td nowrap>-log</td><i>logfile</i><td>send output messages to specified logfile instead of stdout</td></tr>
 *  <tr><td nowrap>-P:<i>properyName</i>=<i>propertyValue</i></td><td>add definition of property to global graph's property list</td></tr>
 *  <tr><td nowrap>-properties <i>filename</i></td><td>load definitions of properties form specified file</td></tr>
 *  <tr><td nowrap>-tracking <i>seconds</i></td><td>how frequently output the processing status</td></tr>
 *  <tr><td nowrap>-info</td><td>print info about Clover library version</td></tr>
 *  <tr><td nowrap>-register <i>filename</i></td><td>load/register additional transformation components</td></tr>
 *  <tr><td nowrap><b>filename</b></td><td>name of the file containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  </table>
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 * @revision    $Revision$
 */
public class runGraph {

	private final static String RUN_GRAPH_VERSION="1.8";
	private final static String VERBOSE_SWITCH = "-v";
	public final static String PROPERTY_FILE_SWITCH = "-cfg";
	private final static String PROPERTY_DEFINITION_SWITCH = "-P:";
	private final static String TRACKING_INTERVAL_SWITCH = "-tracking";
	private final static String INFO_SWITCH= "-info";
    private final static String REGISTER_SWITCH= "-register";
    private final static String PLUGINS_SWITCH= "-plugins";
	
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
		boolean verbose = false;
		Properties properties=new Properties();
		int trackingInterval=-1;
		String pluginsRootDirectory = "./plugins";
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
            }else if (args[i].startsWith(REGISTER_SWITCH)){
                i++;
                ComponentDescriptionReader reader = new ComponentDescriptionReader();
                ComponentFactory.registerComponents(reader.getComponentDescriptions(args[i]));          
            }else if (args[i].startsWith(PLUGINS_SWITCH)){
                i++;
                pluginsRootDirectory = args[i];
            }else if (args[i].startsWith("-")) {
				System.err.println("Unknown option: "+args[i]);
				System.exit(-1);
			}
		}
		
        //init all static factory
        Plugins.init(pluginsRootDirectory);
        ComponentFactory.init();
        SequenceFactory.init();
        LookupTableFactory.init();
        ConnectionFactory.init();
        
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
            System.err.println("Error in reading graph from XML !");
            if (verbose) {
                ex.printStackTrace(System.err);
            }
            System.exit(-1);
        }catch(GraphConfigurationException ex){
            System.err.println("Error - graph's configuration invalid !");
            if (verbose) {
                ex.printStackTrace(System.err);
            }
            System.exit(-1);
		} catch (RuntimeException ex) {
			System.err.println("Error during graph initialization !");
			System.err.println(ex.getCause().getMessage());
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
    
    
	private static void printHelp(){
		System.out.println("Usage: runGraph [-(v|log|cfg|P:|tracking|info|register)] <graph definition file>");
		System.out.println("Options:");
		System.out.println("-v\t\t\tbe verbose - print even graph layout");
		System.out.println("-P:<key>=<value>\tadd definition of property to global graph's property list");
		System.out.println("-cfg <filename>\t\tload definitions of properties from specified file");
		System.out.println("-tracking <seconds>\thow frequently output the graph processing status");
		System.out.println("-info\t\t\tprint info about Clover library version");
        System.out.println("-register\t\tload/register additional transformation components");
        System.out.println("-plugins\t\tdirectory where to look for plugins/components");
	}

	private static void printInfo(){
	    System.out.println("CloverETL library version "+JetelVersion.MAJOR_VERSION+"."+JetelVersion.MINOR_VERSION+" build#"+JetelVersion.BUILD_NUMBER+" compiled "+JetelVersion.LIBRARY_BUILD_DATETIME);
	}
	
}

