/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-03  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.main;

import java.io.*;
import java.util.Properties;
import org.jetel.graph.*;

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
 *  <tr><td nowrap><b>filename</b></td><td>name of the file containing graph's layout in XML (this must be the last parameter passed)</td></tr>
 *  </table>
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 * @revision    $Revision$
 */
public class runGraph {

	private final static String VERBOSE_SWITCH = "-v";
	private final static String LOG_SWITCH = "-log";
	private final static String PROPERTY_FILE_SWITCH = "-cfg";
	private final static String PROPERTY_DEFINITION_SWITCH = "-P:";
	
	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
		boolean verbose = false;
		String logFilename = null;
		String propertyFilename;
		OutputStream log = null;
		Properties properties=new Properties();

		System.out.println("***  CloverETL framework/transformation graph runner, (c) 2002-04 D.Pavlis, released under GNU Public Licence  ***\n");
		if (args.length < 1) {
			printHelp();
			System.exit(-1);
		}
		// process command line arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith(VERBOSE_SWITCH)) {
				verbose = true;
			}
			if (args[i].startsWith(LOG_SWITCH)) {
				i++;
				logFilename = args[i];
			}
			if (args[i].startsWith(PROPERTY_FILE_SWITCH)){
				i++;
				try {
					InputStream inStream = new BufferedInputStream(new FileInputStream(args[i]));
					properties.load(inStream);
				} catch (IOException ex) {
					System.err.println(ex.getMessage());
					System.exit(-1);
				}
			}
			if (args[i].startsWith(PROPERTY_DEFINITION_SWITCH)){
				String[] nameValue=args[i].replaceFirst(PROPERTY_DEFINITION_SWITCH,"").split("=");
				properties.setProperty(nameValue[0],nameValue[1]);
			}
		}

		FileInputStream in=null;
		System.out.println("Graph definition file: " + args[args.length - 1]);

		try {
			in = new FileInputStream(args[args.length - 1]);
			if (logFilename != null) {
				log = new FileOutputStream(logFilename);
				System.out.println("\nSending output messages to file: " + logFilename);
			}
		} catch (FileNotFoundException e) {
			System.err.println("File not found: "+e.getMessage());
			System.exit(-1);
		}

		TransformationGraphXMLReaderWriter graphReader = TransformationGraphXMLReaderWriter.getReference();
		TransformationGraph graph = TransformationGraph.getReference();
		graph.loadGraphProperties(properties);

		try {
			if (!graphReader.read(graph, in)) {
				System.err.println("Error in reading graph from XML !");
				System.exit(-1);
			}

			if (!graph.init(log)) {
				System.err.println("Graph initialization failed !");
				System.exit(-1);
			}

			if (verbose) {
				//this can be called only after graph.init()
				TransformationGraph.getReference().dumpGraphConfiguration();
			}
		} catch (RuntimeException ex) {
			System.err.println("!!! Fatal error during graph initialization  !!!");
			System.err.println(ex.getCause().getMessage());
			if (verbose) {
				ex.printStackTrace();
			}
			System.exit(-1);
		}

		//	start all Nodes (each node is one thread)
		boolean finishedOK = false;
		try {
			finishedOK = graph.run();
		} catch (RuntimeException ex) {
			System.err.println("!!! Fatal error during graph run !!!");
			System.err.println(ex.getCause().getMessage());
			if (verbose) {
				ex.printStackTrace();
			}
			System.exit(-1);
		}
		if (finishedOK) {
			// everything O.K.
			System.out.println("Execution of graph finished !");
			System.exit(0);
		} else {
			// something FAILED !!
			System.err.println("Failed starting graph !");
			System.exit(-1);
		}

	}
	
	private static void printHelp(){
		System.out.println("Usage: runGraph [-(v|log|cfg|P:)] <graph definition file>");
		System.out.println("Options:");
		System.out.println("-v\t\t\tbe verbose - print even graph layout");
		System.out.println("-log <logfile>\t\tsend output messages to specified logfile instead of stdout");
		System.out.println("-P:<key>=<value>\tadd definition of property to global graph's property list");
		System.out.println("-cfg <filename>\t\tload definitions of properties from specified file");
	}

}

