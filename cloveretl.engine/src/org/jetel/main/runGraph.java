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
import org.jetel.graph.*;

/**
 *  class for executing transformations described in XML layout file<br><br>
 *  The graph layout is read from specified XML file and the whole transformation is executed.<br>
 *  <tt><pre>
 *  Program parameters:
 *  -v		be verbose - print even graph layout
 *  -log <i>logfile</i>	send output messages to specified logfile instead of stdout
 *  <i>filename</i>	name of the file containing graph's layout in XML (this must be the last parameter passed)  
 *  </pre></tt>
 * @author      dpavlis
 * @since	2003/09/09
 * @revision    $Revision$
 */
public class runGraph {

	/**  Description of the Field */
	private final static String VERBOSE_SWITCH = "-v";
	/**  Description of the Field */
	private final static String LOG_SWITCH = "-log";


	/**
	 *  Description of the Method
	 *
	 * @param  args  Description of the Parameter
	 */
	public static void main(String args[]) {
		boolean verbose = false;
		String logFilename = null;
		OutputStream log = null;

		if (args.length < 1) {
			System.err.println("Usage: runGraph [-v(erbose) -log <filename>] <graph definition file>");
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
		}

		FileInputStream in;
		System.out.println("***  CloverETL framework, (c) 2002-03 D.Pavlis, released under GNU Public Licence  ***\n");
		System.out.println("Graph definition file: " + args[args.length - 1]);

		try {
			in = new FileInputStream(args[args.length - 1]);
			if (logFilename != null) {
				log = new FileOutputStream(logFilename);
				System.out.println("\nSending output messages to file: " + logFilename);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}

		TransformationGraphXMLReaderWriter graphReader = TransformationGraphXMLReaderWriter.getReference();
		TransformationGraph graph = TransformationGraph.getReference();

		if (!graphReader.read(graph, in)) {
			System.err.println("Error in reading graph from XML !");
			return;
		}

		if (!graph.init(log)) {
			System.err.println("Graph initialization failed !");
			return;
		}

		if (verbose) {
			//this can be called only after graph.init()
			TransformationGraph.getReference().dumpGraphConfiguration();
		}

		if (!graph.run()) {// start all Nodes (each node is one thread)
			System.out.println("Failed starting graph !");
		} else {
			System.out.println("Execution of graph finished !");
		}

	}

}

