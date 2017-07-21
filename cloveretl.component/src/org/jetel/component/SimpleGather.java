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
package org.jetel.component;

import java.io.IOException;

import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.BasicComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * <h3>Simple Gather Component</h3>
 * 
 * <!-- All records from all input ports are gathered and copied onto output port [0] -->
 * 
 * <table border="1">
 * <th>Component:</th>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4></td>
 * <td>Simple Gather</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i></h4></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i></h4></td>
 * <td>All records from all input ports are gathered and copied onto output port [0].<br>
 * It goes port by port (waiting/blocked) if there is currently no data on port.<br>
 * Implements inverse RoundRobin.<br>
 * </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i></h4></td>
 * <td>At least one connected output port.</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i></h4></td>
 * <td>[0]- output records (gathered)</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Comment:</i></h4></td>
 * <td></td>
 * </tr>
 * </table>
 * <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr>
 * <td><b>type</b></td>
 * <td>"SIMPLE_GATHER"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * </tr>
 * </table>
 * 
 * @author dpavlis
 * @since April 4, 2002
 */
public class SimpleGather extends Node {

	/** Description of the Field */
	public final static String COMPONENT_TYPE = "SIMPLE_GATHER";

	/**
	 * how many empty loops till thread wait() is called
	 */
	private final static int NUM_EMPTY_LOOPS_TRESHOLD = 29;

	/**
	 * how many millis to wait if we reached the specified number of empty loops (when no data has been read).
	 */
	private final static int EMPTY_LOOPS_WAIT = 10;

	public SimpleGather(String id, TransformationGraph graph) {
		super(id, graph);
	}

	@Override
	public Result execute() throws Exception {
		InputPortDirect inPort;
		/*
		 * we need to keep track of all input ports - it they contain data or signalized that they are empty.
		 */
		int numActive;
		int emptyLoopCounter = 0;
		/*
		 * we need to keep track of all input ports - it they contain data or signalized that they are empty.
		 */
		int readFromPort;
		boolean[] isEOF = new boolean[getInPorts().size()];
		for (int i = 0; i < isEOF.length; i++) {
			isEOF[i] = false;
		}
		InputPortDirect inputPorts[] = (InputPortDirect[]) getInPorts().toArray(new InputPortDirect[0]);
		numActive = inputPorts.length;// counter of still active ports - those without EOF status
		// the metadata is taken from output port definition
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		readFromPort = 0;
		inPort = inputPorts[readFromPort];
		int lastReadPort = -1;
		boolean forceReading = false;
		while (runIt && numActive > 0) {
			if (!isEOF[readFromPort] && (inPort.hasData() || forceReading || numActive == 1)) {
				forceReading = false;
				emptyLoopCounter = 0;
				if (inPort.readRecordDirect(recordBuffer)) {
					writeRecordToOutputPorts(recordBuffer);
					lastReadPort = readFromPort;
				} else {
					isEOF[readFromPort] = true;
					numActive--;
				}
				SynchronizeUtils.cloverYield();
			} else {
				readFromPort = (++readFromPort) % (inputPorts.length);
				inPort = inputPorts[readFromPort];

				if (lastReadPort == readFromPort) {
					forceReading = true;
				}
				// have we reached the maximum empty loops count ?
				if (emptyLoopCounter > NUM_EMPTY_LOOPS_TRESHOLD) {
					Thread.sleep(getSleepTime());
				} else {
					emptyLoopCounter++;
				}
			}
		}

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	protected void writeRecordToOutputPorts(CloverBuffer recordBuffer) throws IOException, InterruptedException {
		writeRecordBroadcastDirect(recordBuffer);
	}
	
	protected long getSleepTime() {
		return EMPTY_LOOPS_WAIT;
	}
	
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		return new SimpleGather(xattribs.getString(XML_ID_ATTRIBUTE), graph);
	}

	/** Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 1, Integer.MAX_VALUE) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		checkMetadata(status, getInMetadata(), getOutMetadata(), false);

		return status;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new BasicComponentTokenTracker(this);
	}
}
