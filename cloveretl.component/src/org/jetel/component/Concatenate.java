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

import java.util.Iterator;

import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.BasicComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * <h3>Concatenate Component</h3>
 * 
 * <!-- All records from all input ports are copied onto output port [0] It goes port by port (waiting/blocked) if there
 * is currently no data on port. When reading from one port is done (EOF status), continues with the next -->
 * 
 * <table border="1">
 * <th>Component:</th>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4></td>
 * <td>Concatenate</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i></h4></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i></h4></td>
 * <td>All records from all input ports are copied onto output port [0].<br>
 * It goes port by port (waiting/blocked) if there is currently no data on port.<br>
 * When reading from one port is done (EOF status), continues with the next.</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i></h4></td>
 * <td>At least one input port defined/connected</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i></h4></td>
 * <td>Output port[0] defined/connected.</td>
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
 * <td>"CONCATENATE"</td>
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
public class Concatenate extends Node {

    /** The type of the component. */
	public static final String COMPONENT_TYPE = "CONCATENATE";

	/** The port index for data record output. */
	private static final int OUTPUT_PORT = 0;

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		return new Concatenate(xattribs.getString(XML_ID_ATTRIBUTE));
	}

    /** A byte buffer used for fast copying of data records as their deserialized version is never used. */
    private CloverBuffer recordBuffer;

    public Concatenate(String id) {
		super(id);
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 1, Integer.MAX_VALUE) || !checkOutputPorts(status, 1, 1)) {
			return status;
		}

		checkMetadata(status, getInMetadata(), getOutMetadata(), false);

		return status;
	}

	@Override
	public synchronized void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}

		super.init();

        recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);

        if (recordBuffer == null) {
            throw new ComponentNotReadyException("Error allocating a data record buffer!");
        }
	}

	@Override
	public Result execute() throws Exception {
		Iterator<InputPort> inputPortsIterator = getInPorts().iterator();
		OutputPortDirect outPort = getOutputPortDirect(OUTPUT_PORT);

		while (runIt && inputPortsIterator.hasNext()) {
			// FIXME: Avoid casting here as soon as the getInPortsDirect() method is available.
			InputPortDirect inPort = (InputPortDirect) inputPortsIterator.next();

			while (runIt && inPort.readRecordDirect(recordBuffer)) {
				outPort.writeRecordDirect(recordBuffer);
				SynchronizeUtils.cloverYield();
			}
		}

		setEOF(OUTPUT_PORT);

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public synchronized void free() {
		super.free();

		recordBuffer = null;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new BasicComponentTokenTracker(this);
	}
	
}
