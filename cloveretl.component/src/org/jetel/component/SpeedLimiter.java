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

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.CopyComponentTokenTracker;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;
import org.jetel.util.CTLMapping.MissingRecordFieldMessage;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.formatter.TimeIntervalUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.CloverString;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>Speed Limiter Component</h3>
 *  
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td><td>Speed Limiter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td><td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td><td>All records from input port:0 are copied onto all connected output ports. 
 * Thread waits specified amount of miliseconds("delay" attribute) after processing each record.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td><td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td><td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td><td></td></tr>
 * </table>
 * <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr><td><b>type</b></td><td>"SPEED_LIMITER"</td></tr>
 * <tr><td><b>id</b></td><td>component identification</td></tr>
 * <tr><td><b>delay</b><td>long value - with this amount of milisecond will be delayed each record processed by this component</td></td></tr>
 * </table>
 *
 * @author Martin Varecha <martin.varecha@@javlinconsulting.cz> (c)
 *         JavlinConsulting s.r.o. www.javlinconsulting.cz
 * @created Nov 5, 2007
 */
public class SpeedLimiter extends Node {

	private static final String XML_DELAY_ATTRIBUTE = "delay";
	private static final String XML_INPUT_MAPPING_ATTRIBUTE = "inputMapping";
    private static final String DELAY_RECORD_NAME = "Delay";

	public final static String COMPONENT_TYPE = "SLEEP";
	private final static int READ_FROM_PORT = 0;

	/** Input record identifier for CTL mappings */
	protected static final String INPUT_RECORD_ID = "input";

    private static final int DELAY_INDEX = 0;
    private static final int DELAY_MILLIS_INDEX = 1;

    protected static final String DELAY_NAME = "delay";
    protected static final String DELAY_MILLIS_NAME = "delayMillis";

	private CloverBuffer recordBuffer;
	private long delay = 0L;

    /**
     * Source code of input mapping.
     */
    private String inputMappingCode;

	/**
	 * Runtime for input mapping.
	 */
    private CTLMapping inputMapping;

    private DataRecord inputRecord; 
    private DataRecord mappingRecord; 
    
	/**
	 * @param id
	 * @param delay - with this amount of millisecond will be delayed each record processed by this component 
	 */
	public SpeedLimiter(String id, long delay) {
		super(id);
		this.delay = delay;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		tryToInit(null);
	}
	
	private void tryToInit(ConfigurationStatus status) throws ComponentNotReadyException {
		recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);

		//create input mapping
		inputMapping = new CTLMapping("Input mapping", this);
		inputMapping.setTransformation(inputMappingCode);
		
		if (getInputPort(0) != null) {
			inputRecord = inputMapping.addInputMetadata(INPUT_RECORD_ID, getInputPort(0).getMetadata());
		}
		mappingRecord = inputMapping.addOutputMetadata(DELAY_RECORD_NAME, createDelayMetadata());
		if (delay >= 0) {
			inputMapping.setDefaultOutputValue(DELAY_RECORD_NAME, DELAY_MILLIS_NAME, delay);
		}

		inputMapping.init(XML_INPUT_MAPPING_ATTRIBUTE, 
				MissingRecordFieldMessage.newOutputFieldMessage(DELAY_RECORD_NAME, "No such delay attribute")
		);
	}

	public static DataRecordMetadata createDelayMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(DELAY_RECORD_NAME);
		
		metadata.addField(DELAY_INDEX, new DataFieldMetadata(DELAY_NAME, DataFieldType.STRING, null));
		metadata.addField(DELAY_MILLIS_INDEX, new DataFieldMetadata(DELAY_MILLIS_NAME, DataFieldType.LONG, null));

		return metadata;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		InputPortDirect inPort = (InputPortDirect) getInputPort(READ_FROM_PORT);
		if (inPort != null) {
			while (inPort.readRecordDirect(recordBuffer) && runIt) {
				inputRecord.deserialize(recordBuffer);
				inputMapping.execute();
				delay();
				recordBuffer.rewind();
				writeRecordBroadcastDirect(recordBuffer);
				SynchronizeUtils.cloverYield();
			}
		}
		else {
			inputMapping.execute();
			delay();
		}
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private void delay() throws InterruptedException {
		CloverString delayStr = (CloverString) mappingRecord.getField(DELAY_INDEX).getValue();
		if (delayStr != null) {
			delay(TimeIntervalUtils.parseInterval(delayStr.toString()));
		} else {
			Long delay = (Long) mappingRecord.getField(DELAY_MILLIS_INDEX).getValue();
			if (delay != null) {
				delay(delay);
			} else {
				throw new JetelRuntimeException("Delay is not specified.");
			}
		}
	}

	/**
	 * Delays thread by preset amount of milliseconds. 
	 * @throws InterruptedException 
	 */
	private void delay(long delay) throws InterruptedException {
		Thread.sleep(delay);
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (getOutputPort(0) != null) {
			checkInputPorts(status, 1, 1);
			checkOutputPorts(status, 1, Integer.MAX_VALUE);
			checkMetadata(status, getInPorts(), getOutPorts());
		}
		else if (getInputPort(0) != null) {
			checkInputPorts(status, 1, 1);
		}

		if (delay == -1 && StringUtils.isEmpty(inputMappingCode)) {
			status.addError(this, XML_DELAY_ATTRIBUTE, "Delay is not specified.");
		}
		
		try {
			tryToInit(null);
		} catch (ComponentNotReadyException e) {
			status.addError(this, null, e);
		} finally {
			free();
		}

		return status;
	}
	
	/**
	 * Creates new instance of this Component from XML definition.
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 * @throws AttributeNotFoundException 
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		SpeedLimiter speedLimiter = new SpeedLimiter(xattribs.getString(XML_ID_ATTRIBUTE), xattribs.getTimeInterval(XML_DELAY_ATTRIBUTE, -1));
        speedLimiter.setInputMapping(xattribs.getStringEx(XML_INPUT_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
        return speedLimiter;
	}

	/**
	 * @return the inputMapping
	 */
	public String getInputMapping() {
		return inputMappingCode;
	}

	/**
	 * @param inputMapping the inputMapping to set
	 */
	public void setInputMapping(String inputMapping) {
		this.inputMappingCode = inputMapping;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new CopyComponentTokenTracker(this);
	}
	
}
