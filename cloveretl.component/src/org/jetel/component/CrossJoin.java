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

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.FileRecordBuffer;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * CrossJoin component, also known as CartesianProduct
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 12. 2014
 */
public class CrossJoin extends Node implements MetadataProvider {
	public final static String COMPONENT_TYPE = "CROSS_JOIN";
	private final static String OUT_METADATA_NAME = "CrossJoin_dynamic";
	private final static String OUT_METADATA_ID_SUFFIX = "_outMetadata";
	private final static int WRITE_TO_PORT = 0;
	private final static int MASTER_PORT = 0;
	private final static int FIRST_SLAVE_PORT = 1;

	private int slaveCount;
	private boolean[] slaveFinishedReading;
	private ShiftingFileBuffer[] slaveRecordsMemory;
	
	private CloverBuffer data = CloverBuffer.allocateDirect(org.jetel.data.Defaults.Record.RECORD_INITIAL_SIZE);
	private CloverBuffer recordInMemory;
	
	//input
	private InputPort masterPort;
	private InputPort[] slavePorts;
	private DataRecord masterRecord;
	private DataRecord[] slaveRecords;
	
	//output
	private OutputPort outPort;
	private DataRecord outRecord;
	
	//static Log logger = LogFactory.getLog(CrossJoin.class);

	public CrossJoin(String id) {
		super(id);
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		slaveCount = inPorts.size() - 1;

		//init input
		masterPort = getInputPort(MASTER_PORT);
		masterRecord = DataRecordFactory.newRecord(masterPort.getMetadata());
		masterRecord.init();
		slavePorts = new InputPort[slaveCount];
		slaveRecords = new DataRecord[slaveCount];
		slaveFinishedReading = new boolean[slaveCount];
		slaveRecordsMemory = new ShiftingFileBuffer[slaveCount];
		for (int slaveIdx = 0; slaveIdx < slaveCount; slaveIdx++) {
			slavePorts[slaveIdx] = getInputPort(FIRST_SLAVE_PORT + slaveIdx);
			slaveRecords[slaveIdx] = DataRecordFactory.newRecord(slavePorts[slaveIdx].getMetadata());
			slaveRecords[slaveIdx].init();
			slaveFinishedReading[slaveIdx] = false;
			slaveRecordsMemory[slaveIdx] = new ShiftingFileBuffer();
		}
		
		// init output
		outPort = getOutputPort(WRITE_TO_PORT);
		outRecord = DataRecordFactory.newRecord(outPort.getMetadata());
		outRecord.init();
		outRecord.reset();
	}
	
	/**
	 * Concatenates passed DataRecord array and writes it to the output port.
	 * @param currentRecords
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeRecord(DataRecord[] currentRecords) throws IOException, InterruptedException {
		int outFieldIndex = 0;
		DataField[] outFields = outRecord.getFields();
		for (DataRecord rec : currentRecords) {
			for (DataField field : rec.getFields()) {
				outFields[outFieldIndex].setValue(field);
				outFieldIndex++;
			}
		}
		outPort.writeRecord(outRecord);
		outRecord.reset();
	}
	
	/**
	 * Reads record from specified slave port and inserts it at the current position of the iterator.
	 * Subsequent call to iterator.next() will return the newly read record. 
	 * @param slaveIdx
	 * @param iter
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private DataRecord readSlaveRecord(int slaveIdx, CloverBuffer intoBuffer) throws IOException, InterruptedException {
		if (slavePorts[slaveIdx].readRecord(slaveRecords[slaveIdx]) == null) {
			// no more input data
			slaveFinishedReading[slaveIdx] = true;
			return null;
		}
		slaveRecords[slaveIdx].serialize(intoBuffer);
		return slaveRecords[slaveIdx];
	}
	
	/**
	 * Recursive method performing the logic of Cartesian product.
	 * @param currentRecords
	 * @param slaveIdx
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void recursiveAppendSlaveRecord(DataRecord[] currentRecords, int slaveIdx) throws IOException, InterruptedException {
		if (slaveIdx >= slaveCount) {
			writeRecord(currentRecords);
			return;
		}
		slaveRecordsMemory[slaveIdx].rewind();
		
		data.clear();
		while (runIt && ((recordInMemory = slaveRecordsMemory[slaveIdx].shift(data)) != null || !slaveFinishedReading[slaveIdx])) {
			if (recordInMemory == null) {
				// no record in memory any more, we need to read more
				DataRecord slaveRecord = readSlaveRecord(slaveIdx, data);
				if (slaveRecord == null) {
					// all records read from this slave
					break;
				} else {
					data.flip();
					slaveRecordsMemory[slaveIdx].push(data);

					
					currentRecords[slaveIdx + 1] = slaveRecord;
				}
			} else {
				// record found in memory
				recordInMemory.flip();
				currentRecords[slaveIdx + 1].deserialize(recordInMemory);
			}
			
			recursiveAppendSlaveRecord(currentRecords, slaveIdx + 1);
			data.clear();
		}
	}
	
	@Override
	protected Result execute() throws IOException, InterruptedException {
		DataRecord[] currentRecords = new DataRecord[slaveCount + 1]; //master and slaves
		for (int slaveIdx = 0; slaveIdx < slaveCount; slaveIdx++) {
			currentRecords[slaveIdx + 1] = slaveRecords[slaveIdx].duplicate();
		}
		while (runIt && masterPort.readRecord(masterRecord) != null) {
			currentRecords[0] = masterRecord.duplicate();
			recursiveAppendSlaveRecord(currentRecords, 0);
			//SynchronizeUtils.cloverYield();
		}
		setEOF(WRITE_TO_PORT);
		ensureAllRecordsRead();
		return (runIt ? Result.FINISHED_OK : Result.ABORTED);
	}
	
	/**
	 * When no records are received from some input port, some records may be hanging on other
	 * input ports because they were not needed to produce correct result. We need to read these
	 * hanging records because clover doesn't like unread input records. This method just reads
	 * all input records and throws them away.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void ensureAllRecordsRead() throws IOException, InterruptedException {
		for (int slaveIdx = 0; slaveIdx < slaveCount; slaveIdx++) {
			if (!slaveFinishedReading[slaveIdx]) {
				while (runIt && slavePorts[slaveIdx].readRecord(slaveRecords[slaveIdx]) != null) {
					// just blank read here
				}
			}
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		return new CrossJoin(xattribs.getString(XML_ID_ATTRIBUTE));
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if (!checkInputPorts(status, 1, Integer.MAX_VALUE) || !checkOutputPorts(status, 1, 1)) {
			return status;
		}
		
		DataRecordMetadata expectedOutMetadata = getConcatenatedMetadata();
		DataRecordMetadata outMetadata = getOutputPort(WRITE_TO_PORT).getMetadata();
		
		DataFieldMetadata[] expectedFields = expectedOutMetadata.getFields();
		DataFieldMetadata[] outFields = outMetadata.getFields();
		
		if (outFields.length < expectedFields.length) {
			status.add("Incompatible metadata on output. Not enough fields in output metadata.",
					Severity.ERROR, this, Priority.NORMAL);
			return status;
		}
		
		for (int i = 0; i < expectedFields.length; i++) {
			DataFieldType expectedType = expectedFields[i].getDataType();
			DataFieldType outType = outFields[i].getDataType();
			if (!expectedType.isSubtype(outType)) {
				status.add("Incompatible metadata on output. Expected type " + expectedType + " on position " + (i + 1) +
						". Found: " + outType + ".", Severity.ERROR, this, Priority.NORMAL);
			}
		}
		
		return status;
	}

	/**
	 * Produces metadata for output based on metadata on inputs. The goal is to produce exactly the metadata user
	 * wants in most cases.
	 * 
	 * Output metadata are made by copying metadata on first input port and copying all fields from other input ports. 
	 * @return
	 */
	private DataRecordMetadata getConcatenatedMetadata() {
		InputPort[] ports = inPorts.values().toArray(new InputPort[0]);
		DataRecordMetadata outMeta = ports[0].getMetadata().duplicate();

		for (int i = 1; i < ports.length; i++) {
			for (DataFieldMetadata inFieldMeta : ports[i].getMetadata().getFields()) {
				outMeta.addField(inFieldMeta.duplicate());
			}
		}
		outMeta.setLabel(null); // because of normalization which is done later (it copies label into name, we don't want that)
		outMeta.setName(OUT_METADATA_NAME);
		TypedProperties props = outMeta.getRecordProperties();
		props.clear(); // clear GUI properties (preview attachment etc.)
		DataRecordMetadata.normalizeMetadata(outMeta);
		return outMeta;
	}

	@Override
	public MVMetadata getInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		return null;
	}

	@Override
	public MVMetadata getOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex == WRITE_TO_PORT) {
			for (InputPort port : inPorts.values()) {
				if (port.getMetadata() == null) {
					return null;
				}
			}
			return metadataPropagationResolver.createMVMetadata(getConcatenatedMetadata(), this, OUT_METADATA_ID_SUFFIX);
		}
		return null;
	}
	
	/**
	 * This implementation provides no records immediately after writing and allows reading only from the beginning using rewind().
	 */
	private static class ShiftingFileBuffer extends FileRecordBuffer {
		@Override
		public void push(CloverBuffer data) throws IOException {
			super.push(data);
			readPosition = writePosition;
		}
		
	}
}
