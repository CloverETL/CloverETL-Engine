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
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.FileRecordBuffer;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.TransformException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
	
	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	
	private final static int WRITE_TO_PORT = 0;
	private final static int MASTER_PORT = 0;
	private final static int FIRST_SLAVE_PORT = 1;
	
	/** Amount of memory for records from each slave port. When memory is full, the records are swapped to disk. */
	private final static int SLAVE_BUFFER_SIZE = Defaults.Record.RECORD_INITIAL_SIZE * 8; // 512 KB
	
	// attributes
	private String transformClassName;
	private String transformSource;
	private String transformURL;
	private String charset;
	
	private RecordTransform transformation;
	private Properties transformationParameters;

	// slaves management
	private int slaveCount;
	private boolean[] slaveFinishedReading;
	private ShiftingFileBuffer[] slaveRecordsMemory;
	
	/** Record buffer for slave records */
	private CloverBuffer data = CloverBuffer.allocateDirect(org.jetel.data.Defaults.Record.RECORD_INITIAL_SIZE);
	
	/** Helper variable, needed for maintaining reference to "data" buffer */
	private CloverBuffer recordInMemory;
	
	// input
	private InputPort masterPort;
	private InputPort[] slavePorts;
	private DataRecord masterRecord;
	private DataRecord[] slaveRecords;
	
	// output
	private OutputPort outPort;
	private DataRecord[] outRecord; // size 1
	
	static Log logger = LogFactory.getLog(CrossJoin.class);

	public CrossJoin(String id, String transform, String transformUrl, String transformClass, String charset) {
		super(id);
		this.transformSource = transform;
		this.transformURL = transformUrl;
		this.transformClassName = transformClass;
		this.charset = charset;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[] { getOutputPort(WRITE_TO_PORT).getMetadata() };
		DataRecordMetadata[] inMetadata = getInMetadataArray();
		
		createTransformIfPossible(inMetadata, outMetadata);

		// init transformation
        if (transformation != null && !transformation.init(transformationParameters, inMetadata, outMetadata)) {
            throw new ComponentNotReadyException("Error when initializing tranformation function.");
        }
	}
	
	private void createTransformIfPossible(DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata) {
		if (transformSource != null || transformURL != null || transformClassName != null) {
			transformation = getTransformFactory(inMetadata, outMetadata).createTransform();
        }
	}
	
	private TransformFactory<RecordTransform> getTransformFactory(DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata) {
    	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	transformFactory.setTransform(transformSource);
    	transformFactory.setTransformClass(transformClassName);
    	transformFactory.setTransformUrl(transformURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(inMetadata);
    	transformFactory.setOutMetadata(outMetadata);
    	return transformFactory;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		if (transformation != null) {
			transformation.preExecute();
		}
		
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
			slaveRecordsMemory[slaveIdx] = new ShiftingFileBuffer(SLAVE_BUFFER_SIZE);
		}
		
		// init output
		outPort = getOutputPort(WRITE_TO_PORT);
		outRecord =  new DataRecord[] { DataRecordFactory.newRecord(outPort.getMetadata()) };
		outRecord[WRITE_TO_PORT].init();
		outRecord[WRITE_TO_PORT].reset();
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		if (transformation != null) {
			transformation.postExecute();
		}
	}
	
	@Override
	public void free() {
		super.free();
		try {
			for (int i = 0; i < slaveRecordsMemory.length; i++) {
				slaveRecordsMemory[i].close();
			}
		} catch (IOException e) {
			logger.debug("Exception while clearing slave records memory of " + this.getName() + ". Message: " + e.getMessage());
		}
	}

	/**
	 * Concatenates passed DataRecord array and writes it to the output port.
	 * @param currentRecords
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TransformException 
	 */
	private void writeRecord(DataRecord[] currentRecords) throws IOException, InterruptedException, TransformException {
		if (transformation != null) {
			int transformResult;
			try {
				transformResult = transformation.transform(currentRecords, outRecord);
			} catch (Exception exception) {
				transformResult = transformation.transformOnError(exception, currentRecords, outRecord);
			}
			
		} else {
			int outFieldIndex = 0;
			DataField[] outFields = outRecord[WRITE_TO_PORT].getFields();
			for (DataRecord rec : currentRecords) {
				for (DataField field : rec.getFields()) {
					outFields[outFieldIndex].setValue(field);
					outFieldIndex++;
				}
			}
			
		}
		
		outPort.writeRecord(outRecord[WRITE_TO_PORT]);
		outRecord[WRITE_TO_PORT].reset();
	}
	
	/**
	 * Reads record from specified slave port and inserts it at the current position of the iterator.
	 * Subsequent call to iterator.next() will return the newly read record. 
	 * @param slaveIdx
	 * @param iter
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private DataRecord readSlaveRecord(int slaveIdx) throws IOException, InterruptedException {
		if (slavePorts[slaveIdx].readRecord(slaveRecords[slaveIdx]) == null) {
			// no more input data
			slaveFinishedReading[slaveIdx] = true;
			return null;
		}
		
		return slaveRecords[slaveIdx];
	}
	
	/**
	 * Recursive method performing the logic of Cartesian product.
	 * @param currentRecords
	 * @param slaveIdx
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TransformException 
	 */
	private void recursiveAppendSlaveRecord(DataRecord[] currentRecords, int slaveIdx) throws IOException, InterruptedException, TransformException {
		if (slaveIdx >= slaveCount) {
			writeRecord(currentRecords);
			return;
		}
		slaveRecordsMemory[slaveIdx].rewind();
		
		data.clear();
		while (runIt && ((recordInMemory = slaveRecordsMemory[slaveIdx].shift(data)) != null || !slaveFinishedReading[slaveIdx])) {
			if (recordInMemory == null) {
				// no record in memory any more, we need to read more
				DataRecord slaveRecord = readSlaveRecord(slaveIdx);
				if (slaveRecord == null) {
					// all records read from this slave
					break;
				} else {
					slaveRecord.serialize(data);
					data.flip();
					slaveRecordsMemory[slaveIdx].push(data);
					currentRecords[slaveIdx + 1] = slaveRecord;
				}
			} else {
				// record found in memory
				// At this point, data and recordInMemory are actually the same buffer instance.
				data.flip();
				currentRecords[slaveIdx + 1].deserialize(data);
			}
			
			recursiveAppendSlaveRecord(currentRecords, slaveIdx + 1);
			data.clear();
		}
	}
	
	@Override
	protected Result execute() throws IOException, InterruptedException, TransformException {
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
		
		CrossJoin join = new CrossJoin(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF),
				xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE, null, RefResFlag.URL),
				xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null),
				xattribs.getString(XML_CHARSET_ATTRIBUTE, null)
				);
		
		join.setTransformationParameters(xattribs.attributes2Properties(
				new String[] {XML_ID_ATTRIBUTE, XML_TRANSFORM_ATTRIBUTE, XML_TRANSFORMCLASS_ATTRIBUTE}));	
		
		return join;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if (!checkInputPorts(status, 1, Integer.MAX_VALUE) || !checkOutputPorts(status, 1, 1)) {
			return status;
		}
		
		DataRecordMetadata[] outMeta = new DataRecordMetadata[] { getOutputPort(WRITE_TO_PORT).getMetadata() };
		DataRecordMetadata[] inMeta = getInMetadataArray();
		createTransformIfPossible(inMeta, outMeta);
		
		if (transformation == null) {
			DataRecordMetadata expectedOutMetadata = getConcatenatedMetadata(null);
			DataRecordMetadata outMetadata = getOutputPort(WRITE_TO_PORT).getMetadata();
			DataFieldMetadata[] expectedFields = expectedOutMetadata.getFields();
			DataFieldMetadata[] outFields = outMetadata.getFields();

			if (outFields.length < expectedFields.length) {
				status.add("Incompatible metadata on output. Not enough fields in output metadata.", Severity.ERROR, this, Priority.NORMAL);
				return status;
			}

			for (int i = 0; i < expectedFields.length; i++) {
				DataFieldType expectedType = expectedFields[i].getDataType();
				DataFieldType outType = outFields[i].getDataType();
				DataFieldContainerType expectedContainer = expectedFields[i].getContainerType();
				DataFieldContainerType outContainer = outFields[i].getContainerType();
				if (expectedType != outType || expectedContainer != outContainer) {
					StringBuilder sb = new StringBuilder("Incompatible metadata on output. Expected type ");
					if (expectedContainer == null || expectedContainer == DataFieldContainerType.SINGLE) {
						sb.append(StringUtils.quote(expectedType.toString()));
					} else if (expectedContainer == DataFieldContainerType.LIST) {
						sb.append(StringUtils.quote("list[" + expectedType + "]"));
					} else if (expectedContainer == DataFieldContainerType.MAP) {
						sb.append(StringUtils.quote("map[" + expectedType + "]"));
					}
					sb.append(" on position " + (i + 1) + ". Found: ");
					if (outContainer == null || outContainer == DataFieldContainerType.SINGLE) {
						sb.append(StringUtils.quote(outType.toString()));
					} else if (outContainer == DataFieldContainerType.LIST) {
						sb.append(StringUtils.quote("list[" + outType + "]"));
					} else if (outContainer == DataFieldContainerType.MAP) {
						sb.append(StringUtils.quote("map[" + outType + "]"));
					}
					sb.append(".");
					status.add(sb.toString(), Severity.ERROR, this, Priority.NORMAL);
				}
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
	private DataRecordMetadata getConcatenatedMetadata(DataRecordMetadata[] inputMetadata) {
		if (inputMetadata == null) {
			inputMetadata = new DataRecordMetadata[inPorts.size()];
			for (int i = 0; i < inputMetadata.length; i++) {
				inputMetadata[i] = getInputPort(i).getMetadata();
			}
		}
		
		DataRecordMetadata outMeta = inputMetadata[0].duplicate();
		for (int i = 1; i < inputMetadata.length; i++) {
			for (DataFieldMetadata inFieldMeta : inputMetadata[i].getFields()) {
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
			DataRecordMetadata[] inputMetadata = new DataRecordMetadata[inPorts.size()];
			int index = 0;
			for (InputPort port : inPorts.values()) {
				MVMetadata metadata = metadataPropagationResolver.findMetadata(port.getEdge());
				if (metadata == null) {
					return null;
				}
				inputMetadata[index] = metadata.getModel();
				index++;
			}
			return metadataPropagationResolver.createMVMetadata(getConcatenatedMetadata(inputMetadata), this, OUT_METADATA_ID_SUFFIX);
		}
		return null;
	}
	
	public void setTransformationParameters(Properties transformationParameters) {
		this.transformationParameters = transformationParameters;
	}
	
	/**
	 * This implementation provides no records immediately after writing and allows reading only from the beginning using rewind().
	 */
	private static class ShiftingFileBuffer extends FileRecordBuffer {

		public ShiftingFileBuffer(int bufferSize) {
			super(bufferSize);
		}

		@Override
		public void push(CloverBuffer data) throws IOException {
			super.push(data);
			readPosition = writePosition;
		}
		
	}
}
