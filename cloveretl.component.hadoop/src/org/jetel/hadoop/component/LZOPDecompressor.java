package org.jetel.hadoop.component;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.anarres.lzo.LzopInputStream;
import org.apache.log4j.Logger;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ReadableChannelPortIterator;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.w3c.dom.Element;

/**
 * Simple LZOP stream decompressing component.
 * 
 * @author tkramolis
 */
public class LZOPDecompressor extends Node {

	private static final Logger logger = Logger.getLogger(LZOPDecompressor.class);
	
	private static final String COMPONENT_TYPE = "LZOP_DECOMPRESSOR";
	private static final int INPUT_PORT = 0;
	private static final int OUTPUT_PORT = 0;
	
	
	private ByteDataField inputField;
	private ByteDataField outputField;
	private DataRecord inRecord;
	private DataRecord outRecord;
	
	private ReadableChannelPortIterator portReadingIterator;
	
	public LZOPDecompressor(String id) {
		super(id);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		DataRecordMetadata inMetadata = getInputPort(INPUT_PORT).getMetadata();
		DataRecordMetadata outMetadata = getOutputPort(OUTPUT_PORT).getMetadata();
		
		inRecord = DataRecordFactory.newRecord(inMetadata);
		inRecord.init();
		
		outRecord = DataRecordFactory.newRecord(outMetadata);
		outRecord.init();
		
		inputField = (ByteDataField) inRecord.getField(getByteField(inMetadata).getNumber()); // FIXME NPE CCE(?)
		outputField = (ByteDataField) outRecord.getField(getByteField(outMetadata).getNumber()); // FIXME NPE

		logger.debug("Reading from field \"" + inputField.getMetadata().getName() + "\", writing to field \"" + outputField.getMetadata().getName() + "\"");
		
		portReadingIterator = new ReadableChannelPortIterator(getInputPort(INPUT_PORT), new String[] { "port:$" + INPUT_PORT + "." + inputField.getMetadata().getName() + ":stream" });
		portReadingIterator.setContextURL(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null);
		portReadingIterator.setPropertyRefResolver(new PropertyRefResolver(getGraph().getGraphProperties()));
		portReadingIterator.init();
	}

	private static DataFieldMetadata getByteField(DataRecordMetadata metadata) {
		DataFieldMetadata byteField = null;
		if (metadata != null) {
			for (DataFieldMetadata field : metadata.getFields()) {
				if (DataFieldType.BYTE.isSubtype(field.getDataType())) {
					byteField = field;
				}
			}
		}
		return byteField;
	}
	
	@Override
	protected Result execute() throws Exception {
		try {
			ReadableByteChannel channel;
			byte[] buffer = new byte[8*1024];
			
			while (runIt && ((channel = portReadingIterator.getNextData()) != null)) {
				InputStream is = new LzopInputStream(Channels.newInputStream(channel));
				
				int read;
				while (runIt && (read = is.read(buffer)) != -1) {
					byte[] value = new byte[read];
					System.arraycopy(buffer, 0, value, 0, read);
					
					outputField.setValue(value);
					getOutputPort(OUTPUT_PORT).writeRecord(outRecord);
				}
				
				is.close();
			}
		} finally {
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		checkInputPorts(status, 1, 1);
		checkOutputPorts(status, 1, 1);
		
		DataFieldMetadata byteField = getByteField(getInputPort(INPUT_PORT).getMetadata());
		if (byteField == null) {
			status.add("No field of type byte/cbyte in input edge metadata", Severity.ERROR, this, Priority.NORMAL);
		}
		byteField = getByteField(getOutputPort(OUTPUT_PORT).getMetadata());
		if (byteField == null) {
			status.add("No field of type byte/cbyte in output edge metadata", Severity.ERROR, this, Priority.NORMAL);
		}
		
		return status;
	}

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		LZOPDecompressor decompressor = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			decompressor = new LZOPDecompressor(xattribs.getString(Node.XML_ID_ATTRIBUTE));
		} catch (AttributeNotFoundException ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + ex.getMessage(), ex);
		}

		return decompressor;
	}
	
}
