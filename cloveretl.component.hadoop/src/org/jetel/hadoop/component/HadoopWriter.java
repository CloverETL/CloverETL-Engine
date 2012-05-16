package org.jetel.hadoop.component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.database.IConnection;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.connection.HadoopConnection;
import org.jetel.hadoop.connection.IHadoopConnection;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

public class HadoopWriter extends Node {

	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	private static final String XML_KEY_FIELD_NAME_ATTRIBUTE = "keyField";
	private static final String XML_VALUE_FIELD_NAME_ATTRIBUTE = "valueField";
	private static final String XML_CONNECTION_ATTRIBUTE = "connection";
	

	private String fileURL;
	private String connectionID;
	private boolean appendData;
	private String keyField;
	private String valueField;
	private IHadoopConnection connection;
	private IHadoopSequenceFileFormatter formatter;
	private MultiFileWriter writer;
    private int skip;
	private int numRecords;
	private int recordsPerFile;
	private int bytesPerFile;
	
	
	private String partition;
	private String attrPartitionKey;
	private LookupTable lookupTable;
	private String attrPartitionOutFields;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	
	private InputPort inPort;

	public final static String COMPONENT_TYPE = "HADOOP_WRITER";
	private final static int READ_FROM_PORT = 0;
	//private final static int OUTPUT_PORT = 0;

	private static Log logger = LogFactory.getLog(HadoopWriter.class);

	/**
	 * Constructor
	 * 
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param appendData
	 * @param fields
	 */
	public HadoopWriter(String id, String fileURL, String connectionID, String keyField, String valueField, 
			boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.connectionID=connectionID;
		this.appendData = appendData;
		this.keyField=keyField;
		this.valueField=valueField;
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		if (firstRun()) {
	         //TODO:  writer.init(getInputPort(READ_FROM_PORT).getMetadata()); - not used for now, need change once isFile preffered is impemented
			
			IConnection conn = getGraph().getConnection(connectionID);
			if (conn == null) {
				throw new ComponentNotReadyException(this,"Can't find HadoopConnection ID: " + connectionID);
			}
			if (!(conn instanceof HadoopConnection)) {
				throw new ComponentNotReadyException(this,"Connection with ID: " + connectionID + " isn't instance of the HadoopConnection class - "+conn.getClass().toString());
			}
			
			logger.debug(String.format("Connecting to HDFS via [%s].",conn.getId()));
			this.connection= ((HadoopConnection) conn).getConnection();

			inPort = getInputPort(READ_FROM_PORT);
			
			try {
				this.formatter=connection.createFormatter( this.keyField, this.valueField,! this.appendData);
				this.formatter.init(inPort.getMetadata());
				this.formatter.setDataTarget(new URI(this.fileURL));
			} catch (IOException e) {
				throw new ComponentNotReadyException(this,"Can't create Hadoop formatter.",e);
			} catch (URISyntaxException e) {
				throw new ComponentNotReadyException(this,"Invalid fileURL format.",e);
			}
			
		}
		else {
			//TODO: writer.reset(); - see above
		}
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		while (record != null && runIt) {
			record = inPort.readRecord(record);
			if (record != null) {
		        formatter.write(record);
			}
			SynchronizeUtils.cloverYield();
		}
		formatter.finish();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		try {
			if (formatter!=null) formatter.close();
		}
		catch (IOException e) {
			throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(),e);
		}
	}

	@Override
	public synchronized void free() {
		super.free();
			try {
				if (formatter != null) formatter.close();
				if (connection !=null) connection.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'. " + t.getMessage(), t);
			}
		
		/*if (writer != null)
			try {
				writer.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'. " + t.getMessage(), t);
			}
			*/
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if(!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}

		/*  can't easily check writability - would need also Hadoop connection
		   
        try {
        	FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() 
        			: null, fileURL);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }
        */
        return status;
    }
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		initLookupTable();
		
		
		/*  We don't use MultiFileWriter - we need Formatter to create Hadoop structured data output streams

		// based on file mask, create/open output file
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatter, graph != null ? graph.getRuntimeContext().getContextURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel =  new SystemOutByteChannel();
			}
	        writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(writableByteChannel));
		}
        writer.setLogger(logger);
        writer.setBytesPerFile(bytesPerFile);
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(appendData);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
        writer.setDictionary(graph.getDictionary());
        if (attrPartitionKey != null) {
            writer.setLookupTable(lookupTable);
            writer.setPartitionKeyNames(attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            writer.setPartitionFileTag(partitionFileTagType);
        	if (attrPartitionOutFields != null) {
        		writer.setPartitionOutFields(attrPartitionOutFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
        	}
        }
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
        */
		
	}

	/**
	 * Creates and initializes lookup table.
	 * 
	 * @throws ComponentNotReadyException
	 */
	private void initLookupTable() throws ComponentNotReadyException {
		if (partition == null) return;
		
		// Initializing lookup table
		lookupTable = getGraph().getLookupTable(partition);
		if (lookupTable == null) {
			throw new ComponentNotReadyException("Lookup table \"" + partition + "\" not found.");
		}
		if (!lookupTable.isInitialized()) {
			lookupTable.init();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#fromXML(org.jetel.graph.TransformationGraph, org.w3c.dom.Element)
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		HadoopWriter aDataWriter = null;
		
		try{
			aDataWriter = new HadoopWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
									xattribs.getString(XML_CONNECTION_ATTRIBUTE),
									xattribs.getString(XML_KEY_FIELD_NAME_ATTRIBUTE),
									xattribs.getString(XML_VALUE_FIELD_NAME_ATTRIBUTE),
									xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false));
			if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
				aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
				aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
			}
            if(xattribs.exists(XML_RECORDS_PER_FILE)) {
            	aDataWriter.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
            }
            if(xattribs.exists(XML_BYTES_PER_FILE)) {
            	aDataWriter.setBytesPerFile(xattribs.getInteger(XML_BYTES_PER_FILE));
            }
			if(xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
				aDataWriter.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
				aDataWriter.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
				aDataWriter.setPartitionFileTag(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_OUTFIELDS_ATTRIBUTE)) {
				aDataWriter.setPartitionOutFields(xattribs.getString(XML_PARTITION_OUTFIELDS_ATTRIBUTE));
            }
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + xattribs.getString(Node.XML_ID_ATTRIBUTE,"unknown ID") + ":" + ex.getMessage());
			return null;
		}
		
		return aDataWriter;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	@Override
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		xmlElement.setAttribute(XML_CONNECTION_ATTRIBUTE,this.connectionID);
		xmlElement.setAttribute(XML_KEY_FIELD_NAME_ATTRIBUTE,this.keyField);
		xmlElement.setAttribute(XML_VALUE_FIELD_NAME_ATTRIBUTE,this.valueField);
		
		
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
		if (skip != 0){
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skip));
		}
		if (numRecords != 0){
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE,String.valueOf(numRecords));
		}
		if (recordsPerFile > 0) {
			xmlElement.setAttribute(XML_RECORDS_PER_FILE, Integer.toString(recordsPerFile));
		}
		if (bytesPerFile > 0) {
			xmlElement.setAttribute(XML_BYTES_PER_FILE, Integer.toString(bytesPerFile));
		}
		if (partition != null) {
			xmlElement.setAttribute(XML_PARTITION_ATTRIBUTE, partition);
		} else if (lookupTable != null) {
			xmlElement.setAttribute(XML_PARTITION_ATTRIBUTE, lookupTable.getId());
		}
		if (attrPartitionKey != null) {
			xmlElement.setAttribute(XML_PARTITIONKEY_ATTRIBUTE, attrPartitionKey);
		}
		if (attrPartitionOutFields != null) {
			xmlElement.setAttribute(XML_PARTITION_OUTFIELDS_ATTRIBUTE, attrPartitionOutFields);
		}
		xmlElement.setAttribute(XML_PARTITION_FILETAG_ATTRIBUTE, partitionFileTagType.name());
	}
	
    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }

    /**
     * Sets number of written records.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public void setBytesPerFile(int bytesPerFile) {
        this.bytesPerFile = bytesPerFile;
    }

    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }

    /**
     * Sets lookup table for data partition.
     * 
     * @param lookupTable
     */
	public void setLookupTable(LookupTable lookupTable) {
		this.lookupTable = lookupTable;
	}

	/**
	 * Gets lookup table for data partition.
	 * 
	 * @return
	 */
	public LookupTable getLookupTable() {
		return lookupTable;
	}

	/**
	 * Gets partition (lookup table id) for data partition.
	 * 
	 * @param partition
	 */
	public void setPartition(String partition) {
		this.partition = partition;
	}

	/**
	 * Gets partition (lookup table id) for data partition.
	 * 
	 * @return
	 */
	public String getPartition() {
		return partition;
	}

	/**
	 * Sets partition key for data partition.
	 * 
	 * @param partitionKey
	 */
	public void setPartitionKey(String partitionKey) {
		this.attrPartitionKey = partitionKey;
	}

	/**
	 * Gets partition key for data partition.
	 * 
	 * @return
	 */
	public String getPartitionKey() {
		return attrPartitionKey;
	}
	
	/**
	 * Sets fields which are used for file output name.
	 * 
	 * @param partitionOutFields
	 */
	public void setPartitionOutFields(String partitionOutFields) {
		attrPartitionOutFields = partitionOutFields;
	}

	/**
	 * Sets number file tag for data partition.
	 * 
	 * @param partitionKey
	 */
	public void setPartitionFileTag(String partitionFileTagType) {
		this.partitionFileTagType = PartitionFileTagType.valueOfIgnoreCase(partitionFileTagType);
	}

	/**
	 * Gets number file tag for data partition.
	 * 
	 * @return
	 */
	public PartitionFileTagType getPartitionFileTag() {
		return partitionFileTagType;
	}

}
