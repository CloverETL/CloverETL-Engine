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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.formatter.AbstractFormatter;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This writer component reads data records from any number of input ports 
 * and according to ports mapping definition of relations among records, creates structured XML file.
 *
 * example of mapping definition:
          <Mapping element="customer" inPort="2" key="CUSTOMERID" fieldsAs="elements" fieldsAsExcept="CUSTOMERID;CompanyName" >
              <Mapping element="order" inPort="0" key="OrderID" relationKeysToParent="CustomerID" fieldsAs="elements" fieldsAsExcept="OrderID">
				<Mapping element="address" inPort="3" key="AddressID" relationKeysToParent="OrderID" fieldsAs="attributes" >
				</Mapping>
				<Mapping element="employee" inPort="1" key="EmployeeID" relationKeysFromParent="EmployeeID" fieldsAs="elements" >
				</Mapping>
              </Mapping>
          </Mapping>
 * 
 * 
 * see example graphXmlWriter
 * 
 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
 * (c) JavlinConsulting s.r.o.
 * www.javlinconsulting.cz
 * @created Dec 20, 2007
 */
public class XmlWriter extends Node {
	static Log logger = LogFactory.getLog(XmlWriter.class);
	private final static int OUTPUT_PORT = 0;
	
	private final static Pattern NAMESPACE = Pattern.compile("(.+)[=]([\"]|['])(.+)([\"]|['])$");

	/*
	 * node element attributes
	 * */
	/** component attribute: component type */
	public final static String COMPONENT_TYPE = "XML_WRITER";
	/** component attribute: URL for mapping file */
	public static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	/** component attribute: mapping */
	public static final String XML_MAPPING_ATTRIBUTE = "mapping";
	/** component attribute: output file(s) */
	public static final String XML_FILE_URL_ATTRIBUTE = "fileUrl";
	/** component attribute: charset */
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	/** component attribute: records per file */
	public static final String XML_RECORDS_PER_FILE_ATTRIBUTE = "recordsPerFile";
	/** component attribute: records skipped */
	public static final String XML_RECORDS_SKIP_ATTRIBUTE = "recordSkip";
	/** component attribute: total records count */
	public static final String XML_RECORDS_COUNT_ATTRIBUTE = "recordCount";
	/** component attribute: output XML root element */
	public static final String XML_ROOT_ELEMENT_ATTRIBUTE = "rootElement";
	/** component attribute: use root element switch */
	public static final String XML_USE_ROOT_ELEMENT_ATTRIBUTE = "useRootElement";
	/** component attribute: use root info attributes switch */
	public static final String XML_ROOT_INFO_ATTRIBUTES = "rootInfoAttributes";
	/** component attribute: single row stitch */
	public static final String XML_SINGLE_ROW_ATTRIBUTE = "singleRow"; // alias for XML_OMIT_NEW_LINES_ATTRIBUTE
	/** component attribute: root default namespace */
	public static final String XML_ROOT_DEFAULT_NAMESPACE_ATTRIBUTE = "rootDefaultNamespace";
	/** component attribute: root namespaces */
	public static final String XML_ROOT_NAMESPACES_ATTRIBUTE = "rootNamespaces";
	/** component attribute: DTD publicId */
	public static final String XML_DTD_PUBLIC_ID_ATTRIBUTE = "dtdPublicId";
	/** component attribute: DTD systemId */
	public static final String XML_DTD_SYSTEM_ID_ATTRIBUTE = "dtdSystemId";
	/** component attribute: XSD schema locator */
	public static final String XML_XSD_LOCATION_ATTRIBUTE = "xsdSchemaLocation";
	/** component attribute: make dirs */
	public static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
	/** component attribute: compress level */
	public static final String XML_COMPRESSLEVEL_ATTRIBUTE = "compressLevel";
	/** component attribute: omit new lines 
	 * @deprecated use {@link #XML_SINGLE_ROW_ATTRIBUTE} instead */
	public static final String XML_OMIT_NEW_LINES_ATTRIBUTE = "omitNewLines";

	/*
	 * attributed of mapping element
	 * */
	public static final String XML_MAPPING_ELEMENT = "Mapping";
	/** mapping attribute: index of input port */
	public static final String XML_INDEX_ATTRIBUTE = "inPort";
	/** mapping attribute: key columns on this input port */
	public static final String XML_KEYS_ATTRIBUTE = "key";
	/** mapping attribute: key columns on the parent port (if any) */
	public static final String XML_PARENT_KEYS_ATTRIBUTE = "parentKey";
	/** mapping attribute 
	 * @deprecated use {@link #XML_PARENT_KEYS_ATTRIBUTE} and {@link #XML_KEYS_ATTRIBUTE} instead */
	public static final String XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE = "keyToParent";
	/** mapping attribute 
	 * @deprecated use {@link #XML_PARENT_KEYS_ATTRIBUTE} and {@link #XML_KEYS_ATTRIBUTE} instead */
	public static final String XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE = "keyFromParent";
	/** mapping attribute: element name in output XML */
	public static final String XML_ELEMENT_ATTRIBUTE = "element";
	/** mapping attribute: values are created as attributes or element in output XML */
	public static final String XML_FIELDS_AS_ATTRIBUTE = "fieldsAs";
	/** mapping attribute: columns created in different way as specified by "fieldsAs" attribute */
	public static final String XML_FIELDS_AS_EXCEPT_ATTRIBUTE = "fieldsAsExcept";
	/** mapping attribute: columns excluded from output XML */
	public static final String XML_FIELDS_IGNORE_ATTRIBUTE = "fieldsIgnore";
	/** mapping attribute: namespaces */
	public static final String XML_NAMESPACES_ATTRIBUTE = "namespaces";
	/** mapping attribute: default namespace */
	public static final String XML_DEFAULT_NAMESPACE_ATTRIBUTE = "defaultNamespace";
	/** mapping attribute: namespace prefix */
	public static final String XML_NAMESPACE_PREFIX_ATTRIBUTE = "fieldsNamespacePrefix";
	
	/*
	 * defaults for output XML
	 * */
	/** default root element for output XML */
	public static final String DEFAULT_ROOT_ELEMENT = "root";
	/** default record element for output XML */
	public static final String DEFAULT_RECORD_ELEMENT = "record";
	
	/*
	 * output XML root element attributes
	 * */
	/** output XML root element attribute */
	public static final String ATTRIBUTE_COMPONENT_ID = "component";
	/** output XML root element attribute */
	public static final String ATTRIBUTE_GRAPH_NAME = "graph";
	/** output XML root element attribute */
	public static final String ATTRIBUTE_CREATED = "created";
	
	
	public static final String XSI_URI = "http://www.w3.org/2001/XMLSchema-instance";
	
	/**
	 * Map of portIndex => PortDefinition
	 * It's read from XML during initialization.
	 */
	private Map<Integer, PortDefinition> allPortDefinitionMap;
	/**
	 * Root port definitions.
	 */
	private PortDefinition rootPortDefinition;
	protected int portsCnt;
	/**
	 * URL of out XMl file.
	 * */
	private String fileUrl;
	/**
	 * Name of root element in out XML file. It's read from XML definition.
	 * */
	private String rootElement;

	private MultiFileWriter writer;
	
	/**
	 * Charset of output XML.
	 */
	private String charset = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;

	private int recordsSkip = 0;
	private int recordsCount = 0;
	private int recordsPerFile = 0;
	private boolean omitNewLines = false;
	/** If set to false, XML without root element is produced, which is invalid XML. */
	private boolean useRootElement = true;
	/** If set to true (default) root element will contain couple of info attributes (nodeId, graphId, created). */
	private boolean rootInfoAttributes = true;
	private Map<String, String> namespaces;
	private String dtdPublicId;
	private String dtdSystemId;
	private String rootDefaultNamespace;
	private String xsdSchemaLocation;
	private int compressLevel;
	private boolean mkDir;
	private String namespacesString;
	private NodeList mappingNodes;
	private String mappingString;
	private String mappingURL;

	/**
	 * XmlFormatter which methods are called from MultiFileWriter. 
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Jan 4, 2008
	 */
	protected class XmlFormatter extends AbstractFormatter {
		//File outFile = null;
		OutputStream os = null; 
		TransformerHandler th = null;
			
		@Override
		public void close() {
			if (os == null) {
				return;
			}

			try{
				flush();
				os.close();
			}catch(IOException ex){
				ex.printStackTrace();
			}
			os = null;
		}

		@Override
		public void flush() throws IOException {
			if (os != null) os.flush();
		}
		
		@Override
		public void finish() throws IOException {
			if (th!= null)
				writeFooter();
		}

		@Override
		public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		}

		@Override
		public void reset() {
		}
		/*
		 * (non-Javadoc)
		 * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
		 */
		@Override
		public void setDataTarget(Object outputDataTarget) {
			close();
			WritableByteChannel channel = null;
			if (outputDataTarget instanceof WritableByteChannel){
				channel = (WritableByteChannel)outputDataTarget;
				os = Channels.newOutputStream(channel);
			} else 
				throw new IllegalArgumentException("parameter "+outputDataTarget+" is not instance of WritableByteChannel");
			//outFile = (File)outputDataTarget;
		}

		/*
		 * (non-Javadoc)
		 * @see org.jetel.data.formatter.Formatter#writeHeader()
		 */
		@Override
		public int writeHeader() throws IOException {
			try {
				th = createHeader(os);
			} catch (Exception e) {
				logger.error("error header", e);
			}
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
		 */
		@Override
		public int write(DataRecord record) throws IOException {
			if (th == null)
				writeHeader();
			
			List<DataRecord> records = new ArrayList<DataRecord>(); 
			records.add(record);
			try {
				addRecords(th, records, rootPortDefinition);
			} catch (SAXException e) {
				logger.error("error write", e);
			}
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.jetel.data.formatter.Formatter#writeFooter()
		 */
		@Override
		public int writeFooter() throws IOException {
			try {
				if (th == null)
					writeHeader();
				createFooter(os, th);
			} catch (Exception e) {
				logger.error("error footer", e);
			}
			th = null;
			os = null;
			return 0;
		}

	}// inner class XmlFormatter
	
	/**
	 * Description of input port mapping and record's relations with records from another ports.
	 * Also wrapper for read data records.
	 *  
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Dec 20, 2007
	 */
	protected static class PortDefinition {
		PortDefinition(){}
		int portIndex = 0;
		/** Comma separated list of columns names, which identify records from this port. Can be null. */
		String keysAttr = null;
		/** List of columns names, which identify records from this port. Can be null. */
		String[] keys = null;

		String keysDeprecatedAttr = null;
		/** Comma separated list of columns from this record which identify parent record. */
		String keysToParentDeprecatedAttr = null;
		/** Comma separated list of columns from parent record which identify this record. */
		String keysFromParentDeprecatedAttr = null;

		/** Comma separated list of columns names, which identify records from parent port. Can be null. */
		String parentKeysAttr = null;
		/** List of columns names, which identify records from parent port. Can be null. */
		String[] parentKeys = null;
		
		List<String> relationKeysStrings = new ArrayList<String>();
		List<String[]> relationKeysArrays = new ArrayList<String[]>();
		
		/** List of children definitions. */
		List<PortDefinition> children;
		/** Parent port definition. It's null for root definition. */
		PortDefinition parent;

		// Map of keyName => recordsMap
		// records may be stored by more different keys
		Map<String,Map<HashKey, TreeRecord>> dataMapsByRelationKeys = new HashMap<String,Map<HashKey, TreeRecord>>();
		DataRecordMetadata metadata;
		List<DataRecord> dataRecords = new ArrayList<DataRecord>();
		
		/** Flag which indicates, that fields will be written as attributes in output XML. */
		boolean fieldsAsAttributes;
		/** Comma separated list of fields which are exception for flag fieldsAsAttributes.
		 *  (if flag is true, fields from this list will be written as elements) */
		public String[] fieldsAsExcept;
		/** Set of fields which will be kicked out of XML output. */
		public Set<String> fieldsIgnore;
		/** Name of element of record in out XMl. May be null, default is "record". */
		String element = null;
		/** lazy initialized list to simplify processing of XML output. */
		public Integer[] fieldsAsAttributesIndexes;
		/** lazy initialized list to simplify processing of XML output. */
		public Integer[] fieldsAsElementsIndexes;
		/** Pairs of prefix-uri for namespaces */
		public Map<String, String> namespaces;
		/** Optional prefix for attributes or elements. */
		public String fieldsNamespacePrefix;
		/** DefaultNamespace */
		public String defaultNamespace;
		
		@Override
		public String toString(){
			return "PortDefinition#"+portIndex + " key:"+keysAttr+" parentKey:"+parentKeysAttr+" relationKeysStrings:"+relationKeysStrings ;
		}
		/** Resets this instance for next execution without graph init. */
		public void reset() {
			dataMapsByRelationKeys = new HashMap<String,Map<HashKey, TreeRecord>>();
		}
		
		private void addDataRecord(String relationKeysString, String[] relationKeysArray, DataRecord record) {
			RecordKey recKey = new RecordKey( relationKeysArray, metadata);
			recKey.init();
			HashKey key = new HashKey(recKey, record);
			TreeRecord tr = getTreeRecord(relationKeysString, key);
			if (tr == null)
				tr = new TreeRecord();
			tr.records.add(record);
			Map<HashKey, TreeRecord> map = dataMapsByRelationKeys.get(relationKeysString);
			if (map == null){
				map = new HashMap<HashKey, TreeRecord>();
				dataMapsByRelationKeys.put(relationKeysString, map);
			}
			map.put(key, tr);
		}

		private TreeRecord getTreeRecord(String relationKeysString, HashKey key) {
			TreeRecord tr = null;
			Map<HashKey, TreeRecord> map = dataMapsByRelationKeys.get(relationKeysString);
			if (map!=null)
				tr = map.get(key);
			return tr;
		}
		
	}
	
	/**
	 * Possible values of "fieldsAs" attribute of Mapping element.
	 * @see PortDefinition.fieldsAsAttributes
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Dec 20, 2007
	 */
	public enum FieldsAs {
		attributes,
		elements
	}
	
	/**
	 * This thread reads records from one input port and stores them to appropriate data structure.
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Dec 11, 2007
	 */
	private class InputReader extends Thread {
		private InputPort inPort;
		/** Port definition of input port which this thread reads from. */
		private PortDefinition portDefinition;
		DataRecordMetadata metadata;

		public InputReader(int index, PortDefinition portDefinition) {
			super(Thread.currentThread().getName() + ".InputThread#" + index);
			this.portDefinition = portDefinition;
			runIt = true;
			inPort = getInputPort(index);
			metadata = inPort.getMetadata();
			portDefinition.metadata = metadata; 
		}
		
		@Override
		public void run() {
			while (runIt) {
				try {
					DataRecord record = DataRecordFactory.newRecord(metadata);
					record.init();
					if (inPort.readRecord(record) == null) // no more input data
						return;

					//portDefinition.dataMap.put(key, item);
					
					for (int i=0; i<portDefinition.relationKeysStrings.size(); i++){
						String relationKeysString = portDefinition.relationKeysStrings.get(i);
						String[] relationKeysArray = portDefinition.relationKeysArrays.get(i);
						portDefinition.addDataRecord(relationKeysString, relationKeysArray, record);
					}// for relationKeys
					if (portDefinition.parent == null){ // root mapping has records list in addition
						portDefinition.dataRecords.add(record);
					}
				} catch (InterruptedException e) {
					logger.debug(getId() + ": thread forcibly aborted", e);
					return;
				} catch (Exception e) {
					logger.error(getId() + ": thread failed", e);
					return;
				}
			} // while
		}
	}// inner class InputReader

	
	/**
	 * Simple wrapper for single record (unique key) or set of records (not unique key).
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Dec 11, 2007
	 */
	private static class TreeRecord {
		
		/** this attribute is set when TreeRecord instance is stored in collection with unique key */
		//public DataRecord record;
		/** this attribute is set when TreeRecord instance is stored in collection with not unique key */
		public List<DataRecord> records = new ArrayList<DataRecord>();
	}// nested class TreeRecord 


	/**
	 * Constructor. Other necessary attributes are set with injection. 
	 * @param id
	 * @param fileUrl
	 * @param rootElement
	 * @param allPortDefinitionMap
	 * @param recordsPerFile 
	 * @param recordsCount 
	 * @param namespaces 
	 * @param dtdSystemId 
	 * @param dtdPublicId 
	 * @param xsdSchemaLocation 
	 * @param rootPortDefinitionList
	 * @deprecated Use simple constructor and setters instead of this.
	 */
	protected XmlWriter(String id, String fileUrl, String rootElement, 
			Map<Integer, PortDefinition> allPortDefinitionMap, PortDefinition rootPortDefinition, 
			int recordsSkip, int recordsCount, int recordsPerFile, boolean omitNewLines, 
			boolean useRootElement, String rootDefaultNamespace, Map<String, String> namespaces, String dtdPublicId, String dtdSystemId, String xsdSchemaLocation,
			boolean rootInfoAttributes) {
		super(id);
		this.fileUrl = fileUrl;
		this.rootElement = rootElement;
		this.allPortDefinitionMap = allPortDefinitionMap;
		this.rootPortDefinition = rootPortDefinition;
		this.recordsSkip = recordsSkip;
		this.recordsCount = recordsCount;
		this.recordsPerFile = recordsPerFile;
		this.omitNewLines = omitNewLines;
		this.useRootElement = useRootElement;
		this.rootInfoAttributes = rootInfoAttributes;
		this.namespaces = namespaces;
		this.dtdPublicId = dtdPublicId;
		this.dtdSystemId = dtdSystemId;
		this.rootDefaultNamespace = rootDefaultNamespace;
		this.xsdSchemaLocation = xsdSchemaLocation;
	}

	public XmlWriter(String id) {
		super(id);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		TransformationGraph graph = getGraph();

		portsCnt = inPorts.size();

        // test that we have at least one input port and one output
        if (portsCnt < 1) {
            throw new ComponentNotReadyException(getId() + ": At least one output port has to be defined!");
        }

        if (namespaces == null)
        	namespaces = XmlWriter.getNamespaces(this.namespacesString);

        if (rootPortDefinition == null){
    		PortDefinition rootPortDefinition = null;
    		Map<Integer, PortDefinition> allPortDefinitionMap = new HashMap<Integer,PortDefinition>();
    		try {
    			if (this.mappingURL != null) {
    				ReadableByteChannel ch = FileUtils.getReadableChannel(graph != null ? graph.getRuntimeContext().getContextURL() : null, mappingURL);
    				Document doc = createDocumentFromChannel(ch);
    	            Element mappingRoot = doc.getDocumentElement();
    				PortDefinition portDef = createInputPortDefinitionStructure(graph, allPortDefinitionMap, mappingRoot);
    				rootPortDefinition = portDef;
    			} else if (this.mappingString != null) {
    	            Document doc = createDocumentFromString(mappingString);
    	            Element mappingRoot = doc.getDocumentElement();
    				PortDefinition portDef = createInputPortDefinitionStructure(graph, allPortDefinitionMap, mappingRoot);
    				rootPortDefinition = portDef;
    	        } else {
    	            //old-fashioned version of mapping definition
    	            //mapping xml elements are child nodes of the component
    	        	List<PortDefinition> list = readInPortsDefinitionFromXml(graph, this.mappingNodes, (PortDefinition)null, allPortDefinitionMap);
    	        	if (list.size() > 1)
    	 	           throw new ComponentNotReadyException("More then 1 root mapping element" );
    	        	else if (list.size() < 1)
    	 	           throw new ComponentNotReadyException("No mapping element" );

    	        	rootPortDefinition = list.get(0);
    	        }
    		} catch (Exception e) {
    			throw new ComponentNotReadyException("cannot instantiate node from XML", e);
    		}
    		this.allPortDefinitionMap = allPortDefinitionMap;
    		this.rootPortDefinition = rootPortDefinition;
        }
		
        XmlFormatter formatter = new XmlFormatter(); 
        writer = new MultiFileWriter(formatter, graph != null ? graph.getRuntimeContext().getContextURL() : null, this.fileUrl);
        writer.setLogger(logger);
        writer.setRecordsPerFile(this.recordsPerFile);
        writer.setAppendData(false);
        writer.setSkip(this.recordsSkip);
        writer.setNumRecords(this.recordsCount);
		writer.setUseChannel(true);
        writer.setLookupTable(null);
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
        writer.setCharset(charset);
        //writer.setPartitionKeyNames(partitionKey);
        //writer.setPartitionFileTag(partitionFileTagType);
        writer.setDictionary(graph.getDictionary());
        writer.setMkDir(mkDir);
        writer.setCompressLevel(compressLevel);
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		if (firstRun()) {
		    writer.init(this.rootPortDefinition.metadata);
		}
		else {
			writer.reset();
			for (PortDefinition def : allPortDefinitionMap.values()){
				def.reset();
			}// for
		}
	}

	
	@Override
	public Result execute() throws Exception {
		InputReader[] portReaders = new InputReader[portsCnt];
		
		// read slave ports in separate threads
		for (int idx = 0; idx < portsCnt; idx++) {
			if (allPortDefinitionMap.get(idx) == null)
				throw new IllegalStateException("Input port "+idx+" is connected, but isn't defined in mapping attribute.");
			portReaders[idx] = new InputReader(idx, allPortDefinitionMap.get(idx));
			portReaders[idx].start();
		}
		// wait for slave input threads to finish their job
		boolean killIt = false;
		for (int idx = 0; idx < portsCnt; idx++) {
			while (portReaders[idx].getState() != Thread.State.TERMINATED) {
				if (killIt) {
					portReaders[idx].interrupt();
					break;
				}
				killIt = !runIt;
				try {
					portReaders[idx].join(1000);
				} catch (InterruptedException e) {
					logger.debug(getId() + " thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}// while
		}// for
		try {
			// and now ... data structure is read ... writing can be processed 
			for (DataRecord record : rootPortDefinition.dataRecords){
				// multiWriter will call formatter.write
				writer.write(record);
			}// for 
			
			//flushXmlSax();
		} catch (Exception e) {
			logger.error("Error during creating XML file", e);
			throw e;
		} finally {
			writer.finish();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}


	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		try {
			writer.close();
		}
		catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	
	/**
	 * Creates output XML from all read records using SAX.
	 * Call this after all records are stored in PortDefinition structures.  
	 * @throws TransformerConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	/*
	private void flushXmlSax() throws TransformerConfigurationException, SAXException, IOException {

		FileOutputStream fos = new FileOutputStream(fileUrl);
		TransformerHandler hd = createHeader(fos);
		PortDefinition portDefinition = rootPortDefinition;
		// for each record of port
		for (Map.Entry<HashKey, TreeRecord> e : portDefinition.dataMap.entrySet()){
			TreeRecord record = e.getValue();
			List<DataRecord> records = new ArrayList<DataRecord>();
			records.add(record.record);
			addRecords(hd, records, portDefinition);
		}// for record

		createFooter(fos, hd);
	}*/

	private TransformerHandler createHeader(OutputStream os) throws FileNotFoundException, TransformerConfigurationException, SAXException {
		StreamResult streamResult = new StreamResult(os);
		SAXTransformerFactory tf = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
		// SAX2.0 ContentHandler.
		TransformerHandler hd = tf.newTransformerHandler();
		Transformer serializer = hd.getTransformer();
        
		serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
		serializer.setOutputProperty(OutputKeys.ENCODING, this.charset);
		//serializer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,"users.dtd");
		if (omitNewLines)
			serializer.setOutputProperty(OutputKeys.INDENT,"no");
		else
			serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		
		hd.setResult(streamResult);
		hd.startDocument();

		String root = (rootElement!=null && rootElement.length()>0) ? rootElement : DEFAULT_ROOT_ELEMENT; 

		if (useRootElement && dtdPublicId != null && dtdPublicId.trim().length()>0 && dtdSystemId != null && dtdSystemId.trim().length()>0){
			hd.startDTD(root, dtdPublicId, dtdSystemId);
			hd.endDTD();
		}
		
		//if (recordsPerFile!=1){
		if (this.useRootElement) {
			AttributesImpl atts = new AttributesImpl();
			if (rootInfoAttributes) {
				atts.addAttribute("", ATTRIBUTE_COMPONENT_ID, ATTRIBUTE_COMPONENT_ID, "CDATA", getId());
				atts.addAttribute("", ATTRIBUTE_GRAPH_NAME, ATTRIBUTE_GRAPH_NAME, "CDATA", this.getGraph().getName());
				atts.addAttribute("", ATTRIBUTE_CREATED, ATTRIBUTE_CREATED, "CDATA", (new Date()).toString());
			}
			if (!StringUtils.isEmpty(xsdSchemaLocation)) {
				atts.addAttribute(XSI_URI, "schemaLocation", "xsi:schemaLocation", "CDATA", this.xsdSchemaLocation);
			}

			for (String prefix : namespaces.keySet()) {
				String uri = namespaces.get(prefix);
				hd.startPrefixMapping(prefix, uri);
			}
			if (!rootDefaultNamespace.isEmpty()) {
				hd.startPrefixMapping("", rootDefaultNamespace);
			}

			hd.startElement(rootDefaultNamespace, getLocalName(root), root, atts);
		}
		return hd;
	}
	
	private String getLocalName(String name) {
		String[] parts = name.split(":", 2);
		if (parts.length < 2) {
			return name;
		} else {
			return parts[1];
		}
	}

	private void createFooter(OutputStream os, TransformerHandler hd) throws TransformerConfigurationException, SAXException, IOException {
		try {
			//if (recordsPerFile!=1){
			if (this.useRootElement){
				 String root = (rootElement!=null && rootElement.length()>0) ? rootElement : DEFAULT_ROOT_ELEMENT; 
				 hd.endElement(rootDefaultNamespace, getLocalName(root), root);
				 for (String prefix : namespaces.keySet())
					 hd.endPrefixMapping(prefix);
			}
			hd.endDocument();
		} finally {
			 os.close();
		}
	}

	private static Map<String, String> getNamespaces(String namespacePaths) throws ComponentNotReadyException {
		Map<String, String> namespaces = new HashMap<String, String>();
		if (namespacePaths == null) return namespaces;
		String ns;
		String path;
		for (String namespacePath: namespacePaths.split(";")) {
			Matcher matcher = NAMESPACE.matcher(namespacePath);
			if (!matcher.find()) 
				throw new ComponentNotReadyException("The namespace expression '"+ namespacePath +"' is not valid.");
			if ((ns = matcher.group(1)) != null && (path = matcher.group(3)) != null) {
				namespaces.put(ns, path);
			}
		}
		return namespaces;
	}

	/**
	 * Writes list of records into XML output.. 
	 * @param hd - XML output handler
	 * @param dataRecords - list of records to write
	 * @param portDefinition - how to write records
	 * @throws SAXException
	 */
	private void addRecords(TransformerHandler hd, List<DataRecord> dataRecords, PortDefinition portDefinition) throws SAXException {
		 AttributesImpl atts = new AttributesImpl();
		 for (DataRecord dataRecord : dataRecords){
			 int fieldsCnt = dataRecord.getNumFields();
			 atts.clear();
			 //atts.addAttribute( "", "", ATTRIBUTE_METADATA_NAME,"CDATA", dataRecord.getMetadata().getName());
			 //hd.startElement("","",ELEMENT_RECORD,atts);
			 String outElementName = portDefinition.element == null ? DEFAULT_RECORD_ELEMENT : portDefinition.element;

			 // lazy init of attribute / element flags
			 if (portDefinition.fieldsAsAttributesIndexes == null) {
				 List<Integer> fieldsAsAttributesIndexes = new ArrayList<Integer>();
				 List<Integer> fieldsAsElementsIndexes = new ArrayList<Integer>();
				 for (int i=0; i<fieldsCnt; i++){
						 String fieldName = dataRecord.getMetadata().getField(i).getName();

						 // ignore field?
						 if (portDefinition.fieldsIgnore != null
								 && portDefinition.fieldsIgnore.contains(fieldName) )
							 continue;

						 if (portDefinition.fieldsAsExcept != null 
								 && portDefinition.fieldsAsExcept.length>0
								 && Arrays.binarySearch(portDefinition.fieldsAsExcept, (Object)fieldName, null)>-1){
							 // found in exception list
							 if (portDefinition.fieldsAsAttributes)
								 fieldsAsElementsIndexes.add(i);
							 else
								 fieldsAsAttributesIndexes.add(i);
						 } else {
							 // NOT found in exception list
							 if (portDefinition.fieldsAsAttributes)
								 fieldsAsAttributesIndexes.add(i);
							 else
								 fieldsAsElementsIndexes.add(i);
						 }
				 }// for
				 portDefinition.fieldsAsAttributesIndexes = fieldsAsAttributesIndexes.toArray( new Integer[fieldsAsAttributesIndexes.size()]);
				 portDefinition.fieldsAsElementsIndexes = fieldsAsElementsIndexes.toArray( new Integer[fieldsAsElementsIndexes.size()]);
			 }//if
			 
			 // fields as attributes
			 for (int x=0; x<portDefinition.fieldsAsAttributesIndexes.length; x++){
				 int i = portDefinition.fieldsAsAttributesIndexes[x];
				 DataField field = dataRecord.getField(i);
				 String value = field.toString();
				 String name = dataRecord.getMetadata().getField(i).getName();
				 if (portDefinition.fieldsNamespacePrefix != null)
					 name = portDefinition.fieldsNamespacePrefix + ":" + name;
				 atts.addAttribute("", name, name, "CDATA", value);
			 } // for
			 
			 for (String prefix : portDefinition.namespaces.keySet()){
				String uri = portDefinition.namespaces.get(prefix);
				hd.startPrefixMapping(prefix, uri);
			 }
			 
			 hd.startElement(portDefinition.defaultNamespace, getLocalName(outElementName), outElementName, atts);

			 // fields as elements
			 for (int x=0; x<portDefinition.fieldsAsElementsIndexes.length; x++){
				 int i = portDefinition.fieldsAsElementsIndexes[x];
				 DataField field = dataRecord.getField(i);
				 atts.clear();
				 String name = dataRecord.getMetadata().getField(i).getName();
				 String qname;
				 if (portDefinition.fieldsNamespacePrefix != null) {
					 qname = portDefinition.fieldsNamespacePrefix + ":" + name;
				 } else {
					 qname = name;
				 }
				 hd.startElement("", name, qname, atts);
				 String value = field.toString();
				 hd.characters(value.toCharArray(),0,value.length());
				 hd.endElement("", name, qname);
				 /*
				 atts.addAttribute( "", "", ATTRIBUTE_FIELD_NAME,"CDATA", dataRecord.getMetadata().getField(i).getName());
				 atts.addAttribute( "", "", ATTRIBUTE_FIELD_TYPE,"CDATA", Character.toString(field.getType()));
				 atts.addAttribute( "", "", ATTRIBUTE_FIELD_VALUE,"CDATA", field.toString());
				 hd.startElement("","",ELEMENT_FIELD,atts);
				 hd.endElement("","",ELEMENT_FIELD);
				 */
			 }// for fields

			 // recursivelly render related children ports and add it to document
			 for (PortDefinition child : portDefinition.children){
				 // obtain list of records related with this record

				 // reference from child to parent
				 if (child.parentKeysAttr != null){
					 RecordKey recKey = new RecordKey( child.parentKeys, portDefinition.metadata);
					 HashKey key = new HashKey(recKey, dataRecord);
					 TreeRecord tr = child.getTreeRecord( child.keysAttr, key);
					 if (tr != null && tr.records != null)
						 addRecords(hd, tr.records, child);
				 }//if

			 }// for children

			 //hd.endElement("","",ELEMENT_RECORD);
			 hd.endElement(portDefinition.defaultNamespace, getLocalName(outElementName), outElementName);
			 
			 for (String prefix : portDefinition.namespaces.keySet())
				 hd.endPrefixMapping(prefix);
			 
		 } // for
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
	
		xmlElement.setAttribute(XML_ID_ATTRIBUTE, getId());
	
		if (charset != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}
	}
	
	/** 
	 * Creates an instance according to XML specification.
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 * @throws AttributeNotFoundException 
	 * @throws ComponentNotReadyException 
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		XmlWriter writer = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		
		writer = new XmlWriter(xattribs.getString(XML_ID_ATTRIBUTE));
		
        // set mapping
        String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null,RefResFlag.SPEC_CHARACTERS_OFF);
        String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
        NodeList nodes = xmlElement.getChildNodes();
        if (mappingURL != null) 
        	writer.setMappingURL(mappingURL);
        else if (mapping != null) 
        	writer.setMapping(mapping);
        else if (nodes != null && nodes.getLength() > 0){
            //old-fashioned version of mapping definition
            //mapping xml elements are child nodes of the component
        	writer.setMappingNodes(nodes);
        } else {
        	xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF); // throw configuration exception
        }

		boolean omitNewLines = xattribs.getBoolean(XML_SINGLE_ROW_ATTRIBUTE, false); // singleRow is deprecated attribute, but still possible ... 
		omitNewLines = xattribs.getBoolean(XML_OMIT_NEW_LINES_ATTRIBUTE, omitNewLines); // ... thus omitNewLines takes precedence over singleRow
		writer.setOmitNewLines(omitNewLines);
		
		boolean useRootElement = xattribs.getBoolean(XML_USE_ROOT_ELEMENT_ATTRIBUTE, true);
		writer.setUseRootElement(useRootElement);
		
		boolean rootInfoAttributes = xattribs.getBoolean(XML_ROOT_INFO_ATTRIBUTES, true);
		writer.setRootInfoAttributes(rootInfoAttributes);
		
		String dtdPublicId = xattribs.getString(XML_DTD_PUBLIC_ID_ATTRIBUTE, null);
		writer.setDtdPublicId(dtdPublicId);
		
		String dtdSystemId = xattribs.getString(XML_DTD_SYSTEM_ID_ATTRIBUTE, null);
		writer.setDtdSystemId(dtdSystemId);
		
		String fileUrl = xattribs.getString(XML_FILE_URL_ATTRIBUTE);
		writer.setFileUrl(fileUrl);
		
		String rootDefaultNamespace = xattribs.getString(XML_ROOT_DEFAULT_NAMESPACE_ATTRIBUTE, "");
		writer.setRootDefaultNamespace(rootDefaultNamespace);
		
		String xsdSchemaLocation = xattribs.getString(XML_XSD_LOCATION_ATTRIBUTE, null);
		writer.setXsdSchemaLocation(xsdSchemaLocation);
		
		String rootNamespaces = xattribs.getString(XML_ROOT_NAMESPACES_ATTRIBUTE, null);
		writer.setRootNamespaces(rootNamespaces);
		
		int recordsSkip = xattribs.getInteger(XML_RECORDS_SKIP_ATTRIBUTE, 0);
		writer.setRecordsSkip(recordsSkip);
		
		int recordsCount = xattribs.getInteger(XML_RECORDS_COUNT_ATTRIBUTE, 0);
		writer.setRecordsCount(recordsCount);
		
		int recordsPerFile = xattribs.getInteger(XML_RECORDS_PER_FILE_ATTRIBUTE, 0);
		writer.setRecordsPerFile(recordsPerFile);
		
		String rootElement = xattribs.getString(XML_ROOT_ELEMENT_ATTRIBUTE, DEFAULT_ROOT_ELEMENT);
		writer.setRootElement(rootElement);
		
		if (xattribs.exists(XML_CHARSET_ATTRIBUTE))
			writer.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		writer.setCompressLevel(xattribs.getInteger(XML_COMPRESSLEVEL_ATTRIBUTE,-1));
		if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			writer.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
        }
		
		return writer;
	}

	/**
	 * Reads PortDefinitions during initialization this component from XML. 
	 * @param graph
	 * @param xmlElement
	 * @param parentPort
	 * @param allPortDefinitionMap
	 * @return
	 * @throws AttributeNotFoundException
	 * @throws ComponentNotReadyException 
	 */
	private static List<PortDefinition> readInPortsDefinitionFromXml(TransformationGraph graph, NodeList mappingElements, PortDefinition parentPort, Map<Integer, PortDefinition> allPortDefinitionMap ) 
			throws AttributeNotFoundException, ComponentNotReadyException {
		List<PortDefinition> portDataList = new ArrayList<PortDefinition>();
		for (int i=0; i<mappingElements.getLength(); i++){
			org.w3c.dom.Node portDescNode = mappingElements.item(i);
			if ( !XML_MAPPING_ELEMENT.equals( portDescNode.getNodeName() ) )
				continue;
			org.w3c.dom.Element portDesc = (org.w3c.dom.Element)mappingElements.item(i);
			PortDefinition portData = readInPortDef(graph, parentPort, allPortDefinitionMap, portDesc);
			portDataList.add(portData);
		}// for
		return portDataList;
	}

	/**
	 * Creates whole input port definition structure from specified DOM element.
	 *   
	 * @param graph
	 * @param allPortDefinitionMap - instance of Map, which will be filled with all created PortDefinition instances
	 * @param portDesc - DOM element "Mapping", containing XML mapping structure
	 * @return
	 * @throws AttributeNotFoundException
	 * @throws ComponentNotReadyException
	 */
	private static PortDefinition createInputPortDefinitionStructure(TransformationGraph graph, Map<Integer,PortDefinition> allPortDefinitionMap, org.w3c.dom.Element portDesc) 
			throws AttributeNotFoundException, ComponentNotReadyException {
		return readInPortDef(graph, null, allPortDefinitionMap, portDesc);
	}
	
	/**
	 * Reads one single PortDefinitions during initialization this component from XML. 
	 * @param graph
	 * @param parentPort
	 * @param allPortDefinitionMap
	 * @param mappingElement
	 * @return
	 * @throws AttributeNotFoundException
	 * @throws ComponentNotReadyException 
	 */
	private static PortDefinition readInPortDef(TransformationGraph graph, PortDefinition parentPort, 
			Map<Integer,PortDefinition> allPortDefinitionMap, org.w3c.dom.Element mappingElement
			) throws AttributeNotFoundException, ComponentNotReadyException {
		PortDefinition portData = new PortDefinition();
		ComponentXMLAttributes portAttribs = new ComponentXMLAttributes(mappingElement, graph);
		portData.parent = parentPort;
		portData.portIndex = portAttribs.getInteger(XML_INDEX_ATTRIBUTE);
		portData.element = portAttribs.getString(XML_ELEMENT_ATTRIBUTE);
		portData.keysAttr = portAttribs.getString(XML_KEYS_ATTRIBUTE, null);
		portData.keysDeprecatedAttr = portAttribs.getString(XML_KEYS_ATTRIBUTE, null);
		portData.parentKeysAttr = portAttribs.getString(XML_PARENT_KEYS_ATTRIBUTE, null);

		portData.keysToParentDeprecatedAttr = portAttribs.getString(XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE, null);
		portData.keysFromParentDeprecatedAttr = portAttribs.getString(XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE, null);
		if (portData.keysToParentDeprecatedAttr != null){
			portData.keysAttr = portData.keysToParentDeprecatedAttr;
			portData.parentKeysAttr = portData.parent.keysDeprecatedAttr;
		}
		if (portData.keysFromParentDeprecatedAttr != null){
			portData.keysAttr = portData.keysDeprecatedAttr;
			portData.parentKeysAttr = portData.keysFromParentDeprecatedAttr;
		}

		portData.namespaces = XmlWriter.getNamespaces(portAttribs.getString(XML_NAMESPACES_ATTRIBUTE, null));
		portData.defaultNamespace = portAttribs.getString(XML_DEFAULT_NAMESPACE_ATTRIBUTE, "");
		portData.fieldsNamespacePrefix = portAttribs.getString(XML_NAMESPACE_PREFIX_ATTRIBUTE, null);
		String s = portAttribs.getString(XML_FIELDS_AS_ATTRIBUTE, null);
		portData.fieldsAsAttributes = false;
		if (s != null){
			try {
				FieldsAs val = FieldsAs.valueOf(s);
				portData.fieldsAsAttributes = (FieldsAs.attributes == val);
			} catch (IllegalArgumentException e){ 
				throw new RuntimeException("Cannot recognize "+XML_FIELDS_AS_ATTRIBUTE+" attribute value:\""+s+"\" for XmlWriter component.");
			}
		}
		
		if (portAttribs.getString(XML_FIELDS_IGNORE_ATTRIBUTE, null) != null){
			String[] ss = portAttribs.getString(XML_FIELDS_IGNORE_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			portData.fieldsIgnore = new HashSet<String>();
			Collections.addAll(portData.fieldsIgnore, ss);
		}
		if (portAttribs.getString(XML_FIELDS_AS_EXCEPT_ATTRIBUTE, null) != null)
			portData.fieldsAsExcept = portAttribs.getString(XML_FIELDS_AS_EXCEPT_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		if (portData.keysAttr != null)
			portData.keys = portData.keysAttr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		if (portData.parentKeysAttr != null)
			portData.parentKeys = portData.parentKeysAttr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

		if (portData.keysAttr != null){
			if (!portData.relationKeysStrings.contains(portData.keysAttr)){
				portData.relationKeysStrings.add(portData.keysAttr);
				portData.relationKeysArrays.add(portData.keys);
			}
		}
		if (portData.parentKeysAttr != null){
			if (!parentPort.relationKeysStrings.contains(portData.parentKeysAttr)){
				parentPort.relationKeysStrings.add(portData.parentKeysAttr);
				parentPort.relationKeysArrays.add(portData.parentKeys);
			}
		}
		
		portData.children = readInPortsDefinitionFromXml(graph, mappingElement.getChildNodes(), portData, allPortDefinitionMap);
		allPortDefinitionMap.put(portData.portIndex, portData);
		return portData;
	}

    /**
     * Creates org.w3c.dom.Document object from the given String.
     * 
     * @param inString
     * @return
     * @throws XMLConfigurationException
     */
    private static Document createDocumentFromString(String inString) throws XMLConfigurationException {
        InputSource is = new InputSource(new StringReader(inString));
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setCoalescing(true);
        Document doc;
        try {
            doc = dbf.newDocumentBuilder().parse(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }

    /**
     * Creates org.w3c.dom.Document object from the given ReadableByteChannel.
     * 
     * @param readableByteChannel
     * @return
     * @throws XMLConfigurationException
     */
    public static Document createDocumentFromChannel(ReadableByteChannel readableByteChannel) throws XMLConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc;
        try {
            doc = dbf.newDocumentBuilder().parse(Channels.newInputStream(readableByteChannel));
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }

    
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if(!checkInputPorts(status, 1, Integer.MAX_VALUE)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }
		
		//Check whether XML mapping schema is valid
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputStream is = null;
			if (this.mappingURL != null) {
				String filePath = FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), mappingURL);
				is = new FileInputStream(new File(filePath));
			} else if (this.mappingString != null) {
				is = new ByteArrayInputStream(mappingString.getBytes(charset));
	        }
			if (is != null) {
				saxParser.parse(is, handler);
				Set<String> attributesNames = ((MyHandler) handler).getAttributesNames();
				for (String attributeName : attributesNames) {
					if (!isXMLAttribute(attributeName)) {
						status.add(new ConfigurationProblem("Can't resolve XML attribute: " + attributeName, Severity.WARNING, this, Priority.NORMAL));
					}
				}
			}
		} catch (Exception e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage("Can't parse XML mapping schema.", e), Severity.ERROR, this, Priority.NORMAL));
		}
        
		//...
		
        return status;
	}
	
	private static class MyHandler extends DefaultHandler { 
		//Handler used at checkConfig to parse XML mapping and retrieve attributes names
		private Set<String> attributesNames = new HashSet<String>();
		
		@Override
		public void startElement(String namespaceURI, String localName, String qName, Attributes atts) { 
			int length = atts.getLength(); 
			for (int i=0; i<length; i++) { 
				attributesNames.add(atts.getQName(i)); 
			}
		}
		
		public Set<String> getAttributesNames() {
			return attributesNames;
		}
	}
	
	private boolean isXMLAttribute(String attribute) {
		//returns true if given attribute is known XML attribute
		if (attribute.equals(XML_MAPPING_ELEMENT) ||
				attribute.equals(XML_INDEX_ATTRIBUTE) ||
				attribute.equals(XML_KEYS_ATTRIBUTE) ||
				attribute.equals(XML_PARENT_KEYS_ATTRIBUTE) ||
				attribute.equals(XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE) ||
				attribute.equals(XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE) ||
				attribute.equals(XML_ELEMENT_ATTRIBUTE) ||
				attribute.equals(XML_FIELDS_AS_ATTRIBUTE) ||
				attribute.equals(XML_FIELDS_AS_EXCEPT_ATTRIBUTE) ||
				attribute.equals(XML_FIELDS_IGNORE_ATTRIBUTE) ||
				attribute.equals(XML_NAMESPACES_ATTRIBUTE) ||
				attribute.equals(XML_DEFAULT_NAMESPACE_ATTRIBUTE) ||
				attribute.equals(XML_NAMESPACE_PREFIX_ATTRIBUTE) ) {
			return true;
		}
		
		return false;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	@Override
	public synchronized void free() {
		super.free();
		if (writer != null)
			try {
				writer.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'.", t);
			}
	}

	/**
	 * Sets make directory.
	 * @param mkDir - true - creates output directories for output file
	 */
	public void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}

	public void setRootElement(String rootElement) {
		this.rootElement = rootElement;
	}

	public void setRecordsPerFile(int recordsPerFile) {
		this.recordsPerFile = recordsPerFile;
	}

	public void setRecordsCount(int recordsCount) {
		this.recordsCount = recordsCount;
	}

	public void setRecordsSkip(int recordsSkip) {
		this.recordsSkip = recordsSkip;
	}

	public void setRootNamespaces(String rootNamespaces) {
		this.namespacesString = rootNamespaces;
	}

	public void setXsdSchemaLocation(String xsdSchemaLocation) {
		this.xsdSchemaLocation = xsdSchemaLocation;
	}

	public void setRootDefaultNamespace(String rootDefaultNamespace) {
		this.rootDefaultNamespace = rootDefaultNamespace;
	}

	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}

	public void setDtdSystemId(String dtdSystemId) {
		this.dtdSystemId = dtdSystemId;
	}

	public void setDtdPublicId(String dtdPublicId) {
		this.dtdPublicId = dtdPublicId;
	}

	public void setRootInfoAttributes(boolean rootInfoAttributes) {
		this.rootInfoAttributes = rootInfoAttributes;
	}

	public void setUseRootElement(boolean useRootElement) {
		this.useRootElement = useRootElement;
	}

	public void setOmitNewLines(boolean omitNewLines) {
		this.omitNewLines = omitNewLines;
	}

	public void setMappingNodes(NodeList mappingNodes) {
		this.mappingNodes = mappingNodes;
	}

	public void setMapping(String mappingString) {
		this.mappingString = mappingString;
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
	}

	public void setCompressLevel(int integer) {
		this.compressLevel = integer;
	}
	
}
