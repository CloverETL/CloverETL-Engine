/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
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
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * This writer component reads data records from any number of input ports 
 * and according to ports mapping definition of relations among records, creates structured XML file.
 *
 * TODO
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
	public final static String COMPONENT_TYPE = "XML_WRITER";

	private static final String XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE = "keyToParent";
	private static final String XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE = "keyFromParent";
	private static final String XML_FILE_URL_ATTRIBUTE = "fileUrl";
	private static final String XML_KEYS_ATTRIBUTE = "key";
	private static final String XML_INDEX_ATTRIBUTE = "inPort";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_MAPPING_ATTRIBUTE = "mapping";
	private static final String XML_MAPPING_ELEMENT = "Mapping";
	private static final String XML_ELEMENT_ATTRIBUTE = "element";
	private static final String XML_FIELDS_AS_ATTRIBUTE = "fieldsAs";
	private static final String XML_FIELDS_AS_EXCEPT_ATTRIBUTE = "fieldsAsExcept";
	private static final String XML_FIELDS_IGNORE_ATTRIBUTE = "fieldsIgnore";
	
	private static final String XML_RECORDS_PER_FILE_ATTRIBUTE = "recordsPerFile";
	private static final String XML_RECORDS_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORDS_COUNT_ATTRIBUTE = "recordCount";
	
	private static final String ELEMENT_RECORDS = "records";
	private static final String ELEMENT_RECORD = "record";
	private static final String ATTRIBUTE_COMPONENT_ID = "component";
	private static final String ATTRIBUTE_GRAPH_NAME = "graph";
	private static final String XML_ROOT_ELEMENT_ATTRIBUTE = "rootElement";
	private static final String ATTRIBUTE_CREATED = "created";
	protected int initialCapacity = 50;

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
	private String charset = null;

	private int recordsSkip = 0;
	private int recordsCount = 0;
	private int recordsPerFile = 1;

	/**
	 * XmlFormatter which methods are called from MultiFileWriter. 
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Jan 4, 2008
	 */
	protected class XmlFormatter implements Formatter {
		//File outFile = null;
		OutputStream os = null; 
		TransformerHandler th = null;
			
		public void close() {
		}

		public void flush() throws IOException {
		}
		
		public void finish() throws IOException {
			if (th!= null)
				writeFooter();
		}

		public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		}

		public void reset() {
		}
		/*
		 * (non-Javadoc)
		 * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
		 */
		public void setDataTarget(Object outputDataTarget) {
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
		public int write(DataRecord record) throws IOException {
			if (th == null)
				writeHeader();
			RecordKey recKey = new RecordKey( rootPortDefinition.keys, rootPortDefinition.metadata);
			HashKey key = new HashKey(recKey, record);
			TreeRecord tr = rootPortDefinition.dataMap.get(key);
			List<DataRecord> records = new ArrayList<DataRecord>();
			records.add(tr.record);
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
		/** Comma separated list of columns names, which identify records from this port. Cannot be null. */
		String keysAttr = null;
		/** List of columns names, which identify records from this port. Cannot be null. */
		String[] keys = null;
		/** Comma separated list of columns from this record which identify parent record. */
		String relationKeysToParentAttr = null;
		/** List of columns from this record which identify parent record. */
		String[] relationKeysToParent = null;
		/** Comma separated list of columns from parent record which identify this record. */
		String relationKeysFromParentAttr = null;
		/** List of columns from parent record which identify this record. */
		String[] relationKeysFromParent = null;
		/** List of children definitions. */
		List<PortDefinition> children;
		/** Parent port definition. It's null for root definition. */
		PortDefinition parent;
		/** Data read from this port, stored by "primary key" (keysAttr).  */
		LinkedHashMap<HashKey, TreeRecord> dataMap = new LinkedHashMap<HashKey, TreeRecord>();
		/** Data read from this port, stored by parent key (relationKeysToParentAttr). Organizing records by parent's key. */
		LinkedHashMap<HashKey, TreeRecord> dataMapByRelationKey = new LinkedHashMap<HashKey, TreeRecord>();
		DataRecordMetadata metadata;
		
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
		public String toString(){
			return "PortDefinition#"+portIndex + " keys:"+keysAttr+" relationKeysToParent:"+relationKeysToParentAttr + " relationKeysFromParent:"+relationKeysFromParentAttr;
		}
		/** Resets this instance for next execution without graph init. */
		public void reset() {
			dataMap = new LinkedHashMap<HashKey, TreeRecord>();			
			dataMapByRelationKey = new LinkedHashMap<HashKey, TreeRecord>();
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
		RecordKey recKey;
		DataRecordMetadata metadata;

		public InputReader(int index, PortDefinition portDefinition) {
			super(Thread.currentThread().getName() + ".InputThread#" + index);
			this.portDefinition = portDefinition;
			runIt = true;
			inPort = getInputPort(index);
			metadata = inPort.getMetadata();
			portDefinition.metadata = metadata; 
		}
		
		public void run() {
			while (runIt) {
				try {
					DataRecord record = new DataRecord(metadata);
					record.init();
					if (inPort.readRecord(record) == null) { // no more input data
						return;
					}
					recKey = new RecordKey( portDefinition.keys, metadata);
					HashKey key = new HashKey(recKey, record);
					//TreeRecord item = portDefinition.dataMap.get(key);

					// -- add tp primary storage - map by primary KEY

					TreeRecord item = new TreeRecord();
					item.record = record;
					portDefinition.dataMap.put(key, item);

					// -- add to Map storage - map by relation key
					// only relation from child to parent
					if (portDefinition.relationKeysToParent != null){
						recKey = new RecordKey( portDefinition.relationKeysToParent, metadata);
						HashKey relationKey = new HashKey(recKey, record);
						TreeRecord tr = portDefinition.dataMapByRelationKey.get(relationKey);
						if (tr == null){
							tr = new TreeRecord();
							portDefinition.dataMapByRelationKey.put(relationKey, tr);
						}
						List<DataRecord> recordsList = tr.records; 
						if (recordsList == null){
							recordsList = new ArrayList<DataRecord>();
							tr.records = recordsList;
						}
						recordsList.add(record);
					}
					
				} catch (InterruptedException e) {
					logger.error(getId() + ": thread forcibly aborted", e);
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
		public DataRecord record;
		/** this attribute is set when TreeRecord instance is stored in collection with not unique key */
		public List<DataRecord> records; 
	}// nested class TreeRecord 


	/**
	 * Constructor. Other necessary attributes are set with injection. 
	 * @param id
	 * @param fileUrl
	 * @param rootElement
	 * @param allPortDefinitionMap
	 * @param recordsPerFile 
	 * @param recordsCount 
	 * @param rootPortDefinitionList
	 */
	public XmlWriter(String id, String fileUrl, String rootElement, Map<Integer, PortDefinition> allPortDefinitionMap, PortDefinition rootPortDefinition, int recordsSkip, int recordsCount, int recordsPerFile) {
		super(id);
		this.fileUrl = fileUrl;
		this.rootElement = rootElement;
		this.allPortDefinitionMap = allPortDefinitionMap;
		this.rootPortDefinition = rootPortDefinition;
		this.recordsSkip = recordsSkip;
		this.recordsCount = recordsCount;
		this.recordsPerFile = recordsPerFile;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		portsCnt = inPorts.size();

        // test that we have at least one input port and one output
        if (portsCnt < 1) {
            throw new ComponentNotReadyException(getId() + ": At least one output port has to be defined!");
        }

        if (charset == null)
        	charset = "UTF-8";
            //throw new ComponentNotReadyException(getId() + ": Specify charset of out file");

        /*
        if (rootPortDefinition.size() < 1) {
            throw new ComponentNotReadyException(
                    getId()
                    + ": At least one mapping has to be defined.  <Mapping element=\"elementToCreate\" inPort=\"123\" ... />");
        }*/

        XmlFormatter formatter = new XmlFormatter(); 
        writer = new MultiFileWriter(formatter, getGraph() != null ? getGraph().getProjectURL() : null, this.fileUrl);
        writer.setLogger(logger);
        writer.setRecordsPerFile(this.recordsPerFile);
        writer.setAppendData(false);
        writer.setSkip(this.recordsSkip);
        writer.setNumRecords(this.recordsCount);
		writer.setUseChannel(true);
        writer.setLookupTable(null);
        //writer.setPartitionKeyNames(partitionKey);
        //writer.setPartitionFileTag(partitionFileTagType);
        writer.init( this.rootPortDefinition.metadata );
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
					logger.warn(getId() + "thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}// while
		}// for
		try {
			
			// and now ... data structure is read ... writing can be processed 
			for (TreeRecord treeRecord : rootPortDefinition.dataMap.values()){
				// multiWriter will call formatter.write
				writer.write(treeRecord.record);
			}// for 
			
			//flushXmlSax();
		} catch (Exception e) {
			logger.error("Error during creating XML file", e);
		} finally {
			writer.finish();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
		serializer.setOutputProperty(OutputKeys.ENCODING, this.charset);
		//serializer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,"users.dtd");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		hd.setResult(streamResult);
		hd.startDocument();
		// if there may be more then 1 record in document, there has to be wrapping root element 
		if (recordsPerFile>1){
			 AttributesImpl atts = new AttributesImpl();
			 atts.addAttribute( "", "", ATTRIBUTE_COMPONENT_ID,"CDATA", getId());
			 atts.addAttribute( "","",ATTRIBUTE_GRAPH_NAME,"CDATA",this.getGraph().getName());
			 atts.addAttribute( "", "", ATTRIBUTE_CREATED,"CDATA", (new Date()).toString());
			 String root = rootElement!=null ? rootElement : ELEMENT_RECORDS; 
			 hd.startElement("","",root,atts);
		}
		return hd;
	}

	private void createFooter(OutputStream os, TransformerHandler hd) throws TransformerConfigurationException, SAXException, IOException {
		try {
			if (recordsPerFile>1){
				 String root = rootElement!=null ? rootElement : ELEMENT_RECORDS; 
				 hd.endElement("","",root);
			}
			hd.endDocument();
		} finally {
			 os.close();
		}
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
			 String outElementName = portDefinition.element == null ? ELEMENT_RECORD : portDefinition.element;
			 

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
				 atts.addAttribute("","", dataRecord.getMetadata().getField(i).getName(),"CDATA", value);
			 } // for			 
			 
			 hd.startElement("","", outElementName, atts);

			 // fields as elements
			 for (int x=0; x<portDefinition.fieldsAsElementsIndexes.length; x++){
				 int i = portDefinition.fieldsAsElementsIndexes[x];
				 DataField field = dataRecord.getField(i);
				 atts.clear();
				 hd.startElement("","",dataRecord.getMetadata().getField(i).getName(),atts);
				 String value = field.toString();
				 hd.characters(value.toCharArray(),0,value.length());
				 hd.endElement("","",dataRecord.getMetadata().getField(i).getName());
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
				 if (child.relationKeysToParentAttr != null){
					 // use parent primaryKey from parent metadata as key for searching in mapByRelationKey
					 RecordKey recKey = new RecordKey( portDefinition.keys, portDefinition.metadata);
					 HashKey key = new HashKey(recKey, dataRecord);
					 TreeRecord tr = child.dataMapByRelationKey.get(key);
					 if (tr != null && tr.records != null)
						 addRecords(hd, tr.records, child);
				 }//if

				 // reference from parent to child
				 if (child.relationKeysFromParentAttr != null){
					 // use parent relation key from parent metadata as key for searching in data map
					 RecordKey recKey = new RecordKey( child.relationKeysFromParent, portDefinition.metadata);
					 HashKey key = new HashKey(recKey, dataRecord);
					 TreeRecord tr = child.dataMap.get(key);
					 if (tr != null && tr.record != null){
						List<DataRecord> records = new ArrayList<DataRecord>();
						records.add(tr.record);
						addRecords(hd, records, child);
					 }
				 }//if
				 
			 }// for children

			 //hd.endElement("","",ELEMENT_RECORD);
			 hd.endElement("","",outElementName);
		 } // for
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
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
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		XmlWriter writer = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		PortDefinition rootPortDefinition = null;
		Map<Integer, PortDefinition> allPortDefinitionMap = new HashMap<Integer,PortDefinition>();
		try {
	        if(xattribs.exists(XML_MAPPING_ATTRIBUTE)) {
	            //read mapping from string in attribute 'mapping'
	            String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE);
	            Document doc = createDocumentFromString(mapping);
	            
	            Element mappingRoot = doc.getDocumentElement();
				PortDefinition portDef = readInPortDef(graph, (PortDefinition)null, allPortDefinitionMap, mappingRoot);
				rootPortDefinition = portDef;
	        } else {
	            //old-fashioned version of mapping definition
	            //mapping xml elements are child nodes of the component
	        	Element mappingParent = xmlElement; 
	        	List<PortDefinition> list = readInPortsDefinitionFromXml(graph, mappingParent, (PortDefinition)null, allPortDefinitionMap);
	        	if (list.size() != 1)
	 	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," more then 1 mapping element") + ":");
	        		
	        	rootPortDefinition = list.get(0);
	        }
		} catch (AttributeNotFoundException e) {
			logger.error("cannot instantiate node from XML", e);
		}
		
		try {
			String fileUrl = xattribs.getString(XML_FILE_URL_ATTRIBUTE);
			int recordsSkip = xattribs.getInteger(XML_RECORDS_SKIP_ATTRIBUTE, 0);
			int recordsCount = xattribs.getInteger(XML_RECORDS_COUNT_ATTRIBUTE, 0);
			int recordsPerFile = xattribs.getInteger(XML_RECORDS_PER_FILE_ATTRIBUTE, 1);
			writer = new XmlWriter(xattribs.getString(XML_ID_ATTRIBUTE), fileUrl, xattribs.getString(XML_ROOT_ELEMENT_ATTRIBUTE), allPortDefinitionMap, rootPortDefinition, 
					recordsSkip, recordsCount, recordsPerFile);
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE))
				writer.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
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
	 */
	private static List<PortDefinition> readInPortsDefinitionFromXml(TransformationGraph graph, Element xmlElement, PortDefinition parentPort, Map<Integer, PortDefinition> allPortDefinitionMap ) throws AttributeNotFoundException {
		List<PortDefinition> portDataList = new ArrayList<PortDefinition>();
		NodeList ports = xmlElement.getChildNodes();//.getElementsByTagName(XML_INPORT_ELEMENT);
		for (int i=0; i<ports.getLength(); i++){
			org.w3c.dom.Node portDescNode = ports.item(i);
			if ( !XML_MAPPING_ELEMENT.equals( portDescNode.getNodeName() ) )
				continue;
			org.w3c.dom.Element portDesc = (org.w3c.dom.Element)ports.item(i);
			PortDefinition portData = readInPortDef(graph, parentPort, allPortDefinitionMap, portDesc);
			portDataList.add(portData);
		}// for
		return portDataList;
	}

	/**
	 * Reads one single PortDefinitions during initialization this component from XML. 
	 * @param graph
	 * @param parentPort
	 * @param allPortDefinitionMap
	 * @param portDesc
	 * @return
	 * @throws AttributeNotFoundException
	 */
	private static PortDefinition readInPortDef(TransformationGraph graph, PortDefinition parentPort, 
			Map<Integer,PortDefinition> allPortDefinitionMap, org.w3c.dom.Element portDesc
			) throws AttributeNotFoundException {
		PortDefinition portData = new PortDefinition();
		ComponentXMLAttributes portAttribs = new ComponentXMLAttributes(portDesc, graph);
		portData.parent = parentPort;
		portData.portIndex = portAttribs.getInteger(XML_INDEX_ATTRIBUTE);
		portData.element = portAttribs.getString(XML_ELEMENT_ATTRIBUTE);
		portData.keysAttr = portAttribs.getString(XML_KEYS_ATTRIBUTE);
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
		if (portDesc.getAttribute(XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE).length()>0){
			portData.relationKeysToParentAttr = portAttribs.getString(XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE);
			if (portData.relationKeysToParentAttr != null)
				portData.relationKeysToParent = portData.relationKeysToParentAttr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		}
		if (portDesc.getAttribute(XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE).length()>0){
			portData.relationKeysFromParentAttr = portAttribs.getString(XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE);
			if (portData.relationKeysFromParentAttr != null)
				portData.relationKeysFromParent = portData.relationKeysFromParentAttr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		}
		portData.children = readInPortsDefinitionFromXml(graph, portDesc, portData, allPortDefinitionMap);
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

    
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if(!checkInputPorts(status, 1, Integer.MAX_VALUE)
				|| !checkOutputPorts(status, 0, 0)) {
			return status;
		}
		
		//...
		
        return status;
	}

	private void setCharset(String charset) {
		this.charset = charset;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		this.writer.reset();
		for (PortDefinition def : allPortDefinitionMap.values()){
			def.reset();
		}// for
	}

	@Override
	public synchronized void free() {
		super.free();
		writer.close();
	}

	/**
	 * This attribute has to be set just oncafter 
	 * @param allPortDefinitionMap
	 */
	/*
	private void setAllPortDefinitionMap(Map<Integer,PortDefinition> allPortDefinitionMap) {
		this.allPortDefinitionMap = allPortDefinitionMap;
	}

	private void setRootPortDefinitionList(List<PortDefinition> rootPortDefinitionList) {
		this.rootPortDefinitionList = rootPortDefinitionList;
	}
	*/

}
