/**
 * 
 */
package org.jetel.component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.FileRecordBuffer;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CodeParser;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;
import org.jetel.util.StringAproxComparator;
import org.jetel.util.SynchronizeUtils;

/**
 * @author avackova
 *
 */
public class AproxMergeJoin extends Node {

	private static final String XML_SLAVE_OVERWRITE_KEY_ATTRIBUTE = "slaveOverwriteKey";
	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";
	private static final String XML_REFERENCE_KEY_ATTRIBUTE="referenceKey";
	private static final String XML_SLAVE_REF_OVERWRITE_ATTRIBUTE = "slaveRefOverwrite";
	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE = "transformClassForSuspicious";
	private static final String XML_LIBRARY_PATH_ATTRIBUTE = "libraryPath";
	private static final String XML_LIBRARY_PATH_FOR_SUSPICIOUS_ATTRIBUTE = "libraryPathForSuspicious";
	private static final String XML_JAVA_SOURCE_ATTRIBUTE = "javaSource";
	private static final String XML_JAVA_SOURCE_FOR_SUSPICIOUS_ATTRIBUTE = "javaSourceForSuspicious";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE = "transformForSuspicious";
	private static final String XML_COMPARISON_STRENGHT_ATTRIBUTE = "strenght";
	private static final String XML_CONFORMITY_ATTRIBUTE = "conformity";
	
	public final static String COMPONENT_TYPE = "APROX_MERGE_JOIN";

	private final static int CONFORMING_OUT = 0;
	private final static int SUSPICIOUS_OUT = 1;
	private final static int NOT_MATCH_DRIVER_OUT = 2;
	private final static int NOT_MATCH_SLAVE_OUT = 3;
	private final static int DRIVER_ON_PORT = 0;
	private final static int SLAVE_ON_PORT = 1;
	
	private final static double DEFAULT_CONFORMITY_LIMIT=0.75;

	private final static int CURRENT = 0;
	private final static int PREVIOUS = 1;
	private final static int TEMPORARY = 1;

	private String transformClassName;
	private String libraryPath = null;
	private String transformSource = null;
	private String transformClassNameForSuspicious;
	private String libraryPathForSuspicious = null;
	private String transformSourceForSuspicious = null;

	private RecordTransform transformation = null;
	private DynamicJavaCode dynamicTransformation = null;
	private RecordTransform transformationForSuspicious = null;
	private DynamicJavaCode dynamicTransformationForSuspicious = null;

	private String[] joinKeys;
	private String[] slaveOverwriteKeys = null;
	private String[] referenceKey=new String[1];
	private String[] slaveReferenceKey=null;

	private RecordKey[] recordKey;
	private int[][] fieldsToCompare=new int[2][];
	
	private StringAproxComparator comparator;
	private int[] maxDiffrenceLetters;
	private int maxDiffrence; //max result can be obtained from method diffrence

	private double conformityLimit;
	
	private ByteBuffer dataBuffer;
	private FileRecordBuffer recordBuffer;

	// for passing data records into transform function
	private final static DataRecord[] inRecords = new DataRecord[2];
	private DataRecord[] outConformingRecords=new DataRecord[2];
	private DataRecord[] outSuspiciousRcords = new DataRecord[2];

	private Properties transformationParameters;
	private Properties transformationParametersForSuspicious;
	
	static Log logger = LogFactory.getLog(MergeJoin.class);

	/**
	 * @param id
	 */
	public AproxMergeJoin(String id,String[] joinKeys,int[] maxDiffrences,
			String referenceKey,String transformClass) throws JetelException{
		super(id);
		this.joinKeys = joinKeys;
		this.maxDiffrenceLetters=maxDiffrences;
		this.referenceKey[0]=referenceKey;
		this.transformClassName = transformClass;
		this.comparator=new StringAproxComparator();
	}

	public AproxMergeJoin(String id, String[] joinKeys,int[] maxDiffrences, 
			String referenceKey, DynamicJavaCode dynaTransCode)  throws JetelException{
		super(id);
		this.joinKeys = joinKeys;
		this.maxDiffrenceLetters=maxDiffrences;
		this.referenceKey[0]=referenceKey;
		this.dynamicTransformation=dynaTransCode;
		this.comparator=new StringAproxComparator();
	}

	public AproxMergeJoin(String id, String[] joinKeys,int[] maxDiffrences,
			String referenceKey, String transform, boolean distincter) throws JetelException{
		super(id);
		this.joinKeys = joinKeys;
		this.maxDiffrenceLetters=maxDiffrences;
		this.referenceKey[0]=referenceKey;
		this.transformSource = transform;
		this.comparator=new StringAproxComparator();
	}

	/**
	 *  Sets specific key (string) for slave records<br>
	 *  Can be used if slave record has different names
	 *  for fields composing the key
	 *
	 * @param  slaveKeys  The new slaveOverrideKey value
	 */
	private void setSlaveOverrideKey(String[] slaveKeys) {
		this.slaveOverwriteKeys = slaveKeys;
	}

	private void setSlaveReferenceKey(String slaveRefKey){
		slaveReferenceKey=new String[1];
		this.slaveReferenceKey[0]=slaveRefKey;
	}
	
	private void setComparatorStrenght(boolean[] strenght) throws JetelException{
		this.comparator.setStrentgh(strenght[0],strenght[1],strenght[2],strenght[3]);
		int m=comparator.getMaxCostForOneLetter();
		int max=0;
		for (int i=0;i<maxDiffrenceLetters.length;i++){
			max+=m*(maxDiffrenceLetters[i]+1);
		}
		maxDiffrence=max;
	}
	/**
	 *  Populates record buffer with all slave records having the same key
	 *
	 * @param  port                      Description of the Parameter
	 * @param  nextRecord                next record from slave
	 * @param  key                       Description of the Parameter
	 * @param  currRecord                Description of the Parameter
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 * @exception  JetelException        Description of the Exception
	 */
	private void fillRecordBuffer(InputPort port, DataRecord currRecord, DataRecord nextRecord, RecordKey key)
			 throws IOException, InterruptedException, JetelException {

		recordBuffer.clear();
		if (currRecord != null) {
			dataBuffer.clear();
			currRecord.serialize(dataBuffer);
			dataBuffer.flip();
			recordBuffer.push(dataBuffer);
			while (nextRecord != null) {
				nextRecord = port.readRecord(nextRecord);
				if (nextRecord != null) {
					switch (key.compare(currRecord, nextRecord)) {
						case 0:
							dataBuffer.clear();
							nextRecord.serialize(dataBuffer);
							dataBuffer.flip();
							recordBuffer.push(dataBuffer);
							break;
						case -1:
							return;
						case 1:
							throw new JetelException("Slave record out of order!");
					}
				}
			}
		}
	}

	/**
	 *  Finds corresponding slave record for current driver (if there is some)
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  slavePort                 Description of the Parameter
	 * @param  key                       Description of the Parameter
	 * @return                           The correspondingRecord value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private int getCorrespondingRecord(DataRecord driver, DataRecord slave, 
			InputPort slavePort, OutputPort outSlave, RecordKey[] key)
			 throws IOException, InterruptedException {

		while (slave != null) {
			switch (key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT], driver, slave)) {
				case 1:
					outSlave.writeRecord(slave);
					slave = slavePort.readRecord(slave);
					break;
				case 0:
					return 0;
				case -1:
					return -1;
			}
		}
		return -1;
		// no more records on slave port
	}

	private double diffrence(DataRecord r1,DataRecord r2,int[][] fieldsToCompare,int[] diff){
		int r=0;
		for (int i=0;i<fieldsToCompare[DRIVER_ON_PORT].length;i++){
			comparator.setMaxLettersToChange(diff[i]);
			String locale=r1.getField(fieldsToCompare[DRIVER_ON_PORT][i]).getMetadata().getLocaleStr();
			if (locale != null){
				if (comparator.getLocale()==null || !locale.toUpperCase().startsWith(comparator.getLocale())){
					comparator.setLocale(locale);
				}
			}else if (comparator.getLocale()!=null){
				comparator.setLocale("");
			}
			r+=comparator.distance(
					r1.getField(fieldsToCompare[DRIVER_ON_PORT][i]).getValue().toString(),
					r2.getField(fieldsToCompare[SLAVE_ON_PORT][i]).getValue().toString());
		}
		return (double)r/maxDiffrence;
	}
	
	/**
	 *  Outputs all combinations of current driver record and all slaves with the
	 *  same key
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  out                       Description of the Parameter
	 * @param  port                      Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private boolean flushCombinations(DataRecord driver, DataRecord slave, DataRecord outConforming,
			DataRecord outSuspicious, OutputPort conforming, OutputPort suspicious)
			 throws IOException, InterruptedException {
		recordBuffer.rewind();
		dataBuffer.clear();
		inRecords[0] = driver;
		inRecords[1] = slave;
		outConformingRecords[0] = outConforming;
		outSuspiciousRcords[0] = outSuspicious;

		while (recordBuffer.shift(dataBuffer) != null) {
			dataBuffer.flip();
			slave.deserialize(dataBuffer);
			double conformity=1-diffrence(driver,slave,fieldsToCompare,maxDiffrenceLetters);
			// **** call transform function here ****
			if (conformity>=conformityLimit) {
				if (!transformation.transform(inRecords, outConformingRecords)) {
					resultMsg = transformation.getMessage();
					return false;
				}
				conforming.writeRecord(outConforming);
			}else{
				if (!transformationForSuspicious.transform(inRecords,outSuspiciousRcords)){
					resultMsg = transformation.getMessage();
					return false;
				}
				suspicious.writeRecord(outSuspicious);
			}
			dataBuffer.clear();
		}
		return true;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  metadata  Description of the Parameter
	 * @param  count     Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private DataRecord[] allocateRecords(DataRecordMetadata metadata, int count) {
		DataRecord[] data = new DataRecord[count];

		for (int i = 0; i < count; i++) {
			data[i] = new DataRecord(metadata);
			data[i].init();
		}
		return data;
	}

	public void run() {
		boolean isDriverDifferent;

		// get all ports involved
		InputPort driverPort = getInputPort(DRIVER_ON_PORT);
		InputPort slavePort = getInputPort(SLAVE_ON_PORT);
		OutputPort conformingPort = getOutputPort(CONFORMING_OUT);
		OutputPort suspiciousPort = getOutputPort(SUSPICIOUS_OUT);
		OutputPort notMatchDriverPort = getOutputPort(NOT_MATCH_DRIVER_OUT);
		OutputPort notMatchSlavePort = getOutputPort(NOT_MATCH_SLAVE_OUT);

		//initialize input records driver & slave
		DataRecord[] driverRecords = allocateRecords(driverPort.getMetadata(), 2);
		DataRecord[] slaveRecords = allocateRecords(slavePort.getMetadata(), 2);

		// initialize output record
		DataRecordMetadata outConformingMetadata = conformingPort.getMetadata();
		DataRecord outConformingRecord = new DataRecord(outConformingMetadata);
		outConformingRecord.init();

		DataRecordMetadata outSuspiciousMetadata = suspiciousPort.getMetadata();
		DataRecord outSuspiciousRecord = new DataRecord(outSuspiciousMetadata);
		outSuspiciousRecord.init();
		
		// tmp record for switching contents
		DataRecord tmpRec;
		// create file buffer for slave records - system TEMP path
		recordBuffer = new FileRecordBuffer(null);

		//for the first time (as initialization), we expect that records are different
		isDriverDifferent = true;
		try {
			// first initial load of records
			driverRecords[CURRENT] = driverPort.readRecord(driverRecords[CURRENT]);
			slaveRecords[CURRENT] = slavePort.readRecord(slaveRecords[CURRENT]);
			while (runIt && driverRecords[CURRENT] != null) {
				if (isDriverDifferent) {
					switch (getCorrespondingRecord(driverRecords[CURRENT], slaveRecords[CURRENT], slavePort, notMatchSlavePort, recordKey)) {
						case -1:
							// driver lower
							// no corresponding slave
							notMatchDriverPort.writeRecord(driverRecords[CURRENT]);
							driverRecords[CURRENT] = driverPort.readRecord(driverRecords[CURRENT]);
							isDriverDifferent = true;
							continue;
						case 0:
							// match
							fillRecordBuffer(slavePort, slaveRecords[CURRENT], slaveRecords[TEMPORARY], recordKey[SLAVE_ON_PORT]);
							// switch temporary --> current
							tmpRec = slaveRecords[CURRENT];
							slaveRecords[CURRENT] = slaveRecords[TEMPORARY];
							slaveRecords[TEMPORARY] = tmpRec;
							isDriverDifferent = false;
							break;
					}
				}
				flushCombinations(driverRecords[CURRENT], slaveRecords[TEMPORARY], 
						outConformingRecord, outSuspiciousRecord, 
						conformingPort,suspiciousPort);
				// get next driver
				
				driverRecords[TEMPORARY] = driverPort.readRecord(driverRecords[TEMPORARY]);
				if (driverRecords[TEMPORARY] != null) {
					// different driver record ??
					switch (recordKey[DRIVER_ON_PORT].compare(driverRecords[CURRENT], driverRecords[TEMPORARY])) {
						case 0:
							break;
						case -1:
							// detected change;
							isDriverDifferent = true;
							break;
						case 1:
							throw new JetelException("Driver record out of order!");
					}
				}
				// switch temporary --> current
				tmpRec = driverRecords[CURRENT];
				driverRecords[CURRENT] = driverRecords[TEMPORARY];
				driverRecords[TEMPORARY] = tmpRec;
				SynchronizeUtils.cloverYield();
			}
			// if full outer join defined and there are some slave records left, flush them
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		// signal end of records stream to transformation function
		transformation.finished();
		broadcastEOF();		
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}

	public void init() throws ComponentNotReadyException {
		Class tClass;
		// test that we have at two input ports and one output
		if (inPorts.size() != 2) {
			throw new ComponentNotReadyException("Two input ports have to be defined!");
		} else if (outPorts.size() != 4) {
			throw new ComponentNotReadyException("One output port has to be defined!");
		}
		if (slaveOverwriteKeys == null) {
			slaveOverwriteKeys = joinKeys;
		}
		RecordKey[] rk = new RecordKey[2];
		rk[DRIVER_ON_PORT] = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata());
		rk[SLAVE_ON_PORT] = new RecordKey(slaveOverwriteKeys, getInputPort(SLAVE_ON_PORT).getMetadata());
		rk[DRIVER_ON_PORT].init();
		rk[SLAVE_ON_PORT].init();
		fieldsToCompare[DRIVER_ON_PORT]=rk[DRIVER_ON_PORT].getKeyFields();
		fieldsToCompare[SLAVE_ON_PORT]=rk[SLAVE_ON_PORT].getKeyFields();
		if (slaveReferenceKey == null){
			slaveReferenceKey=referenceKey;
		}
		recordKey = new RecordKey[2];
		recordKey[DRIVER_ON_PORT] = new RecordKey(referenceKey, getInputPort(DRIVER_ON_PORT).getMetadata());
		recordKey[SLAVE_ON_PORT] = new RecordKey(slaveReferenceKey, getInputPort(SLAVE_ON_PORT).getMetadata());
		recordKey[DRIVER_ON_PORT].init();
		recordKey[SLAVE_ON_PORT].init();
		if (transformation == null) {
			if (transformClassName != null) {
				// try to load in transformation class & instantiate
				try {
					tClass = Class.forName(transformClassName);
				} catch (ClassNotFoundException ex) {
					// let's try to load in any additional .jar library (if specified)
					if(libraryPath == null) {
						throw new ComponentNotReadyException("Can't find specified transformation class: " + transformClassName);
					}
					String urlString = "file:" + libraryPath;
					URL[] myURLs;
					try {
						myURLs = new URL[] { new URL(urlString) };
						URLClassLoader classLoader = new URLClassLoader(myURLs, Thread.currentThread().getContextClassLoader());
						tClass = Class.forName(transformClassName, true, classLoader);
					} catch (MalformedURLException ex1) {
						throw new RuntimeException("Malformed URL: " + ex1.getMessage());
					} catch (ClassNotFoundException ex1) {
						throw new RuntimeException("Can not find class: " + ex1);
					}
				}
				try {
					transformation = (RecordTransform) tClass.newInstance();
				} catch (Exception ex) {
					throw new ComponentNotReadyException(ex.getMessage());
				}
			} else {
			    if(dynamicTransformation == null) { //transformSource is set
			        //creating dynamicTransformation from internal transformation format
			        CodeParser codeParser = new CodeParser((DataRecordMetadata[]) getInMetadata().toArray(new DataRecordMetadata[0]), (DataRecordMetadata[]) getOutMetadata().toArray(new DataRecordMetadata[0]));
					codeParser.setSourceCode(transformSource);
					codeParser.parse();
					codeParser.addTransformCodeStub("Transform"+this.id+"ForConforming");
					// DEBUG
					// System.out.println(codeParser.getSourceCode());
			        dynamicTransformation = new DynamicJavaCode(codeParser.getSourceCode());
			        dynamicTransformation.setCaptureCompilerOutput(true);
			    }
				logger.info(" (compiling dynamic source) ");
				// use DynamicJavaCode to instantiate transformation class
				Object transObject = null;
				try {
				    transObject = dynamicTransformation.instantiate();
				} catch(RuntimeException ex) {
				    logger.debug(dynamicTransformation.getCompilerOutput());
				    logger.debug(dynamicTransformation.getSourceCode());
					throw new ComponentNotReadyException("Transformation code is not compilable.\n"
					        + "reason: " + ex.getMessage());
							
				}
				if (transObject instanceof RecordTransform) {
					transformation = (RecordTransform) transObject;
				} else {
					throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordTransform.");
				}
			}
		}
        transformation.setGraph(graph);
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[2];
		inMetadata[0]=getInputPort(DRIVER_ON_PORT).getMetadata();
		inMetadata[1]=getInputPort(SLAVE_ON_PORT).getMetadata();
		// put aside: getOutputPort(WRITE_TO_PORT).getMetadata()
		if (!transformation.init(transformationParameters,inMetadata, null)) {
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
		if (transformationForSuspicious == null) {
			if (transformClassNameForSuspicious != null) {
				// try to load in transformation class & instantiate
				try {
					tClass = Class.forName(transformClassNameForSuspicious);
				} catch (ClassNotFoundException ex) {
					// let's try to load in any additional .jar library (if specified)
					if(libraryPathForSuspicious == null) {
						throw new ComponentNotReadyException("Can't find specified transformation class: " + transformClassName);
					}
					String urlString = "file:" + libraryPathForSuspicious;
					URL[] myURLs;
					try {
						myURLs = new URL[] { new URL(urlString) };
						URLClassLoader classLoader = new URLClassLoader(myURLs, Thread.currentThread().getContextClassLoader());
						tClass = Class.forName(transformClassNameForSuspicious, true, classLoader);
					} catch (MalformedURLException ex1) {
						throw new RuntimeException("Malformed URL: " + ex1.getMessage());
					} catch (ClassNotFoundException ex1) {
						throw new RuntimeException("Can not find class: " + ex1);
					}
				}
				try {
					transformationForSuspicious = (RecordTransform) tClass.newInstance();
				} catch (Exception ex) {
					throw new ComponentNotReadyException(ex.getMessage());
				}
			} else {
			    if(dynamicTransformationForSuspicious == null) { //transformSource is set
			        //creating dynamicTransformation from internal transformation format
			        CodeParser codeParser = new CodeParser((DataRecordMetadata[]) getInMetadata().toArray(new DataRecordMetadata[0]), (DataRecordMetadata[]) getOutMetadata().toArray(new DataRecordMetadata[0]));
					codeParser.setSourceCode(transformSourceForSuspicious);
					codeParser.parse();
					codeParser.addTransformCodeStub("Transform"+this.id+"ForSuspicious");
					// DEBUG
					// System.out.println(codeParser.getSourceCode());
			        dynamicTransformationForSuspicious = new DynamicJavaCode(codeParser.getSourceCode());
			        dynamicTransformationForSuspicious.setCaptureCompilerOutput(true);
			    }
				logger.info(" (compiling dynamic source) ");
				// use DynamicJavaCode to instantiate transformation class
				Object transObject = null;
				try {
				    transObject = dynamicTransformationForSuspicious.instantiate();
				} catch(RuntimeException ex) {
				    logger.debug(dynamicTransformationForSuspicious.getCompilerOutput());
				    logger.debug(dynamicTransformationForSuspicious.getSourceCode());
					throw new ComponentNotReadyException("Transformation code is not compilable.\n"
					        + "reason: " + ex.getMessage());
							
				}
				if (transObject instanceof RecordTransform) {
					transformationForSuspicious = (RecordTransform) transObject;
				} else {
					throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordTransform.");
				}
			}
		}
        transformationForSuspicious.setGraph(graph);
		if (!transformationForSuspicious.init(transformationParametersForSuspicious,inMetadata, null)) {
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
	}

    /**
     * @param transformationParameters The transformationParameters to set.
     */
    public void setTransformationParameters(Properties transformationParameters) {
        this.transformationParameters = transformationParameters;
    }

    public void setTransformationParametersForSuspicious(Properties transformationParameters) {
        this.transformationParametersForSuspicious = transformationParameters;
    }

    public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		AproxMergeJoin join;
		DynamicJavaCode dynaTransCode = null;

		try {
			String[] join_parameters=xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			String[] joinKeys=new String[join_parameters.length];
			int[] maxDiffrences=new int[join_parameters.length];;
			for (int i=0;i<join_parameters.length;i++){
				String[] pom=join_parameters[i].split(" ");
				joinKeys[i]=pom[0];
				if (pom.length!=1)
					maxDiffrences[i]=Integer.parseInt(pom[1]);
				else 
					maxDiffrences[i]=1;
			}
			if (xattribs.exists(XML_TRANSFORM_CLASS_ATTRIBUTE)){
				join = new AproxMergeJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						joinKeys,maxDiffrences,
						xattribs.getString(XML_REFERENCE_KEY_ATTRIBUTE),
						xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE));
				if (xattribs.exists(XML_LIBRARY_PATH_ATTRIBUTE)) {
					join.setLibraryPath(xattribs.getString(XML_LIBRARY_PATH_ATTRIBUTE));
				}
			}else{
				if (xattribs.exists(XML_JAVA_SOURCE_ATTRIBUTE)){
					dynaTransCode = new DynamicJavaCode(xattribs.getString(XML_JAVA_SOURCE_ATTRIBUTE));
				}else{
					// do we have child node wich Java source code ?
					try {
					    dynaTransCode = DynamicJavaCode.fromXML(graph, nodeXML);
					} catch(Exception ex) {
				        //do nothing
				    }				}
				if (dynaTransCode != null) {
					join = new AproxMergeJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
							joinKeys,maxDiffrences,
							xattribs.getString(XML_REFERENCE_KEY_ATTRIBUTE),
							dynaTransCode);
				} else { //last chance to find reformat code is in transform attribute
					if (xattribs.exists(XML_TRANSFORM_ATTRIBUTE)) {
						join = new AproxMergeJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
								joinKeys,maxDiffrences,
								xattribs.getString(XML_REFERENCE_KEY_ATTRIBUTE),
								xattribs.getString(XML_TRANSFORM_ATTRIBUTE), true); 
					} else {
						throw new RuntimeException("Can't create DynamicJavaCode object - source code not found !");
					}
				}
			}
			dynaTransCode = null;
			if (xattribs.exists(XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE)){
				join.setTransformClassNameForSuspicious(xattribs.getString(XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE));
				if (xattribs.exists(XML_LIBRARY_PATH_FOR_SUSPICIOUS_ATTRIBUTE)) {
					join.setLibraryPathForSuspicious(xattribs.getString(XML_LIBRARY_PATH_FOR_SUSPICIOUS_ATTRIBUTE));
				}
			}else{
				if (xattribs.exists(XML_JAVA_SOURCE_FOR_SUSPICIOUS_ATTRIBUTE)){
					dynaTransCode = new DynamicJavaCode(xattribs.getString(XML_JAVA_SOURCE_FOR_SUSPICIOUS_ATTRIBUTE));
				}else{
					// do we have child node wich Java source code ?
					try {
					    dynaTransCode = DynamicJavaCode.fromXML(graph, nodeXML);
					} catch(Exception ex) {
				        //do nothing
				    }				}
				if (dynaTransCode != null) {
					join.setDynamicTransformationForSuspicious(dynaTransCode);
				} else { //chance to find reformat code is in transform attribute
					if (xattribs.exists(XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE)) {
						join.setTransformSourceForSuspicious(	xattribs.getString(XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE), true); 
					}else{ //get transformation from output por 0
						
					}
				}
			}
			if (xattribs.exists(XML_SLAVE_OVERWRITE_KEY_ATTRIBUTE)) {
				join.setSlaveOverrideKey(xattribs.getString(XML_SLAVE_OVERWRITE_KEY_ATTRIBUTE).
						split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

			}
			if (xattribs.exists(XML_SLAVE_REF_OVERWRITE_ATTRIBUTE)) {
				join.setSlaveReferenceKey(xattribs.getString(XML_SLAVE_REF_OVERWRITE_ATTRIBUTE));
			}
			boolean[] strenght=new boolean[StringAproxComparator.IDENTICAL];
			if (xattribs.exists(XML_COMPARISON_STRENGHT_ATTRIBUTE)) {
				String[] pom=xattribs.getString(XML_COMPARISON_STRENGHT_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				for (int i=0;i<StringAproxComparator.IDENTICAL;i++){
					strenght[i]=pom[i].equalsIgnoreCase("true");
				}
			}else{
				for (int i=0;i<StringAproxComparator.IDENTICAL;i++){
					if (i==0)
						strenght[i]=true;
					else
						strenght[i]=false;
				}
			}
			join.setComparatorStrenght(strenght);
			join.setConformityLimit(xattribs.getDouble(XML_CONFORMITY_ATTRIBUTE,DEFAULT_CONFORMITY_LIMIT));
			join.setTransformationParameters(xattribs.attributes2Properties(
	                new String[]{XML_TRANSFORM_CLASS_ATTRIBUTE}));
			join.setTransformationParametersForSuspicious(xattribs.attributes2Properties(
	                new String[]{XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE}));
			
			return join;
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}
 
	private void setLibraryPath(String libraryPath) {
		this.libraryPath = libraryPath;
	}

	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}

	private void setConformityLimit(double conformityLimit) {
		this.conformityLimit = conformityLimit;
	}

	private void setTransformClassNameForSuspicious(
			String transformClassNameForSuspicious) {
		this.transformClassNameForSuspicious = transformClassNameForSuspicious;
	}

	private void setDynamicTransformationForSuspicious(
			DynamicJavaCode dynamicTransformationForSuspicious) {
		this.dynamicTransformationForSuspicious = dynamicTransformationForSuspicious;
	}

	private void setTransformSourceForSuspicious(String transformSourceForSuspicious, boolean distincter) {
		this.transformSourceForSuspicious = transformSourceForSuspicious;
	}

	private void setLibraryPathForSuspicious(String libraryPathForSuspicious) {
		this.libraryPathForSuspicious = libraryPathForSuspicious;
	}
	
}
