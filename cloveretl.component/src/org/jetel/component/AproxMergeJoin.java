/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-05  Javlin Consulting <info@javlinconsulting.cz>
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.FileRecordBuffer;
import org.jetel.data.RecordKey;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringAproxComparator;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 * <h3>Approximate Merge Join Component</h3>
 *
 * <!-- Joins records on port 0 (Master records) with records on port 1 (Slave records).
 * Joining is based on specified key fields and their similarity - with defined
 * maximum disimilarity/distance - computed as edit distance - "Levenshtein distance"-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td> <td>Approximate Merge Join</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Joins records on port 0 (Master records) with records on port 1 (Slave records).
 * Joining is based on specified key fields and their similarity - with defined
 * maximum dissimilarity/distance - computed as edit distance - "Levenshtein distance"</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td> [0] - driver records<br>
 *    [1] - slave records<br></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - joined records with conformity greater then given limit. If in output metadada 
 * there are fields called "_total_conformity_" or "_keyName_konformity_", where keyName is 
 * one of field's names from joinKey parameter, to these fields will be assigned computed 
 * conformity. These fields have to be of type "numeric"<br>
 *        [1] - joined records with conformity smaller then given limit but with the same 
 *        matching key. Output metadata can have additional fields (see above)<br>
 *        [2] (optional) - driver records for which there is not matching slave  
 *        [3] (optional) - slave records for which there is not matching driver<br></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>APROX_MERGE_JOIN</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>joinKey</b></td><td>field names with number of letters, weight 
 *  	and strength of comparison as four boolean values for each comparison 
 *  	level separated by :;|  {colon, semicolon, pipe}. General form:
 *  fieldName maxNumberOfLetterstoChange fieldWeight identicalComparison tertiaryComparison 
 *  secondaryComparison primaryComparison<br>Conformity between two strings equals 
 *  zero if transformation of one string to another necessitates more then maxNumberOfLetterstoChange
 *  letters to change.<br>Wheight of each field diffrence is counted due to following 
 *  algorithm: weight given by user divided by sum of weights given by user.<br>
 *  Comparison strength:<br>
 *  identical - letters equals if they are identical<br>
 *  tertiary - comparison does not depend on upper or lower case<br>
 *  secondary - diacritic letters and theirs latin equivalents are equals<br>
 *  primary - letters with additional features (e.g.:penduncle, pick, circle) and
 *   theirs latin equivalents are equals
 *  <tr><td><b>slaveOverrideKey</b><br><i>optional</i></td><td>can be used 
 *  	to specify different key field names for records on slave input; 
 *  	field names separated by :;| {colon, semicolon, pipe}</td>
 *  <tr><td><b>matchingKey</b></td><td>field name for comparing driver and 
 *  	slave records</td>
 *  <tr><td><b>slaveMatchingOverride</b><br><i>optional</i></td><td>can be
 *  	 used to specify different key field name for records on slave input</td></tr>
 *  <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class
 *  	 to be used for transforming joined data which has conformity greater
 *  	 then conformity limit.If no class name is specified then it is expected
 *  	 that the transformation Java source code is embedded in XML</td></tr>
 *  <tr><td><b>transformClassForSuspicious</b><br><i>optional</i></td><td>name
 *  	 of the class to be used for transforming joined data which has conformity
 *  	 smaller then conformity limit.If no class name is specified then it
 *  	 is expected that the transformation Java source code is embedded in XML</td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation for
 *  	 joined records which has conformity greater then conformity limit as java code, 
 *  	 in internal clover format or in Transform Language</td>
 *  <tr><td><b>transformForSuspicious</b></td><td>contains definition of transformation
 *  	 for joined records which has conformity smaller then conformity limit
 *  	 as java code, in internal clover format or in Transform Language</td>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code for
 *  	 joined records which has conformity greater then conformity limit</td></tr>
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code for
 *  	 joined records which has conformity smaller then conformity limit</td></tr>
 *  <tr><td><b>charset</b><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>conformity</b><br></td><td>joined records are divided to two 
 *  	sets depending on their computed conformity is greater or smaller then
 *  	 this parameter</td></tr>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="APROX_MERGE_JOIN0" type="APROX_MERGE_JOIN"&gt;
 *  &lt;attr name="matchingKey"&gt;key&lt;/attr&gt;
 *  &lt;attr name="transform"&gt;${out.0.lname} = ${in.0.lname};
 *  ${out.0.fname} = ${in.0.fname};
 *  ${out.0.birthdate} = ${in.0.birthdate};
 *  ${out.0.gender} = ${in.0.gender};
 *  ${out.0.first_name} = ${in.1.first_name};
 *  ${out.0.last_name} = ${in.1.last_name};
 *  ${out.0.birth_date} = ${in.1.birth_date};
 *  ${out.0.gender1} = ${in.1.gender};
 *  &lt;/attr&gt;
 *  &lt;attr name="conformity"&gt;0.8&lt;/attr&gt;
 *  &lt;attr name="slaveOverrideKey"&gt;last_name;first_name&lt;/attr&gt;
 *  &lt;attr name="joinKey"&gt;lname 2 0.75 false true true true;fname 4 0.25 false false false true&lt;/attr&gt;
 *  &lt;attr name="transformForSuspicious"&gt;${out.1.lname} = ${in.0.lname};
 *  ${out.1.fname} = ${in.0.fname};
 *  ${out.1.first_name} = ${in.1.first_name};
 *  ${out.1.last_name} = ${in.1.last_name};
 *  &lt;/attr&gt;
 *  &lt;/Node &gt;
 *  </pre>
 *
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *	www.javlinconsulting.cz
 *	@created October 10, 2006
 *
 */
public class AproxMergeJoin extends Node {

	private static final String XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE = "slaveOverrideKey";
	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";
	private static final String XML_MATCHING_KEY_ATTRIBUTE="matchingKey";
	private static final String XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE = "slaveMatchingOverride";
	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE = "transformClassForSuspicious";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE = "transformForSuspicious";
	private static final String XML_TRANSFORM_URL_ATTRIBUTE = "transformURL";
	private static final String XML_TRANSFORM_URL_FOR_SUSPICIOUS_ATTRIBUTE = "transformURLForSuspicious";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_CONFORMITY_ATTRIBUTE = "conformity";
	
	public final static String COMPONENT_TYPE = "APROX_MERGE_JOIN";

	//Definition of input and output ports
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
	private String transformSource = null;
	private String transformClassNameForSuspicious;
	private String transformSourceForSuspicious = null;
	private String transformURL = null;
	private String transformURLForsuspicious = null;
	private String charset = null;

	private RecordTransform transformation = null;
	private RecordTransform transformationForSuspicious = null;

	private String[] joinParameters;
	private String[] joinKeys;
	private String[] slaveOverrideKeys = null;
	private String[] matchingKey=new String[1];
	private String[] slaveMatchingKey=null;
	private RecordKey[] recordKey;
	private int[] conformityFieldsForConforming;
	private int[] conformityFieldsForSuspicious;
	private int[][] fieldsToCompare=new int[2][];
	private double[] weights;
	
	private StringAproxComparator[] comparator;
	private int[] maxDifferenceLetters;

	private double conformityLimit;
	
	private ByteBuffer dataBuffer;
	private FileRecordBuffer recordBuffer;

	// for passing data records into transform function
	private final static DataRecord[] inRecords = new DataRecord[2];
	private DataRecord[] outConformingRecords=new DataRecord[1];
	private DataRecord[] outSuspiciousRecords = new DataRecord[1];

	private Properties transformationParameters;
	private Properties transformationParametersForSuspicious;
	
	static Log logger = LogFactory.getLog(MergeJoin.class);

	/**
	 * @param id
	 */
	
	public AproxMergeJoin(String id,String[] joinParameters, String matchingKey,
			String transform, String transformClass, String transformForSusp, 
			String transformClassForSus, String transformURL, String transformURLforSus) throws JetelException{
		super(id);
		this.joinParameters=joinParameters;
		this.matchingKey[0]=matchingKey;
		this.transformSource = transform;
		this.transformClassName = transformClass;
		this.transformSourceForSuspicious = transformForSusp;
		this.transformClassNameForSuspicious = transformClassForSus;
		this.transformURL = transformURL;
		this.transformURLForsuspicious = transformURLforSus;
	}
	
	public AproxMergeJoin(String id,String[] joinParameters, String matchingKey,
			RecordTransform transform, RecordTransform transformForSusp) 
				throws JetelException{
		this(id, joinParameters, matchingKey, null, null, null, null, null, null);
		this.transformation = transform;
		this.transformationForSuspicious = transformForSusp;
	}

	/**
	 *  Sets specific key (string) for slave records<br>
	 *  Can be used if slave record has different names
	 *  for fields composing the key
	 *
	 * @param  slaveKeys  The new slaveOverrideKey value
	 */
	private void setSlaveOverrideKey(String[] slaveKeys) {
		this.slaveOverrideKeys = slaveKeys;
	}

	/**
	 *  Sets specific key (string) for slave records<br>
	 *  Can be used if slave record has different name
	 *  for matching key field
	 *
	 * @param slaveMatchingKey
	 */
	private void setSlaveMatchingKey(String slaveMatchingKey){
		this.slaveMatchingKey=new String[1];
		this.slaveMatchingKey[0]=slaveMatchingKey;
	}
	
	/**
	 *  Populates record buffer with all slave records having the same matching key
	 *
	 * @param  inPort                      Description of the Parameter
	 * @param  nextRecord                next record from slave
	 * @param  key                       Description of the Parameter
	 * @param  currRecord                Description of the Parameter
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 * @exception  JetelException        Description of the Exception
	 */
	private void fillRecordBuffer(InputPort inPort, DataRecord[] records, RecordKey key)
			 throws IOException, InterruptedException, JetelException {

		recordBuffer.clear();
		if (records[CURRENT] != null) {
			dataBuffer.clear();
			records[CURRENT].serialize(dataBuffer);
			dataBuffer.flip();
			recordBuffer.push(dataBuffer);
			while (records[TEMPORARY] != null) {
				records[TEMPORARY] = inPort.readRecord(records[TEMPORARY]);
				if (records[TEMPORARY] != null) {
					switch (key.compare(records[CURRENT], records[TEMPORARY])) {
						case 0:
							dataBuffer.clear();
							records[TEMPORARY].serialize(dataBuffer);
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
	 * @param  outSlave                  Description of the Parameter
	 * @param  key                       Description of the Parameter
	 * @return                           The correspondingRecord value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 */
	private int getCorrespondingRecord(DataRecord driver, DataRecord[] slave, 
			InputPort slavePort, OutputPort outSlave, RecordKey[] key)
			 throws IOException, InterruptedException {

		while (slave[CURRENT] != null) {
			switch (key[DRIVER_ON_PORT].compare(key[SLAVE_ON_PORT], driver, slave[CURRENT])) {
				case 1:
					if (outSlave != null) {
						outSlave.writeRecord(slave[CURRENT]);
					}					
					slave[CURRENT] = slavePort.readRecord(slave[CURRENT]);
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

	/**
	 * Calculates difference on given fields between two records 
	 * 
	 * @param record1 - driver record
	 * @param record2 - slave record
	 * @param fieldsToCompare 
	 * @return difference between two records from interval <0,1>
	 */
	private double[] conformity(DataRecord record1,DataRecord record2,int[][] fieldsToCompare){
		double[] result=new double[fieldsToCompare[DRIVER_ON_PORT].length+1];
		double totalResult=0;
		int max=0;
		for (int i=0;i<fieldsToCompare[DRIVER_ON_PORT].length;i++){
			comparator[i].setMaxLettersToChange(maxDifferenceLetters[i]);
			max=(maxDifferenceLetters[i]+1)*comparator[i].getMaxCostForOneLetter();
			int distance=comparator[i].distance(
					record1.getField(fieldsToCompare[DRIVER_ON_PORT][i]).getValue().toString(),
					record2.getField(fieldsToCompare[SLAVE_ON_PORT][i]).getValue().toString());
			result[i+1]=1-(double)distance/(double)max;
			totalResult+=result[i+1]*weights[i];
		}
		result[0] = totalResult;
		return result;
	}
	
	/**
	 *  Outputs all combinations of current driver record and all slaves 
	 *  with the same key. When conformity between two records is greater 
	 *  then conformityLimit proper combination is sent to output port 
	 *  CONFORMING_OUT, else another combination is sent to output port
	 *  SUSPICIOUS_OUT
	 *
	 * @param  driver                    Description of the Parameter
	 * @param  slave                     Description of the Parameter
	 * @param  outComforming             Description of the Parameter
	 * @param  outSuspicious             Description of the Parameter
	 * @param  conformingPort                Description of the Parameter
	 * @param  suspiciousPort                Description of the Parameter
	 * @return                           Description of the Return Value
	 * @exception  IOException           Description of the Exception
	 * @exception  InterruptedException  Description of the Exception
	 * @throws TransformException 
	 */
	private boolean flushCombinations(DataRecord driver, DataRecord slave, 
			DataRecord outConforming,DataRecord outSuspicious, OutputPort conformingPort, 
			OutputPort suspiciousPort) throws IOException, InterruptedException, TransformException {
		recordBuffer.rewind();
		dataBuffer.clear();
		inRecords[0] = driver;
		inRecords[1] = slave;
		outConformingRecords[0] = outConforming;
		outSuspiciousRecords[0] = outSuspicious;

		while (recordBuffer.shift(dataBuffer) != null) {
			dataBuffer.flip();
			slave.deserialize(dataBuffer);
			double[] conformity=conformity(driver,slave,fieldsToCompare);
			if (conformity[0]>=conformityLimit) {
				// **** call transform function here ****
				if (!transformation.transform(inRecords, outConformingRecords)) {
					logger.warn(transformation.getMessage());
					return false;
				}
				//fill aditional fields
				if (conformityFieldsForConforming.length>0){
					for (int i=0;i<conformityFieldsForConforming.length;i++){
						if (conformityFieldsForConforming[i]>-1){
							((Numeric)outConforming.getField(conformityFieldsForConforming[i])).setValue(conformity[i]);
						}
					}
				}
				conformingPort.writeRecord(outConforming);
			}else{
				// **** call transform function here ****
				if (!transformationForSuspicious.transform(inRecords,outSuspiciousRecords)){
					logger.warn(transformation.getMessage());
					return false;
				}
				//fill aditional fields
				if (conformityFieldsForSuspicious.length>0){
					for (int i=0;i<conformityFieldsForSuspicious.length;i++){
						if (conformityFieldsForSuspicious[i]>-1){
							outSuspicious.getField(conformityFieldsForSuspicious[i]).setValue(new Double(conformity[i]));
						}
					}
				}
				suspiciousPort.writeRecord(outSuspicious);
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

	@Override
	public Result execute() throws Exception {
		boolean isDriverDifferent;

		// get all ports involved
		InputPort driverPort = getInputPort(DRIVER_ON_PORT);
		InputPort slavePort = getInputPort(SLAVE_ON_PORT);
		OutputPort conformingPort = getOutputPort(CONFORMING_OUT);
		OutputPort suspiciousPort = getOutputPort(SUSPICIOUS_OUT);
		OutputPort notMatchDriverPort = getOutputPort(NOT_MATCH_DRIVER_OUT);
		OutputPort notMatchSlavePort = getOutputPort(NOT_MATCH_SLAVE_OUT);

		// initialize input records driver & slave
		DataRecord[] driverRecords = allocateRecords(driverPort.getMetadata(),
				2);
		DataRecord[] slaveRecords = allocateRecords(slavePort.getMetadata(), 2);

		// initialize output record
		DataRecordMetadata outConformingMetadata = conformingPort.getMetadata();
		DataRecord outConformingRecord = new DataRecord(outConformingMetadata);
		outConformingRecord.init();
		outConformingRecord.reset();

		DataRecordMetadata outSuspiciousMetadata = suspiciousPort.getMetadata();
		DataRecord outSuspiciousRecord = new DataRecord(outSuspiciousMetadata);
		outSuspiciousRecord.init();
		outSuspiciousRecord.reset();

		// tmp record for switching contents
		DataRecord tmpRec;
		// create file buffer for slave records - system TEMP path
		recordBuffer = new FileRecordBuffer(null);

		// for the first time (as initialization), we expect that records are
		// different
		isDriverDifferent = true;
		// first initial load of records
		driverRecords[CURRENT] = driverPort.readRecord(driverRecords[CURRENT]);
		slaveRecords[CURRENT] = slavePort.readRecord(slaveRecords[CURRENT]);
		while (runIt && driverRecords[CURRENT] != null) {
			if (isDriverDifferent) {
				switch (getCorrespondingRecord(driverRecords[CURRENT],
						slaveRecords, slavePort, notMatchSlavePort, recordKey)) {
				case -1:
					// driver lower
					// no corresponding slave
					if (notMatchDriverPort != null) {
						notMatchDriverPort.writeRecord(driverRecords[CURRENT]);
					}					
						driverRecords[CURRENT] = driverPort
							.readRecord(driverRecords[CURRENT]);
					isDriverDifferent = true;
					continue;
				case 0:
					// match
					fillRecordBuffer(slavePort, slaveRecords,
							recordKey[SLAVE_ON_PORT]);
					// switch temporary --> current
					tmpRec = slaveRecords[CURRENT];
					slaveRecords[CURRENT] = slaveRecords[TEMPORARY];
					slaveRecords[TEMPORARY] = tmpRec;
					isDriverDifferent = false;
					break;
				}
			}
			flushCombinations(driverRecords[CURRENT], slaveRecords[TEMPORARY],
					outConformingRecord, outSuspiciousRecord, conformingPort,
					suspiciousPort);
			// get next driver

			driverRecords[TEMPORARY] = driverPort
					.readRecord(driverRecords[TEMPORARY]);
			if (driverRecords[TEMPORARY] != null) {
				// different driver record ??
				switch (recordKey[DRIVER_ON_PORT].compare(
						driverRecords[CURRENT], driverRecords[TEMPORARY])) {
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
		// signal end of records stream to transformation function
		transformation.finished();
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	
	public void init() throws ComponentNotReadyException {
        //Checking metadata on input and on  NOT_MATCH_DRIVER_OUT and NOT_MATCH_SLAVE_OUT outputs
		DataRecordMetadata[] inMetadata = new DataRecordMetadata[2];
		inMetadata[0]=getInputPort(DRIVER_ON_PORT).getMetadata();
		inMetadata[1]=getInputPort(SLAVE_ON_PORT).getMetadata();
		DataRecordMetadata[] outMetadata = new DataRecordMetadata[2];
		outMetadata[0] = getOutputPort(NOT_MATCH_DRIVER_OUT) != null ? 
				getOutputPort(NOT_MATCH_DRIVER_OUT).getMetadata() : null;
		outMetadata[1] = getOutputPort(NOT_MATCH_SLAVE_OUT) != null ?
				getOutputPort(NOT_MATCH_SLAVE_OUT).getMetadata() : null;
		if (outMetadata[0] != null && !outMetadata[0].equals(inMetadata[0])){
			throw new ComponentNotReadyException("Wrong metadata on output port no " + 
					NOT_MATCH_DRIVER_OUT + " (NOT_MATCH_DRIVER_OUT)");
		}
		if (outMetadata[1] != null && !outMetadata[1].equals(inMetadata[1])) {
			throw new ComponentNotReadyException(
					"Wrong metadata on output port no " + NOT_MATCH_SLAVE_OUT + " (NOT_MATCH_SLAVE_OUT)");
		}
		outMetadata = new DataRecordMetadata[] { getOutputPort(CONFORMING_OUT)
				.getMetadata() };
		if (transformation != null) {
			transformation.init(transformationParameters, inMetadata, outMetadata);
		} else {
			transformation = RecordTransformFactory.createTransform(transformSource, transformClassName, 
					transformURL, charset, this, inMetadata, outMetadata, transformationParameters, 
					this.getClass().getClassLoader());
		}
		outMetadata = new DataRecordMetadata[] { getOutputPort(SUSPICIOUS_OUT).getMetadata() };
		if (transformationForSuspicious != null) {
			transformationForSuspicious.init(transformationParametersForSuspicious, 
					inMetadata,	outMetadata);
		} else {
			transformationForSuspicious = RecordTransformFactory.createTransform(transformSourceForSuspicious, 
					transformClassNameForSuspicious, transformURLForsuspicious, charset, this, 
					inMetadata, outMetadata, transformationParametersForSuspicious, this.getClass().getClassLoader());
		}
		// initializing join parameters
		joinKeys = new String[joinParameters.length];
		maxDifferenceLetters = new int[joinParameters.length];
		boolean[][] strength=new boolean[joinParameters.length][StringAproxComparator.IDENTICAL];
		weights = new double[joinParameters.length];
		String[] tmp;
		for (int i=0;i<joinParameters.length;i++){
			tmp=joinParameters[i].split(" ");
			joinKeys[i]=tmp[0];
			maxDifferenceLetters[i]=Integer.parseInt(tmp[1]);
			weights[i]=Double.parseDouble(tmp[2]);
			for (int j=0;j<StringAproxComparator.IDENTICAL;j++){
				strength[i][j] = tmp[3+j].equals("true") ? true : false;
			}
		}
		double sumOfWeights=0;
		for (int i=0;i<weights.length;i++){
			sumOfWeights+=weights[i];
		}
		for (int i=0;i<weights.length;i++){
			weights[i]=weights[i]/sumOfWeights;
		}
		if (slaveOverrideKeys == null) {
			slaveOverrideKeys = joinKeys;
		}
		RecordKey[] recKey = new RecordKey[2];
		recKey[DRIVER_ON_PORT] = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata());
		recKey[SLAVE_ON_PORT] = new RecordKey(slaveOverrideKeys, getInputPort(SLAVE_ON_PORT).getMetadata());
		recKey[DRIVER_ON_PORT].init();
		recKey[SLAVE_ON_PORT].init();
		fieldsToCompare[DRIVER_ON_PORT]=recKey[DRIVER_ON_PORT].getKeyFields();
		fieldsToCompare[SLAVE_ON_PORT]=recKey[SLAVE_ON_PORT].getKeyFields();
		comparator = new StringAproxComparator[joinParameters.length];
		for (int i=0;i<comparator.length;i++){
			String locale=inMetadata[DRIVER_ON_PORT].getField(fieldsToCompare[DRIVER_ON_PORT][i]).getLocaleStr();
			try {
				comparator[i] = StringAproxComparator.createComparator(locale,strength[i]);
			}catch(JetelException ex){
				throw new ComponentNotReadyException(ex.getLocalizedMessage());
			}
		}
		if (slaveMatchingKey == null){
			slaveMatchingKey=matchingKey;
		}
		recordKey = new RecordKey[2];
		recordKey[DRIVER_ON_PORT] = new RecordKey(matchingKey, getInputPort(DRIVER_ON_PORT).getMetadata());
		recordKey[SLAVE_ON_PORT] = new RecordKey(slaveMatchingKey, getInputPort(SLAVE_ON_PORT).getMetadata());
		recordKey[DRIVER_ON_PORT].init();
		recordKey[SLAVE_ON_PORT].init();
		conformityFieldsForConforming = findOutFields(joinKeys,getOutputPort(CONFORMING_OUT).getMetadata());
		conformityFieldsForSuspicious = findOutFields(slaveOverrideKeys,getOutputPort(SUSPICIOUS_OUT).getMetadata());
		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
	}
	
	/**
	 * This method finds out fields in outMetadata which are to be filled
	 *  by computed conformity
	 * 
	 * @param names of fields in input metadata for which there are 
	 * 		computed conformities
	 * @param metadata
	 * @return numbers of fields where are to be conformities. When 
	 * 		such field there is not in out metadata on corresponding position 
	 * 		there is -1. First number in array is field number for _total_conformity
	 */
	private int[] findOutFields(String[] names,DataRecordMetadata metadata){
		String[] outKeyNames=new String[names.length+1];
		outKeyNames[0] = "_total_conformity_";
		for (int i=1;i<names.length+1;i++){
			outKeyNames[i] = "_"+joinKeys[i-1]+"_conformity_";
		}
		int[] outKey = new int[outKeyNames.length];
		DataFieldMetadata field;
		for (int i=0;i<outKeyNames.length;i++){
			field = metadata.getField(outKeyNames[i]);
			if (field!=null && field.isNumeric()){
				outKey[i] = metadata.getFieldPosition(outKeyNames[i]);
			}else{
				outKey[i] = -1;
			}
		}
		return outKey;
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

    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		AproxMergeJoin join;

		try {
            join = new AproxMergeJoin(
                    xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                    xattribs.getString(XML_MATCHING_KEY_ATTRIBUTE),
                    xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                    xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE, null),
                    xattribs.getString(XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE,null),
                    xattribs.getString(XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE,null),
                    xattribs.getString(XML_TRANSFORM_URL_ATTRIBUTE, null),
                    xattribs.getString(XML_TRANSFORM_URL_FOR_SUSPICIOUS_ATTRIBUTE, null));
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
            	join.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }
			if (xattribs.exists(XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE)) {
				join.setSlaveOverrideKey(xattribs.getString(XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE).
						split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

			}
			if (xattribs.exists(XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE)) {
				join.setSlaveMatchingKey(xattribs.getString(XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE));
			}
			join.setConformityLimit(xattribs.getDouble(XML_CONFORMITY_ATTRIBUTE,DEFAULT_CONFORMITY_LIMIT));
			join.setTransformationParameters(xattribs.attributes2Properties(
	                new String[]{XML_ID_ATTRIBUTE,XML_JOIN_KEY_ATTRIBUTE,
	                		XML_MATCHING_KEY_ATTRIBUTE,XML_TRANSFORM_ATTRIBUTE,
	                		XML_TRANSFORM_CLASS_ATTRIBUTE,XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE,
	                		XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE,XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE,
	                		XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE,XML_CONFORMITY_ATTRIBUTE}));
			join.setTransformationParametersForSuspicious(xattribs.attributes2Properties(
	                new String[]{XML_ID_ATTRIBUTE,XML_JOIN_KEY_ATTRIBUTE,
	                		XML_MATCHING_KEY_ATTRIBUTE,XML_TRANSFORM_ATTRIBUTE,
	                		XML_TRANSFORM_CLASS_ATTRIBUTE,XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE,
	                		XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE,XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE,
	                		XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE,XML_CONFORMITY_ATTRIBUTE}));
			
			return join;
        }catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
	}
 
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (joinKeys != null) {
			StringBuffer buf = new StringBuffer(joinKeys[0]);
			for (int i=1; i< joinKeys.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + joinKeys[i]); 
			}
			xmlElement.setAttribute(XML_JOIN_KEY_ATTRIBUTE,buf.toString());
		}
		
		if (slaveOverrideKeys!= null) {
			StringBuffer buf = new StringBuffer(slaveOverrideKeys[0]);
			for (int i=1; i< slaveOverrideKeys.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + slaveOverrideKeys[i]); 
			}
			xmlElement.setAttribute(XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE,buf.toString());
		}
		
		if (matchingKey!=null){
			xmlElement.setAttribute(XML_MATCHING_KEY_ATTRIBUTE,matchingKey[0]);
		}
		
		if (slaveMatchingKey!=null){
			xmlElement.setAttribute(XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE,slaveMatchingKey[0]);
		}
		
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORM_CLASS_ATTRIBUTE,transformClassName);
		} 
		
		if (transformClassNameForSuspicious != null){
			xmlElement.setAttribute(XML_TRANSFORM_CLASS_FOR_SUSPICIOUS_ATTRIBUTE, transformClassNameForSuspicious);
		}
		
		if (transformSource!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,transformSource);
		}
		
		if (transformSourceForSuspicious != null){
			xmlElement.setAttribute(XML_TRANSFORM_FOR_SUSPICIOUS_ATTRIBUTE, transformSourceForSuspicious);
		}
		
		if (transformURL != null) {
			xmlElement.setAttribute(XML_TRANSFORM_URL_ATTRIBUTE, transformURL);
		}
		
		if (transformURLForsuspicious != null ){
			xmlElement.setAttribute(XML_TRANSFORM_URL_FOR_SUSPICIOUS_ATTRIBUTE, transformURLForsuspicious);
		}
		
		if (charset != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}
		
		xmlElement.setAttribute(XML_CONFORMITY_ATTRIBUTE,String.valueOf(conformityLimit));
        
		if (transformationParameters != null) {
			Enumeration propertyAtts = transformationParameters.propertyNames();
			while (propertyAtts.hasMoreElements()) {
				String attName = (String)propertyAtts.nextElement();
				xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
			}
		}
		
	}
    
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 2, 2);
        checkOutputPorts(status, 2, 4);
		if (getOutputPort(NOT_MATCH_DRIVER_OUT) != null) {
			checkMetadata(status, getInputPort(DRIVER_ON_PORT).getMetadata(),
					getOutputPort(NOT_MATCH_DRIVER_OUT).getMetadata());
		}		
		if (getOutputPort(NOT_MATCH_SLAVE_OUT) != null) {
			checkMetadata(status, getInputPort(SLAVE_ON_PORT).getMetadata(),
					getOutputPort(NOT_MATCH_SLAVE_OUT).getMetadata());
		}		
		try {
        	
    		DataRecordMetadata[] inMetadata = new DataRecordMetadata[2];
    		inMetadata[0]=getInputPort(DRIVER_ON_PORT).getMetadata();
    		inMetadata[1]=getInputPort(SLAVE_ON_PORT).getMetadata();
    		// initializing join parameters
    		joinKeys = new String[joinParameters.length];
    		maxDifferenceLetters = new int[joinParameters.length];
    		boolean[][] strength=new boolean[joinParameters.length][StringAproxComparator.IDENTICAL];
    		weights = new double[joinParameters.length];
    		String[] tmp;
    		for (int i=0;i<joinParameters.length;i++){
    			tmp=joinParameters[i].split(" ");
    			joinKeys[i]=tmp[0];
    			maxDifferenceLetters[i]=Integer.parseInt(tmp[1]);
    			weights[i]=Double.parseDouble(tmp[2]);
    			for (int j=0;j<StringAproxComparator.IDENTICAL;j++){
    				strength[i][j] = tmp[3+j].equals("true") ? true : false;
    			}
    		}
    		double sumOfWeights=0;
    		for (int i=0;i<weights.length;i++){
    			sumOfWeights+=weights[i];
    		}
    		for (int i=0;i<weights.length;i++){
    			weights[i]=weights[i]/sumOfWeights;
    		}
    		if (slaveOverrideKeys == null) {
    			slaveOverrideKeys = joinKeys;
    		}
    		RecordKey[] recKey = new RecordKey[2];
    		recKey[DRIVER_ON_PORT] = new RecordKey(joinKeys, getInputPort(DRIVER_ON_PORT).getMetadata());
    		recKey[SLAVE_ON_PORT] = new RecordKey(slaveOverrideKeys, getInputPort(SLAVE_ON_PORT).getMetadata());
    		RecordKey.checkKeys(recKey[DRIVER_ON_PORT], XML_JOIN_KEY_ATTRIBUTE, 
    				recKey[SLAVE_ON_PORT], XML_SLAVE_OVERRRIDE_KEY_ATTRIBUTE, status, this);
    		
    		fieldsToCompare[DRIVER_ON_PORT]=recKey[DRIVER_ON_PORT].getKeyFields();
    		fieldsToCompare[SLAVE_ON_PORT]=recKey[SLAVE_ON_PORT].getKeyFields();
    		comparator = new StringAproxComparator[joinParameters.length];
    		for (int i=0;i<comparator.length;i++){
    			String locale=inMetadata[DRIVER_ON_PORT].getField(fieldsToCompare[DRIVER_ON_PORT][i]).getLocaleStr();
    			try {
    				comparator[i] = StringAproxComparator.createComparator(locale,strength[i]);
    			}catch(JetelException ex){
    				throw new ComponentNotReadyException(ex.getLocalizedMessage());
    			}
    		}
    		if (slaveMatchingKey == null){
    			slaveMatchingKey=matchingKey;
    		}
    		recordKey = new RecordKey[2];
    		recordKey[DRIVER_ON_PORT] = new RecordKey(matchingKey, getInputPort(DRIVER_ON_PORT).getMetadata());
    		recordKey[SLAVE_ON_PORT] = new RecordKey(slaveMatchingKey, getInputPort(SLAVE_ON_PORT).getMetadata());
       		RecordKey.checkKeys(recordKey[DRIVER_ON_PORT], XML_MATCHING_KEY_ATTRIBUTE, 
    				recordKey[SLAVE_ON_PORT], XML_SLAVE_MATCHING_OVERRIDE_ATTRIBUTE, status, this);
     		
    		conformityFieldsForConforming = findOutFields(joinKeys,getOutputPort(CONFORMING_OUT).getMetadata());
    		conformityFieldsForSuspicious = findOutFields(slaveOverrideKeys,getOutputPort(SUSPICIOUS_OUT).getMetadata());
    		dataBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
        	
//            init();
//            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
    }
	
	public String getType(){
		return COMPONENT_TYPE;
	}

	private void setConformityLimit(double conformityLimit) {
		this.conformityLimit = conformityLimit;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}
		
}
