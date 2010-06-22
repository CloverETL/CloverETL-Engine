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
package org.jetel.component.partition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.jetel.component.RecordTransformFactory;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.lookup.RangeLookupTable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

/**
 * This factory provides PartitionFunction implementation based on various settings.
 * The factory is used directly from Partition component and PartitionWriter component.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.10.2009
 */
public class PartitionFunctionFactory {
	public static final char EQUAL = '=';
	public static final char COMMA = ',';
	public static final char START_OPENED = '(';
	public static final char START_CLOSED = '<';
	public static final char END_OPENED = ')';
	public static final char END_CLOSED = '>';

	private static final String PORT_NO_FIELD_NAME = "portNo"; 

	public static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+" + PartitionTL.GET_OUTPUT_PORT_FUNCTION_NAME);

	private Node node;

	private DataRecordMetadata metadata;
	
	private String charset;
	
	private Properties additionalParameters;
	
	private Log logger;
	
	private String[] partitionRanges;
	
	private String[] partitionKeyNames;
	
	private boolean useI18N;
	
	private String locale;

	//runtime variables
	private boolean[] startInclude;
	private boolean[] endInclude;

	public PartitionFunction createPartitionFunction(String partitionSource, String partitionClass, String partitionURL) throws ComponentNotReadyException {
		PartitionFunction partitionFunction;
		
		if (partitionClass != null) {
			partitionFunction = createPartitionFce(partitionClass);
		} else {
			if (partitionURL != null && StringUtils.isEmpty(partitionSource)) {
				partitionSource = FileUtils.getStringFromURL(node.getGraph().getRuntimeContext().getContextURL(), partitionURL, charset);
			}
			if (!StringUtils.isEmpty(partitionSource)) {
				partitionFunction = createPartitionDynamic(partitionSource);
			} else if (partitionRanges != null) {
				//create RangePartition function from partitionKey and partitionRanges
				String rangesData = analyzeRangesString(partitionRanges);
				//create metadata for RangeLookupTable
				DataRecordMetadata lookupMetadata = new DataRecordMetadata("lookup_metadata", DataRecordMetadata.DELIMITED_RECORD);
				lookupMetadata.addField(new DataFieldMetadata(PORT_NO_FIELD_NAME, DataFieldMetadata.INTEGER_FIELD, String.valueOf(EQUAL)));
				DataFieldMetadata keyField, startField, endField;
				String[] startFields = new String[partitionKeyNames.length];
				String[] endFields = new String[partitionKeyNames.length];
				//create startField and endField for each key field
				for (int i = 0; i < partitionKeyNames.length; i++) {
					keyField = metadata.getField(partitionKeyNames[i]);
					startField = keyField.duplicate();
					startField.setName(keyField.getName() + "_start");
					startField.setDelimiter(String.valueOf(COMMA));
					startFields[i] = startField.getName(); 
					endField = keyField.duplicate();
					endField.setName(keyField.getName() + "_end");
					endField.setDelimiter(i == partitionKeyNames.length -1 ? 
							Defaults.Component.KEY_FIELDS_DELIMITER : String.valueOf(COMMA));
					endFields[i] = endField.getName();
					lookupMetadata.addField(startField);
					lookupMetadata.addField(endField);
				}
				//create RangeLookupTable 
				RangeLookupTable table = new RangeLookupTable(node.getId() + "_lookupTable", 
						lookupMetadata, startFields, endFields, new DelimitedDataParser());
				table.setUseI18N(useI18N);
				table.setLocale(locale);
				table.setData(rangesData);
				table.setStartInclude(startInclude);
				table.setEndInclude(endInclude);
				int rejectedPort = node.getOutPorts().size();
				partitionFunction = new RangePartition(table,
						lookupMetadata.getFieldPosition(PORT_NO_FIELD_NAME), 
						partitionRanges.length < rejectedPort ? rejectedPort -1 : RangePartition.NONEXISTENT_REJECTED_PORT);
			} else if (partitionKeyNames != null) {
				partitionFunction = new HashPartition();
			} else {
				partitionFunction = new RoundRobinPartition();
			}
		}

		return partitionFunction;
	}
	
	/**
	 * Creates partition function from given class name.
	 * 
	 * @param partitionClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private static PartitionFunction createPartitionFce(String partitionClass) throws ComponentNotReadyException{
		PartitionFunction result;
		
        try {
         	result = (PartitionFunction) Class.forName(partitionClass).newInstance();
        } catch (InstantiationException ex) {
        	throw new ComponentNotReadyException("Can't instantiate partition function class: " + ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new ComponentNotReadyException("Can't instantiate partition function class: " + ex.getMessage());
        } catch (ClassNotFoundException ex) {
            throw new ComponentNotReadyException("Can't find specified partition function class: " + partitionClass);
        }
        
        return result;
	}

	/**
	 * Method for creation partition function from CloverETL language or java source
	 * 
	 * @param partitionCode source code
	 * @return Partition function
	 * @throws ComponentNotReadyException
	 */
	public PartitionFunction createPartitionDynamic(String partitionSource) throws ComponentNotReadyException {
		int transformType = RecordTransformFactory.guessTransformType(partitionSource);

		//check if source code is in CloverETL format
		if (transformType == RecordTransformFactory.TRANSFORM_CLOVER_TL) {
			PartitionTL function =  new PartitionTL(partitionSource, metadata, additionalParameters, logger);
			function.setNode(node);
			return function;
		} else if (transformType == RecordTransformFactory.TRANSFORM_CTL) {
			// compile the CTL code
			ITLCompiler compiler = TLCompilerFactory.createCompiler(
					node.getGraph(),
					new DataRecordMetadata[] { metadata },
					new DataRecordMetadata[] { metadata },
					"UTF-8");
        	List<ErrorMessage> msgs = compiler.compile(partitionSource, CTLRecordPartition.class, node.getId());
        	
        	if (compiler.errorCount() > 0) {
        		for (ErrorMessage msg : msgs) {
        			logger.error(msg.toString());
        		}
        		throw new ComponentNotReadyException("CTL code compilation finished with " + compiler.errorCount() + " errors.");
        	}
        	
        	Object ret = compiler.getCompiledCode();
        	PartitionFunction function = null;
        	if (ret instanceof TransformLangExecutor) {
        		// setup interpreted runtime
        		function = new CTLRecordPartitionAdapter((TransformLangExecutor) ret, logger);
        	} else if (ret instanceof CTLRecordPartition) {
        		function = (CTLRecordPartition) ret;
        	} else {
        		// this should never happen as compiler always generates correct interface
        		throw new ComponentNotReadyException("Invalid type of record transformation.");
        	}
        	// pass graph instance to transformation (if CTL it can use lookups etc.)
			function.setNode(node);
			return function;
		} else if (transformType == RecordTransformFactory.TRANSFORM_JAVA_SOURCE) {
			//get partition function form java code
	        Object transObject = DynamicJavaClass.instantiate(partitionSource, this.getClass().getClassLoader(),
	        		node.getGraph().getRuntimeContext().getClassPathsUrls());

	        if (transObject instanceof PartitionFunction) {
				return (PartitionFunction) transObject;
	        }

	        throw new ComponentNotReadyException("Provided partition class doesn't implement required interface.");
		}

		throw new ComponentNotReadyException("Cannot determine the type of the transformation code!");
    }

	/**
	 * @param ranges ranges defined by user
	 * @return (0=start00,end00,start01,end01,...,start0n,end0n;1=start10,end10,start11,end11,...;m=startmn,endmn;)
	 */
	private String analyzeRangesString(String[] ranges) throws ComponentNotReadyException {
		StringBuilder rangesData = new StringBuilder();
		int intervalNumbers = ranges[0].split("[(<]").length - 1;
		if (intervalNumbers == 0) {//old reprezentation: x1;x2;..;xn
			//corresponding new form: (,x1>;(x1,x2>;(x2,.....,xn>;(xn,>
			String[] newRanges = new String[ranges.length + 1];
			String start = "";
			String end = ranges[0];
			for (int i = 0; i < newRanges.length; i++) {
				newRanges[i] = START_OPENED + start + COMMA + end + END_CLOSED;
				start = end;
				end = i+1 < newRanges.length - 1 ? ranges[i+1] : "";
			}
			intervalNumbers = 1;
			ranges = newRanges;
		}
		//prepare default start/endInclude
		startInclude = new boolean[intervalNumbers];
		endInclude = new boolean[intervalNumbers];
		Arrays.fill(startInclude, true);
		Arrays.fill(endInclude, false);
		
		int intervalNumber;
		char c;
		//remove brackets, set requested start/endInclude
		for (int i=0; i < ranges.length; i++){
			intervalNumber = 0;
			ranges[i] = ranges[i].trim();
			rangesData.append(i);
			rangesData.append(EQUAL);
			for (int index = 0; index < ranges[i].length(); index++){
				c = ranges[i].charAt(index);
				switch (c) {
				case START_OPENED:
					if (i == 0) {//first '(' - set startInclude to "false"
						startInclude[intervalNumber] = false;
					}else if (startInclude[intervalNumber]) {//there was "<" for this interval before
						logger.warn("startInclude[" + intervalNumber + "] was set to \"true\" before");
					}
					if (intervalNumber != 0){
						rangesData.append(COMMA);
					}
					break;
				case START_CLOSED:
					if (!startInclude[intervalNumber]) {//there was "(" for this interval before
						logger.warn("startInclude[" + intervalNumber + "] was set to \"false\" before");
					}
					if (intervalNumber != 0){
						rangesData.append(COMMA);
					}
					break;
				case END_OPENED:
					if (endInclude[intervalNumber]) {//there was ">" for this interval before
						logger.warn("endInclude[" + intervalNumber + "] was set to \"true\" before");
					}
					intervalNumber++;
					break;
				case END_CLOSED:
					if (i == 0) {//first '>' - set endInclude to "true"
						endInclude[intervalNumber] = true;
					}else if (!endInclude[intervalNumber]) {//there was ")" for this interval before
						logger.warn("endInclude[" + intervalNumber + "] was set to \"false\" before");
					}
					intervalNumber++;
					break;
				default:
					rangesData.append(c);
					break;
				}
			}
			rangesData.append(Defaults.Component.KEY_FIELDS_DELIMITER);
		}
		
		return rangesData.toString();
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setAdditionalParameters(Properties additionalParameters) {
		this.additionalParameters = additionalParameters;
	}

	public void setLogger(Log logger) {
		this.logger = logger;
	}

	public void setPartitionRanges(String[] partitionRanges) {
		this.partitionRanges = partitionRanges;
	}

	public void setPartitionKeyNames(String[] partitionKeyNames) {
		this.partitionKeyNames = partitionKeyNames;
	}

	public void setUseI18N(boolean useI18N) {
		this.useI18N = useI18N;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

}
