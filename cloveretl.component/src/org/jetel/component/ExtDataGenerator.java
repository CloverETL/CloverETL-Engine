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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.graph.Result;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>Data Generator Component</h3> <!-- Generates new records. -->
 * 
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu) (c) Javlin, a.s. (www.javlin.eu)
 * 
 */
public class ExtDataGenerator extends DataGenerator {

	static Log logger = LogFactory.getLog(ExtDataGenerator.class);

	// Description of the Field
	public final static String COMPONENT_TYPE = "EXT_DATA_GENERATOR";

	// XML attribute names
	static final String XML_GENERATECLASS_ATTRIBUTE = "generateClass";
	static final String XML_GENERATE_ATTRIBUTE = "generate";
	static final String XML_GENERATEURL_ATTRIBUTE = "generateURL";

	// Input parameters
	private String generatorSource;
	private String generatorClassName;
	private String generatorURL;
	private String charset;
	private long recordsNumber;

	// data generator
	private Properties generateParameters;
	private RecordGenerate generatorClass;

	/**
	 * Constructor.
	 * @param id
	 * @param generate
	 * @param recordsNumber
	 */
	public ExtDataGenerator(String id, RecordGenerate generate, long recordsNumber) {
		super(id);
		this.generatorClass = generate;
		this.recordsNumber = recordsNumber;
	}
	
	/**
	 * @param id
	 * @param pattern
	 * @param recordsNumber
	 */
	public ExtDataGenerator(String id, String generate, String generateClass,
			String generateURL, long recordsNumber) {
		super(id);
		this.generatorSource = generate;
		this.generatorClassName = generateClass;
		this.generatorURL = generateURL;
		this.recordsNumber = recordsNumber;
	}

	public static String getTransformAttributeName() {
		return XML_GENERATE_ATTRIBUTE;
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	
		if (generatorClass != null) {
			generatorClass.preExecute();
		}

    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		autoFilling.reset();
    	}
    }    

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
     */
    @Override
	public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	
		if (generatorClass != null) {
			generatorClass.postExecute();
			generatorClass.finished();
		}
    }
	
	@Override
	public Result execute() throws Exception {
		// initialize output ports
		int numOutputPorts = getOutPorts().size();
		DataRecord outRecord[] = new DataRecord[numOutputPorts];
		for (int i = 0; i < numOutputPorts; i++) {
			outRecord[i] = DataRecordFactory.newRecord(getOutputPort(i).getMetadata());
			outRecord[i].init();
			outRecord[i].reset();
		}
		if (generatorClass != null)
			executeGenerate(outRecord);
		else
			executeAutoFilling(outRecord);

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * Generate all.
	 * 
	 * @param outRecord
	 * @throws Exception
	 */
	private void executeGenerate(DataRecord[] outRecord) throws Exception {
		for (long i = 0; (recordsNumber < 0 || i < recordsNumber) && runIt; i++) {
			for (DataRecord oRecord : outRecord)
				oRecord.reset();
			int transformResult = -1;

			try {
				transformResult = generatorClass.generate(outRecord);
			} catch (Exception exception) {
				transformResult = generatorClass.generateOnError(exception, outRecord);
			}

			if (transformResult == RecordTransform.ALL) {
				for (int outPort = 0; outPort < outRecord.length; outPort++) {
					autoFilling
							.setLastUsedAutoFillingFields(outRecord[outPort]);
					writeRecord(outPort, outRecord[outPort]);
				}
			} else if (transformResult >= 0) {
				autoFilling
						.setLastUsedAutoFillingFields(outRecord[transformResult]);
				writeRecord(transformResult, outRecord[transformResult]);
			} else if (transformResult == RecordTransform.SKIP) {
				// DO NOTHING - skip the record
			} else if (transformResult == RecordTransform.STOP && recordsNumber < 0) {
				break; // successful termination
			} else {
				throw new TransformException(
						"Transformation finished with code: " + transformResult
								+ ". Error message: " + generatorClass.getMessage());
			}

			SynchronizeUtils.cloverYield();
		}
	}

	/**
	 * Generate only autofilling.
	 * 
	 * @param outRecord
	 * @throws Exception
	 */
	private void executeAutoFilling(DataRecord[] outRecord) throws Exception {
		for (long i = 0; i < recordsNumber && runIt; i++) {
			for (int outPort = 0; outPort < outRecord.length; outPort++) {
				autoFilling.setLastUsedAutoFillingFields(outRecord[outPort]);
				writeRecord(outPort, outRecord[outPort]);
			}
			SynchronizeUtils.cloverYield();
		}
	}

	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (generatorSource != null) {
			xmlElement.setAttribute(XML_GENERATE_ATTRIBUTE, generatorSource);
		}
		if (generatorClassName != null) {
			xmlElement.setAttribute(XML_GENERATECLASS_ATTRIBUTE, generatorClassName);
		}
		if (generatorURL != null) {
			xmlElement.setAttribute(XML_GENERATEURL_ATTRIBUTE, generatorURL);
		}
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		verifyAutofilling(getOutMetadataArray()[0]);

		if (generatorClass == null) {
			generatorClass = getTransformFactory().createTransform();
		}

		// set graph instance to transformation (if CTL it can access lookups etc.)
		initGeneratorClass();

		// autofilling
		autoFilling.setFilename(getId());
		autoFilling.addAutoFillingFields(getOutMetadataArray()[0]);
	}

	private TransformFactory<RecordGenerate> getTransformFactory() {
    	TransformFactory<RecordGenerate> transformFactory = TransformFactory.createTransformFactory(RecordGenerateDescriptor.newInstance());
    	transformFactory.setTransform(generatorSource);
    	transformFactory.setTransformClass(generatorClassName);
    	transformFactory.setTransformUrl(generatorURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setOutMetadata(getOutMetadata());
    	return transformFactory;
	}
	
	private void initGeneratorClass() throws ComponentNotReadyException {
		if (!generatorClass.init(generateParameters, getOutMetadataArray())) {
			throw new ComponentNotReadyException("Generator failed to initialize: " + generatorClass.getMessage());
		}
	}
	
	/**
	 * Verifies autofilling.
	 * @throws ComponentNotReadyException
	 */
	private void verifyAutofilling(DataRecordMetadata outMetadata) throws ComponentNotReadyException {

		// if no generator then verify and prepare autofilling
		if (generatorSource == null && generatorClassName == null && generatorURL == null && generatorClass == null) {
			boolean isAutoFilling = false;
			for (DataFieldMetadata fMetadata : outMetadata.getFields()) {
				if (fMetadata.isAutoFilled()) {
					isAutoFilling = true;
				}
			}
			if (!isAutoFilling)
				throw new ComponentNotReadyException("Attribute/property not found: " + XML_GENERATE_ATTRIBUTE);
		}
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 0, 0) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		try {
			verifyAutofilling(getOutMetadata().get(0));

			if (generatorClass == null) {
				getTransformFactory().checkConfig(status);
			}
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);

			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		}
		return status;
	}

	/**
	 * @param generateParameters
	 *            The generationParameters to set.
	 */
	public void setTransformationParameters(Properties generateParameters) {
		this.generateParameters = generateParameters;
	}

	/**
	 * Creates generator instance using given Java source.
	 * @param generatorCode
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private RecordGenerate createGeneratorDynamic(String generatorCode) throws ComponentNotReadyException {
        return DynamicJavaClass.instantiate(generatorCode, RecordGenerate.class, this);
    }

	/**
	 * Sets the charset for URL transform file.
	 * @param charset
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}
}
