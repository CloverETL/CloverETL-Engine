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

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.graph.Result;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.file.FileUtils;
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
	private int recordsNumber;

	// data generator
	private Properties generateParameters;
	private RecordGenerate generatorClass;

	/**
	 * Constructor.
	 * @param id
	 * @param generate
	 * @param recordsNumber
	 */
	public ExtDataGenerator(String id, RecordGenerate generate, int recordsNumber) {
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
			String generateURL, int recordsNumber) {
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
			outRecord[i] = new DataRecord(getOutputPort(i).getMetadata());
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
		for (int i = 0; i < recordsNumber && runIt; i++) {
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
		for (int i = 0; i < recordsNumber && runIt; i++) {
			for (int outPort = 0; outPort < outRecord.length; outPort++) {
				autoFilling.setLastUsedAutoFillingFields(outRecord[outPort]);
				writeRecord(outPort, outRecord[outPort]);
			}
			SynchronizeUtils.cloverYield();
		}
	}

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

	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		DataRecordMetadata[] outMetadata = getOutMetadata().toArray(new DataRecordMetadata[getOutMetadata().size()]);
		verifyAutofilling(outMetadata[0]);
		createGeneratorClass(getGraph().getRuntimeContext(), outMetadata);

		// set graph instance to transformation (if CTL it can access lookups etc.)
		if (generatorClass != null) {
			initGeneratorClass(outMetadata);
		}

		// autofilling
		autoFilling.setFilename(getId());
		autoFilling.addAutoFillingFields(outMetadata[0]);
	}

	private void initGeneratorClass(DataRecordMetadata[] outMetadata) throws ComponentNotReadyException {
        generatorClass.setNode(this);
		
		if (!generatorClass.init(generateParameters, outMetadata)) {
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
	
	/**
	 * Creates generate class (and verifies generator language).
	 * @param watchDog
	 * @throws ComponentNotReadyException
	 */
	private void createGeneratorClass(GraphRuntimeContext runtimeContext, DataRecordMetadata outMetadata[]) throws ComponentNotReadyException {
		if (generatorSource != null || generatorClassName != null || generatorURL != null) {
			// create instance of the record generator
			if (generatorClass == null) {
				
				if (generatorClassName != null) {
					// load class base on its class name
					if (runtimeContext == null) return;

					generatorClass = (RecordGenerate) RecordTransformFactory.loadClass(this.getClass().getClassLoader(),
							generatorClassName, runtimeContext.getClassPath());
				} else if (generatorSource == null) {
					// read source code from URL
					generatorSource = FileUtils.getStringFromURL(getGraph().getRuntimeContext().getContextURL(), generatorURL, charset);
				}
				
				if (generatorClassName == null) {
					switch (RecordTransformFactory.guessTransformType(generatorSource)) {
					case RecordTransformFactory.TRANSFORM_JAVA_SOURCE:
						generatorClass = createGeneratorDynamic(generatorSource);
						break;
					case RecordTransformFactory.TRANSFORM_CLOVER_TL:
						generatorClass = new RecordGenerateTL(generatorSource,logger);
						break;
					case RecordTransformFactory.TRANSFORM_CTL:
						ITLCompiler compiler = TLCompilerFactory.createCompiler(getGraph(),null,outMetadata,"UTF-8");
						List<ErrorMessage> msgs = compiler.compile(generatorSource,CTLRecordGenerate.class, getId());
						if (compiler.errorCount() > 0) {
							String report = ErrorMessage.listToString(msgs, logger);
							throw new ComponentNotReadyException(
									"CTL code compilation finished with "
											+ compiler.errorCount() + " errors" + report);
						}
						Object ret = compiler.getCompiledCode();
						if (ret instanceof TransformLangExecutor) {
							// setup interpreted runtime
							generatorClass = new CTLRecordGenerateAdapter(
									(TransformLangExecutor) ret, logger);
						} else if (ret instanceof CTLRecordGenerate) {
							generatorClass = (CTLRecordGenerate)ret;
						} else {
							// this should never happen as compiler always
							// generates correct interface
							throw new ComponentNotReadyException("Invalid type of record transformation");
						}

						break;
					default:
						throw new ComponentNotReadyException(
								"Can't determine transformation code type at component ID :" + getId());
					}
				}
			}
		}
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 0, 0)
				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}
		
		 try {
			 DataRecordMetadata[] outMetadata = getOutMetadata().toArray(new DataRecordMetadata[getOutMetadata().size()]);
			 verifyAutofilling(getOutMetadata().get(0));
			 createGeneratorClass(null, outMetadata);
			 
		 } catch (ComponentNotReadyException e) {
			 ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(),
					 ConfigurationStatus.Severity.ERROR, this,
					 ConfigurationStatus.Priority.NORMAL);
			 
			 if(!StringUtils.isEmpty(e.getAttributeName())) {
				 problem.setAttributeName(e.getAttributeName()); 
			 }
			 status.add(problem); 
		 } finally { 
			 free(); 
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
        Object transObject = DynamicJavaClass.instantiate(generatorCode, this.getClass().getClassLoader(),
        		getGraph().getRuntimeContext().getClassPath().getCompileClassPath());

        if (transObject instanceof RecordGenerate) {
			return (RecordGenerate) transObject;
        }

        throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordGenerate.");
    }

	/**
	 * Sets the charset for URL transform file.
	 * @param charset
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}
}
