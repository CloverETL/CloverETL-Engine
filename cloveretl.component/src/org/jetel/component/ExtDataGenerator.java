
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.graph.Result;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Data Generator Component</h3> <!-- Generates new records.  -->
 *
 *  
 *  @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu) 
 *
 */
public class ExtDataGenerator extends DataGenerator {
	
    static Log logger = LogFactory.getLog(ExtDataGenerator.class);

	// Description of the Field
	public final static String COMPONENT_TYPE = "EXT_DATA_GENERATOR";

	// XML attribute names
	private static final String XML_GENERATECLASS_ATTRIBUTE = "generateClass";
	private static final String XML_GENERATE_ATTRIBUTE = "generate";
	private static final String XML_GENERATEURL_ATTRIBUTE = "generateURL";

	// Input parameters
	private String generate;
	private String generateClass;
	private String generateURL;
	private int recordsNumber;
	
	// data generator
	private Properties generateParameters;
	private RecordGenerate generation;

	/**
	 * @param id
	 * @param pattern
	 * @param recordsNumber
	 */
	public ExtDataGenerator(String id, String generate, String generateClass, String generateURL, int recordsNumber) {
		super(id);
		this.generate = generate;
		this.generateClass = generateClass;
		this.generateURL = generateURL;
		this.recordsNumber = recordsNumber;
	}

	public static String getTransformAttributeName() {
		return XML_GENERATE_ATTRIBUTE;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		// initialize output ports
		int numOutputPorts=getOutPorts().size();
		DataRecord outRecord[] = new DataRecord[numOutputPorts]; 
		for (int i = 0; i < numOutputPorts; i++) {
			outRecord[i] = new DataRecord(getOutputPort(i).getMetadata());
			outRecord[i].init();
			outRecord[i].reset();
		}
		if (generation != null) executeGenerate(outRecord);
		else executeAutoFilling(outRecord);

		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/**
	 * Generate all.
	 * @param outRecord
	 * @throws Exception
	 */
	private void executeGenerate(DataRecord[] outRecord) throws Exception {
		for (int i=0;i<recordsNumber && runIt;i++){
			for (DataRecord oRecord: outRecord)	oRecord.reset();
			int transformResult = generation.generate(outRecord);

			if (transformResult == RecordTransform.ALL) {
				for (int outPort = 0; outPort < outRecord.length; outPort++) {
					autoFilling.setLastUsedAutoFillingFields(outRecord[outPort]);
					writeRecord(outPort, outRecord[outPort]);
				}
			} else if (transformResult >= 0) {
				autoFilling.setLastUsedAutoFillingFields(outRecord[transformResult]);
				writeRecord(transformResult, outRecord[transformResult]);
			} else if (transformResult == -1) {
				//DO NOTHING - skip the record
			} else {
				throw new TransformException("Transformation finished with code: " + transformResult + 
						". Error message: " + generation.getMessage());
			}
			
			SynchronizeUtils.cloverYield();
		}
		
		if (generation != null) {
			generation.finished();
		}
	}
	
	/**
	 * Generate only autofilling.
	 * @param outRecord
	 * @throws Exception
	 */
	private void executeAutoFilling(DataRecord[] outRecord) throws Exception {
		for (int i=0;i<recordsNumber && runIt;i++){
			for (int outPort = 0; outPort < outRecord.length; outPort++) {
				autoFilling.setLastUsedAutoFillingFields(outRecord[outPort]);
				writeRecord(outPort, outRecord[outPort]);
			}
			SynchronizeUtils.cloverYield();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (generate != null) {
			xmlElement.setAttribute(XML_GENERATE_ATTRIBUTE, generate);
		}
		if (generateClass != null) {
			xmlElement.setAttribute(XML_GENERATECLASS_ATTRIBUTE, generateClass);
		}
		if (generateURL != null) {
			xmlElement.setAttribute(XML_GENERATEURL_ATTRIBUTE, generateURL);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
        super.init();
        
        // create output metadata
        int outPortsNum = getOutPorts().size();
        DataRecordMetadata outMetadata[] = new DataRecordMetadata[outPortsNum];
        for(int i = 0; i < outPortsNum; i++) {
            outMetadata[i] = getOutputPort(i).getMetadata();
        }

        // if no generator then verify and prepare autofilling
        if (generate == null && generateClass == null && generateURL == null) {
        	boolean isAutoFilling = false;
        	for (DataFieldMetadata fMetadata: outMetadata[0].getFields()) {
        		if (fMetadata.isAutoFilled()) {
        			isAutoFilling = true;
        		}
        	}
        	if (!isAutoFilling) throw new ComponentNotReadyException("Attribute/property not found: " + XML_GENERATE_ATTRIBUTE);
        	
   		// create instance of the record generator
        } else {
        	generation = RecordTransformFactory.createGenerator(generate, generateClass, 
    				generateURL, this, outMetadata, generateParameters, 
    				this.getClass().getClassLoader(), this.getGraph().getWatchDog().getGraphRuntimeContext().getClassPaths());
        }
		
		// autofilling
		if (outMetadata.length > 0) {
	   		autoFilling.addAutoFillingFields(outMetadata[0]);
		}
   		autoFilling.setFilename(getId());
   		autoFilling.addAutoFillingFields(outMetadata[0]);
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		autoFilling.reset();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
  		 
		if(!checkInputPorts(status, 0, 0) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}
        checkMetadata(status, getOutMetadata());
        
        /*
        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }*/
        
        return status;
	}

    /**
	 * @param generateParameters
	 *            The generationParameters to set.
	 */
    public void setTransformationParameters(Properties generateParameters) {
        this.generateParameters = generateParameters;
    }

}
