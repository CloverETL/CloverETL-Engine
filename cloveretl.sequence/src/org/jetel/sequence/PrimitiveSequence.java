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
package org.jetel.sequence;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * Simple class implementing Sequence interface. It uses internally "long" datatype to
 * store sequence's value. The value is not persistent.<br>
 * 
 * The XML DTD describing the internal structure is as follows:
 * 
 * &lt;!ATTLIST Sequence
 *              id ID #REQUIRED
 *              type NMTOKEN (PRIMITIVE_SEQUENCE) #REQUIRED
 *              name CDATA #REQUIRED
 *              start CDATA #IMPLIED
 *              step CDATA #IMPLIED
 *              
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class PrimitiveSequence extends AbstractSequence {

    public final static String SEQUENCE_TYPE = "PRIMITIVE_SEQUENCE";
    
    public PrimitiveSequence(String id, TransformationGraph graph, String name) {
        super(id, graph, name);
    }
    
    /**
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        try {
            loadExternalSequence();
		} catch (ComponentNotReadyException e) {
			status.addError(this, null, e);
		}
        
        return status;
    }

    /**
     * @see org.jetel.graph.GraphElement#init()
     */
    @Override
    public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		alreadyIncremented = false;
		
		//load external definition of this sequence
		loadExternalSequence();
    }

	/**
	 * Loads settings from config file.
	 * 
	 * @throws ComponentNotReadyException
	 */
	private void loadExternalSequence() throws ComponentNotReadyException {
		if (!StringUtils.isEmpty(configFileName)) {
			try {
				URL projectURL = getContextURL();
				InputStream stream = null;
				try {
					stream = FileUtils.getFileURL(projectURL, configFileName).openStream();

					Properties tempProperties = new Properties();
					tempProperties.load(stream);
					TypedProperties typedProperties = new TypedProperties(tempProperties, getGraph());

					setName(typedProperties.getStringProperty(XML_NAME_ATTRIBUTE));
					setStart(typedProperties.getLongProperty(XML_START_ATTRIBUTE, 0));
					setStep(typedProperties.getLongProperty(XML_STEP_ATTRIBUTE, 0));
				} finally {
					if (stream != null) {
						stream.close();
					}
				}
			} catch (IOException e) {
				throw new ComponentNotReadyException("Loading of external definition of PrimitiveSequence failed.", e);
			}
		}
	}

    @Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		resetValue();
	}

    /**
     * @see org.jetel.graph.GraphElement#free()
     */
    @Override
    public void free() {
        if(!isInitialized()) return;
        super.free();
        //no op
    }

    /**
     * @see org.jetel.data.sequence.Sequence#currentValueLong()
     */
    @Override
	public synchronized long currentValueLong() {
        return alreadyIncremented ? sequenceValue - step : sequenceValue;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueLong()
     */
    @Override
	public synchronized long nextValueLong() {
    	long tmpVal=sequenceValue;
    	sequenceValue += step;
        alreadyIncremented = true;
        return tmpVal;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#resetValue()
     */
    @Override
	public synchronized void resetValue() {
    	alreadyIncremented = false;
    	sequenceValue = start;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#isPersistent()
     */
    @Override
	public boolean isPersistent() {
        return false;
    }
    
    /**
     * Sets start value and resets this sequencer.
     * @param start
     */
    public void setStart(long start) {
        this.start = start;
        resetValue();
    }

    /**
     * Sets step value and resets this sequencer.
     * @param step
     */
    public void setStep(long step) {
        this.step = step;
    }

    public static PrimitiveSequence fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

        String configAttr = xattribs.getString(XML_SEQCONFIG_ATTRIBUTE, "");
		if (configAttr.isEmpty()) {
            PrimitiveSequence seq =  new PrimitiveSequence(
                xattribs.getString(XML_ID_ATTRIBUTE),
                graph,
                xattribs.getString(XML_NAME_ATTRIBUTE, ""));
            if(xattribs.exists(XML_START_ATTRIBUTE)) {
                seq.setStart(xattribs.getLong(XML_START_ATTRIBUTE));
            }
            if(xattribs.exists(XML_STEP_ATTRIBUTE)) {
                seq.setStep(xattribs.getLong(XML_STEP_ATTRIBUTE));
            }
            return seq;
        }
		else {
			PrimitiveSequence seq = new PrimitiveSequence(
					xattribs.getString(XML_ID_ATTRIBUTE),
					graph,
					xattribs.getString(XML_NAME_ATTRIBUTE, ""));
			seq.setConfigFileName(configAttr);
			
			return seq;
		}
    }

    public void setConfigFileName(String configFileName) {
    	this.configFileName = configFileName;
    }

}
