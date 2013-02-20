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

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.jetel.data.sequence.Sequence;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
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
public class PrimitiveSequence extends GraphElement implements Sequence {

    public final static String SEQUENCE_TYPE = "PRIMITIVE_SEQUENCE";

    private static final String XML_NAME_ATTRIBUTE = "name";
    private static final String XML_START_ATTRIBUTE = "start";
    private static final String XML_STEP_ATTRIBUTE = "step";
    private static final String XML_SEQCONFIG_ATTRIBUTE = "seqConfig";

	private static Exception initFromConfigFileException;

    private long value = 0;
    private long start = 0;
    private long step = 1;
    boolean alreadyIncremented = false;
    
    public PrimitiveSequence(String id, TransformationGraph graph, String name) {
        super(id, graph, name);
    }
    
    /**
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
    	if (initFromConfigFileException != null) {
    		status.add("Failed to initialize sequence from definition file; " + initFromConfigFileException, Severity.ERROR, this, Priority.NORMAL);
    		return status;
    	}

        //TODO
        return status;
    }

    /**
     * @see org.jetel.graph.GraphElement#init()
     */
    @Override
    synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		alreadyIncremented = false;
    }

    
    
    @Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	} else {
    		logger.debug("Primitive sequence '" + getId() + "' reset.");
    		resetValue();
    	}
	}

	@Override
    public synchronized void reset() throws ComponentNotReadyException {
    	super.reset();
    }
    
    /**
     * @see org.jetel.graph.GraphElement#free()
     */
    @Override
    synchronized public void free() {
        if(!isInitialized()) return;
        super.free();
        //no op
    }

    /**
     * @see org.jetel.data.sequence.Sequence#currentValueInt()
     */
    @Override
	public int currentValueInt() {
        return (int) currentValueLong();
    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueInt()
     */
    @Override
	public int nextValueInt() {
        return (int) nextValueLong();
    }

    /**
     * @see org.jetel.data.sequence.Sequence#currentValueLong()
     */
    @Override
	public synchronized long currentValueLong() {
        return alreadyIncremented ? value - step : value;

    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueLong()
     */
    @Override
	public synchronized long nextValueLong() {
    	long tmpVal=value;
        value += step;
        alreadyIncremented = true;
        return tmpVal;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#currentValueString()
     */
    @Override
	public String currentValueString() {
        return Long.toString(currentValueLong());
    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueString()
     */
    @Override
	public String nextValueString() {
        return Long.toString(nextValueLong());
    }

    /**
     * @see org.jetel.data.sequence.Sequence#resetValue()
     */
    @Override
	public synchronized void resetValue() {
    	alreadyIncremented = false;
        value = start;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#isPersistent()
     */
    @Override
	public boolean isPersistent() {
        return false;
    }

    public long getStart() {
        return start;
    }

    /**
     * Sets start value and resets this sequencer.
     * @param start
     */
    public synchronized void setStart(long start) {
        this.start = start;
        resetValue();
    }

    public long getStep() {
        return step;
    }

    /**
     * Sets step value and resets this sequencer.
     * @param step
     */
    public synchronized void setStep(long step) {
        this.step = step;
    }

    static public PrimitiveSequence fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
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
			
            try {
            	URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
                InputStream stream = FileUtils.getFileURL(projectURL, configAttr).openStream();

                Properties tempProperties = new Properties();
                tempProperties.load(stream);
        		TypedProperties typedProperties = new TypedProperties(tempProperties, graph);

        		seq.setName(typedProperties.getStringProperty(XML_NAME_ATTRIBUTE));
        		seq.start = typedProperties.getLongProperty(XML_START_ATTRIBUTE, 0);
        		seq.step = typedProperties.getLongProperty(XML_STEP_ATTRIBUTE, 0);
        		
                stream.close();
            } catch (Exception ex) {
                initFromConfigFileException = ex;
            }
			
			return seq;
		}
    }

	@Override
	public boolean isShared() {
		return false;
	}

}
