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
package org.jetel.sequence;

import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
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

    private long value = 0;
    
    private long start = 0;
    
    private long step = 1;
    
    public PrimitiveSequence(String id, TransformationGraph graph, String name) {
        super(id, graph, name);
    }

    /**
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
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
    }

    
    
    @Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	} else {
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
    public int currentValueInt() {
        return (int) value;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueInt()
     */
    public int nextValueInt() {
        value += step;
        return (int) value;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#currentValueLong()
     */
    public long currentValueLong() {
        return value;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueLong()
     */
    public long nextValueLong() {
        value += step;
        return value;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#currentValueString()
     */
    public String currentValueString() {
        return Long.toString(value);
    }

    /**
     * @see org.jetel.data.sequence.Sequence#nextValueString()
     */
    public String nextValueString() {
        value += step;
        return Long.toString(value);
    }

    /**
     * @see org.jetel.data.sequence.Sequence#resetValue()
     */
    public void resetValue() {
        value = start;
    }

    /**
     * @see org.jetel.data.sequence.Sequence#isPersistent()
     */
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
    public void setStart(long start) {
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
    public void setStep(long step) {
        this.step = step;
    }

    static public PrimitiveSequence fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

        try {
            PrimitiveSequence seq =  new PrimitiveSequence(
                xattribs.getString(XML_ID_ATTRIBUTE),
                graph,
                xattribs.getString(XML_NAME_ATTRIBUTE, ""));
            if(xattribs.exists(XML_START_ATTRIBUTE)) {
                seq.setStart(xattribs.getInteger(XML_START_ATTRIBUTE));
            }
            if(xattribs.exists(XML_STEP_ATTRIBUTE)) {
                seq.setStart(xattribs.getInteger(XML_STEP_ATTRIBUTE));
            }
            
            return seq;
        } catch(Exception ex) {
            throw new XMLConfigurationException(SEQUENCE_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(), ex);
        }
    }

	public boolean isShared() {
		return false;
	}

}
