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

import org.jetel.data.sequence.Sequence;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;

/**
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 1. 2015
 */
public abstract class AbstractSequence extends GraphElement implements Sequence {
	
    protected String configFileName; //file name with external definition of this sequence
    
    protected long sequenceValue = 0;
    protected long step = 1;
    protected long start = 0;
    
    protected boolean alreadyIncremented = false;
    
    protected static final String XML_START_ATTRIBUTE = "start";
    protected static final String XML_STEP_ATTRIBUTE = "step";
    protected static final String XML_SEQCONFIG_ATTRIBUTE = "seqConfig";

	public AbstractSequence(String id, TransformationGraph graph, String name) {
		super(id, graph, name);
	}
	
	public AbstractSequence(String id, TransformationGraph graph) {
		super(id, graph);
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
     * @see org.jetel.data.sequence.Sequence#currentValueInt()
     */
	@Override
	public int currentValueInt() {
    	long currentValueLong = currentValueLong();
    	if (isOutOfIntegerBounds(currentValueLong)) {
    		throw new ArithmeticException("Can't get currentValue as integer from sequence " + getName() + " because of value overflow/underflow."
    				+ " Overflow/underflow sequence value: " + currentValueLong);
    	}
        return (int) currentValueLong();
    }
	
	/**
     * @see org.jetel.data.sequence.Sequence#nextValueInt()
     */
    @Override
	public int nextValueInt() {
    	long nextValueLong = nextValueLong();
    	if (isOutOfIntegerBounds(nextValueLong)) {
    		throw new ArithmeticException("Can't get nextValue as integer from sequence " + getName() + " because of value overflow/underflow."
    				+ " Overflow/underflow sequence value: " + nextValueLong);
    	}
        return (int) nextValueLong;
    }
	
	private boolean isOutOfIntegerBounds(long value) {
    	if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
    		return true;
    	}
    	return false;
    }
	
	public long getStart() {
		return start;
	}
	
	public long getStep() {
		return step;
	}

}
