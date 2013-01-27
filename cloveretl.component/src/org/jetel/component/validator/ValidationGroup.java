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
package org.jetel.component.validator;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlRootElement(name="group")
@XmlAccessorType(XmlAccessType.NONE)
public class ValidationGroup extends ValidationNode {
	
	@XmlElementWrapper(name="children")
	@XmlElementRef
	private List<ValidationNode> childs = new ArrayList<ValidationNode>();
	@XmlAttribute(required=true)
	private Conjunction conjunction = Conjunction.AND;
	
	@XmlAccessorType(XmlAccessType.NONE)
	private static class PrelimitaryCondition {
		@XmlElementRef
		private AbstractValidationRule content;
		public AbstractValidationRule getContent() {return content;}
		public void setContent(AbstractValidationRule value) {content = value;}
	}

	// FIXME: Really?!
	// Problems:
	//  - Cannot be wrapped as its not collection.
	//  - If it's collection than it can contain more prelimitary conditions (not wanted)
	//  - Cannot be left out as it couldn't be ommited @XmlElementRef(required=false) requires JAXB 2.1+ (not enabled in Java 6 by default) 
	@XmlElement(name="prelimitaryCondition")
	private PrelimitaryCondition prelimitaryCondition;
	@XmlAttribute
	private boolean laziness = true;
	
	@XmlType(name = "conjunction")
	@XmlEnum
	public enum Conjunction {
		AND, OR;
	}

	/**
	 * @param conjunction The conjunction to be used by group
	 */
	public void setConjunction(Conjunction conjunction) {
		if(conjunction != null) {
			this.conjunction = conjunction;
		}
	}
	
	public Conjunction getConjunction() {
		return conjunction;
	}

	/**
	 * @param prelimitaryCondition Group's entrance condition
	 */
	public void setPrelimitaryCondition(AbstractValidationRule prelimitaryCondition) {
		if(prelimitaryCondition == null) {
			this.prelimitaryCondition = null;
			return;
		}
		if (this.prelimitaryCondition == null) {
			this.prelimitaryCondition = new PrelimitaryCondition();
		}
		this.prelimitaryCondition.setContent(prelimitaryCondition);
	}
	
	public AbstractValidationRule getPrelimitaryCondition() {
		if(prelimitaryCondition == null) {
			return null;
		}
		return prelimitaryCondition.getContent();
	}

	/**
	 * @param laziness True if lazy evaluation is wanted, false otherwise
	 */
	public void setLaziness(boolean laziness) {
		this.laziness = laziness;
	}

	/**
	 * @param child Validation node to be added into group
	 */
	public void addChild(ValidationNode child) {
		childs.add(child);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		AbstractValidationRule prelimitaryCondition = getPrelimitaryCondition();
		logger.trace("Group: " + this.getName() + "\n"
					+ "Conjunction: " + conjunction + "\n"
					+ "Lazy: " + laziness + "\n"
					+ "Prelimitary condition: " + ((prelimitaryCondition == null)? "not present": prelimitaryCondition.getName()));
		if(prelimitaryCondition != null) { 
			if(prelimitaryCondition.isValid(record, null) == State.INVALID) {
				logger.trace("Skipping group: " + this.getName() + " (due to prelimitary condition).");
				return State.NOT_VALIDATED;
			}
		}
		logger.trace("Entering group: " + this.getName());
		State childState;
		boolean isInvalid = false;
		for(int i = 0; i < childs.size(); i++) {
			childState = childs.get(i).isValid(record,ea);
			if(childState == State.NOT_VALIDATED && conjunction == Conjunction.AND) {
				childState = State.VALID;
			}
			if(childState == State.NOT_VALIDATED && conjunction == Conjunction.OR) {
				childState = State.INVALID;
			}
			if(childState == State.INVALID && laziness) {
				logger.trace("Group: " + getName() + " is " + State.INVALID);
				return State.INVALID;
			}
			if(childState == State.INVALID && !laziness) {
				isInvalid = true;
			}
		}
		if(isInvalid) {
			logger.trace("Group: " + getName() + " is " + State.INVALID);
			return State.INVALID;
		}
		logger.trace("Group: " + getName() + " is " + State.VALID);
		return State.VALID;
	}
	
	@Override
	public boolean isReady() {
		for(int i = 0; i < childs.size(); i++) {
			if(!childs.get(i).isReady()) {
				return false;
			}
		}
		return true;
	}

}
