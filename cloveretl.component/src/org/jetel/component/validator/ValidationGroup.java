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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;
import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlRootElement(name="group")
@XmlAccessorType(XmlAccessType.NONE)
public class ValidationGroup extends ValidationNode {
	
	@XmlElementWrapper(name="children")
	@XmlElementRefs({
		@XmlElementRef(type=ValidationGroup.class),
		@XmlElementRef(type=EnumMatchValidationRule.class),
		@XmlElementRef(type=NonEmptyFieldValidationRule.class),
		@XmlElementRef(type=NonEmptySubsetValidationRule.class),
		@XmlElementRef(type=PatternMatchValidationRule.class),
		@XmlElementRef(type=StringLengthValidationRule.class),
	})
	private List<ValidationNode> childs = new ArrayList<ValidationNode>();
	@XmlAttribute
	private Conjunction conjunction = Conjunction.AND;
	
	// Set is only workaround for JAXB unability to wrap simple attribute and to make it opinional
	@XmlElementWrapper(name="prelimitaryCondition")
	//@XmlElementRef
	private Set<AbstractValidationRule> prelimitaryCondition = new HashSet<AbstractValidationRule>();
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
		this.conjunction = conjunction;
	}

	/**
	 * @param prelimitaryCondition Group's entrance condition
	 */
	public void setPrelimitaryCondition(AbstractValidationRule prelimitaryCondition) {
		this.prelimitaryCondition.clear();
		this.prelimitaryCondition.add(prelimitaryCondition);
	}
	
	public AbstractValidationRule getPrelimitaryCondition() {
		if(this.prelimitaryCondition.size() == 0) {
			return null;
		}
		return this.prelimitaryCondition.iterator().next();
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
