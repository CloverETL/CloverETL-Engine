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
package org.jetel.component.validator.params;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Parameter of validation rule which has name, placeholder, disablity and tooltip for GUI.
 * Value is supposed to be stored in children. Provide change and disablity handlers.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 28.11.2012
 */
@XmlTransient
@XmlAccessorType(XmlAccessType.NONE)
public abstract class ValidationParamNode {
	private String name;
	private String placeholder;
	private String tooltip;
	private boolean hidden = false;
	private EnabledHandler enabledHandler;
	private ChangeHandler changeHandler;
	
	protected ValidationParamNode() {} // For JAXB
	
	public ValidationParamNode(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public boolean isEnabled() {
		if(enabledHandler != null) {
			return enabledHandler.isEnabled();
		}
		return true;
	}
	
	/**
	 * @return Returns true when param node is hidden, false otherwise
	 */
	public boolean isHidden() {
		return hidden;
	}
	
	/**
	 * Sets this param nodes as hidden (will not show in GUI)
	 * @param hidden True if node should be hidden.
	 */
	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}
	
	/**
	 * Sets handler which determines if the param node is enabled
	 * @param handler Handler to set
	 */
	public void setEnabledHandler(EnabledHandler handler) {
		enabledHandler = handler;
	}
	
	/**
	 * Sets handler to be executed when this param node value is changed
	 * @param handler Handler to set
	 */
	public void setChangeHandler(ChangeHandler handler) {
		changeHandler = handler;
	}
	
	/**
	 * @return Returns change handler
	 */
	public ChangeHandler getChangeHandler() {
		return changeHandler;
	}

	public static interface EnabledHandler {
		public boolean isEnabled();
	}
	public static interface ChangeHandler {
		public void changed(Object o);
	}
	
	/**
	 * Sets placeholder text to be shown in GUI when param node has no value
	 * @param placeholder Placeholder to show
	 */
	public void setPlaceholder(String placeholder) {
		this.placeholder = placeholder;
	}
	/**
	 * @return Returns placeholder
	 */
	public String getPlaceholder() {
		return placeholder;
	}
	
	/**
	 * Sets tooltip text to be shown in GUI
	 * @param tooltip
	 */
	public void setTooltip(String tooltip) {
		this.tooltip = tooltip;
	}
	
	/** 
	 * @return Gets tooltips text
	 */
	public String getTooltip() {
		return tooltip;
	}
}
