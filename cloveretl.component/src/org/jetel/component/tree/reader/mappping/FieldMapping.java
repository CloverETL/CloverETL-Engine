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
package org.jetel.component.tree.reader.mappping;

/**
 * 'Mapping' mapping element model.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.12.2011
 */
public class FieldMapping extends MappingElement {

	private boolean trim = true;
	/**
	 * for mapping of node to output field
	 */
	private String nodeName;
	/**
	 * output field
	 */
	private String cloverField;
	/**
	 * for mapping of input field to output field
	 */
	private String inputField;
	
	@Override
	public void acceptVisitor(MappingVisitor visitor) {
		visitor.visit(this);
	}
	
	public boolean isTrim() {
		return trim;
	}
	
	public void setTrim(boolean trim) {
		this.trim = trim;
	}
	
	public String getNodeName() {
		return nodeName;
	}
	
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	
	public void setInputField(String inputField) {
		this.inputField = inputField;
	}
	
	public String getInputField() {
		return inputField;
	}
	
	public String getCloverField() {
		return cloverField;
	}
	
	public void setCloverField(String cloverField) {
		this.cloverField = cloverField;
	}
	
	@Override
	public MappingContext getParent() {
		return (MappingContext)super.getParent();
	}
}
