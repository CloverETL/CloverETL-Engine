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
package org.jetel.exception;

/**
 * Exception cast when requrested XML attribute (its value) can't be found or
 * resolving reference to global (TransformationGraph level) variable/property
 * can't be done - the property key is not present.
 * 
 * @see Exception
 * @author D.Pavlis
 */
public class AttributeNotFoundException extends Exception {

	private static final long serialVersionUID = 5458258223226987477L;

	// Attributes
    String attributeName;

    String causedByClass;

    // Associations

    // Operations
    public AttributeNotFoundException(String keyValue) {
        super("Attribute/property not found: " + keyValue);
    }

    public AttributeNotFoundException(String attributeName, String message) {
        super(message);
        this.attributeName = attributeName;
    }

    public AttributeNotFoundException(String attributeName, String message,
            String causedBy) {
        super(message);
        this.attributeName = attributeName;
        this.causedByClass = causedBy;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String keyValue) {
        this.attributeName = keyValue;
    }

    public String getCausedByClass() {
        return causedByClass;
    }

    public void setCausedByClass(String causedByClass) {
        this.causedByClass = causedByClass;
    }

} /* end class AttributeNotFoundException */
