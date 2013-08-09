/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.metadata.extraction;

import java.util.Properties;

import org.jetel.metadata.DataFieldType;

/**
 * Guessed metadata field type.
 * 
 * @author Martin Slama (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created June 25th, 2013
 */
public class FieldTypeGuess {

	private DataFieldType type;
	private Properties typeProperties;
	private String name;
	private String label;
	
	/**
	 * @return Factory method for creating default guess type.
	 */
	public static FieldTypeGuess defaultGuess() {
		return new FieldTypeGuess(DataFieldType.STRING);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param type Guessed field type.
	 */
	public FieldTypeGuess(DataFieldType type) {
		this(type, new Properties());
	}
	
	/**
	 * Constructor.
	 * 
	 * @param type Guessed field type.
	 * @param typeProperties Properties of filed type.
	 */
	public FieldTypeGuess(DataFieldType type, Properties typeProperties) {
		this.setType(type);
		this.typeProperties = typeProperties;
	}
	/**
	 * @return Properties of type.
	 */
	public Properties getTypeProperties() {
		return typeProperties;
	}
	
	/**
	 * @param typeProperties Field type properties to be set.
	 */
	public void setTypeProperties(Properties typeProperties) {
		this.typeProperties = typeProperties;
	}
	
	/**
	 * @return Guessed type.
	 */
	public DataFieldType getType() {
		return type;
	}
	
	/**
	 * @param type the type to set
	 */
	public void setType(DataFieldType type) {
		this.type = type;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @param label the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	@Override
	public String toString() {
		return getType() + " " + typeProperties;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((getType() == null) ? 0 : getType().hashCode());
		result = prime * result + ((typeProperties == null) ? 0 : typeProperties.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FieldTypeGuess other = (FieldTypeGuess) obj;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (getType() != other.getType())
			return false;
		if (typeProperties == null) {
			if (other.typeProperties != null)
				return false;
		} else if (!typeProperties.equals(other.typeProperties))
			return false;
		return true;
	}
}
