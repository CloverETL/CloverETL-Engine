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
package com.linagora.ldap;

import java.util.regex.Pattern;

import javax.naming.directory.Attribute;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.string.StringUtils;

/**
 * this class is a mapping utilities between 
 * LDAP data and Jetel internal data representation.
 * 
 * @author Francois Armand - Linagora
 * @since august 2006
 */

abstract public class Jetel2LdapData {

	/**
	 * In the case where LDAP attribute are
	 * multi valuated, we have to concatenate them.
	 * This is the separator btw value in Jetel format
	 * 
	 * XXX Sould it be static ? Should it be final ?
	 */
	protected String multiSeparator;

	/**
	 * @param _multiSeparator
	 */

	public Jetel2LdapData(String _multiSeparator) {
		this.multiSeparator = _multiSeparator;
	}

	public String getMultiSeparator() {
		return multiSeparator;
	}

	/**
	 * A logger (log4j) for the class
	 */
	static Log logger = LogFactory.getLog(Jetel2LdapData.class);
	
	/*
	 * This fonction set the value of the field df to the the value 
	 * find in Attribute attr. Type must match.
	 * the comportment of the function with multi-valuated attribute 
	 * has to be managed in implementation.
	 */
	abstract public void setAttribute(Attribute a, DataField df) throws BadDataFormatException;
	
	/*
	 * TODO : we should have a kind of "ldapValues from jetel value" to
	 *        take care of multivaluated attributes
	 */
	abstract public Object[] getvalues(DataField df);
	
	
	/* ---------------------------------------------------------------- */
	/*                    Specialized class for type                    */
	/* ---------------------------------------------------------------- */
	

	static public class Jetel2LdapString extends Jetel2LdapData {

		public Jetel2LdapString(String _multiSeparator) {
			super(_multiSeparator);
		}

		/**
		 * 
		 */
		public void setAttribute(Attribute attr, DataField df)
				throws BadDataFormatException {

			/*
			 * df is null in the DataRecord. It's a real problem, 
			 * if the value is null, df is not and reply true to isNull.			 
			 */
			if (df == null) {
				throw new NullPointerException("Field " + attr.getID()
						+ " is null.");
			} else if (df.getType() != DataFieldMetadata.STRING_FIELD) {
				throw new BadDataFormatException(
						"LDAP transformation exception : Field " + attr.getID()
								+ " is not a String.");
			} else if (df.isNull()) {
				// Set Ldap Attr value to null
				attr.clear();
			} else {
				//TODO perhaps the field is multivaluated ? Search for multiSeparator in it.
				// Quick&dirty hack...
				Object[] values = getvalues(df);
				for(int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (!attr.add(o)) {
						throw new BadDataFormatException(
								"LDAP transformation exception : Field "
										+ attr.getID() + " is not a String.");
					}
				}
			}
		}

		public Object[] getvalues(DataField df) {
			String stringValues = df.getValue().toString();
			/*
			 * Have we a multivaluated value ?
			 */
			if (StringUtils.isEmpty(multiSeparator)) {
				return new Object[] { stringValues };
			} else {
				Pattern pattern = Pattern.compile(Pattern.quote(this.multiSeparator));
			
				return pattern.split(stringValues);
			}
		}

	} //end of class CopyStrin

	static public class Jetel2LdapByte extends Jetel2LdapData {

		public Jetel2LdapByte() {
			super(null);
		}

		/**
		 * 
		 */
		public void setAttribute(Attribute attr, DataField df) throws BadDataFormatException {

			/*
			 * df is null in the DataRecord. It's a real problem, 
			 * if the value is null, df is not and reply true to isNull.			 
			 */
			if (df == null) {
				throw new NullPointerException("Field " + attr.getID() + " is null.");
			} else if (df.getType() != DataFieldMetadata.BYTE_FIELD
					&& df.getType() != DataFieldMetadata.BYTE_FIELD_COMPRESSED) {
				throw new BadDataFormatException("LDAP transformation exception : Field " + attr.getID() + " is not a Byte array.");
			} else if (df.isNull()) {
				// Set Ldap Attr value to null
				attr.clear();
			} else {
				Object[] values = getvalues(df);
				for(int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (!attr.add(o)) {
						throw new BadDataFormatException("LDAP transformation exception : Field " + attr.getID() + " is not a Byte array.");
					}
				}
			}
		}

		public Object[] getvalues(DataField df) {
			return new Object[] { df.getValue() };
		}

	}

}
