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

import java.io.UnsupportedEncodingException;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;

import org.jetel.data.DataField;
import org.jetel.data.ListDataField;
import org.jetel.data.MapDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.string.StringUtils;

/**
 * this class is a mapping utilities between LDAP data and Jetel internal data
 * representation.
 * 
 * @author Francois Armand - Linagora, David Pavlis <david.pavlis@cloveretl.com>
 * @since august 2006
 */
public abstract class Ldap2JetelData {

	/**
	 * In the case where LDAP attribute are multi valuated, we have to
	 * concatenat them. This is the separator btw value in Jetel format
	 * 
	 * XXX Sould it be static ? Should it be final ?
	 */
	protected String multiSeparator;

	/**
	 * @param _multiSeparator
	 */

	public Ldap2JetelData(String _multiSeparator) {
		this.multiSeparator = _multiSeparator;
	}

	public String getMultiSeparator() {
		return multiSeparator;
	}

	/*
	 * This fonction set the value of the field df to the the value find in
	 * Attribute attr. Type must match. the comportment of the function with
	 * multi-valuated attribute has to be managed in implementation.
	 */
	abstract public boolean setField(DataField df, Attribute attr)
			throws BadDataFormatException;

	static public class Ldap2JetelString extends Ldap2JetelData {

		public Ldap2JetelString(String _multiSeparator) {
			super(_multiSeparator);
		}

		/**
		 * This function set the value of the field passed in argument
		 * to the (multi)value of the LDAP attr.
		 * @param df the field which has to be set
		 * @param attr the LDAP attribute whose values have to be got 
		 */
		@Override
		public boolean setField(DataField df, Attribute attr)
				throws BadDataFormatException {

			if (attr == null) { // attr not set in the LDAP directory
				df.setNull(true);
				return false;
			}
			
			if (df.getType() != DataFieldMetadata.STRING_FIELD) {
				throw new BadDataFormatException(
						"LDAP attribute to Clover field transformation exception : Field "
								+ attr.getID() + " is not a String.");
			} else {
				NamingEnumeration ne = null;
				try {
					ne = attr.getAll();
				} catch (NamingException e) {
					throw new BadDataFormatException(
							"LDAP attribute to Clover field transformation exception : Field "
									+ attr.getID() + ".", e);
				}
				/*
				 * Perhaps the attribute is multivaluated, so we add all values.
				 */
				try {
					if (ne.hasMore()) {
						if (df.getMetadata().getContainerType() == DataFieldContainerType.SINGLE) {
							StringBuilder resString = new StringBuilder("");
							resString.append(ne.next().toString());
							if (!StringUtils.isEmpty(multiSeparator)) {
								while (ne.hasMore()) {
									Object o = ne.next();
									resString.append(this.multiSeparator);
									resString.append(o.toString());
								}
							}
							df.setValue(resString.toString());
						} else if (df.getMetadata().getContainerType() == DataFieldContainerType.LIST) {
							((ListDataField) df).clear();
							while (ne.hasMore()) {
								Object o = ne.next();
								((ListDataField) df).addField().setValue(o.toString());
							}
						} else {
							throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + " can not be copied to MAP.");
						}
					} else { // attr exist but value are null.
						df.setNull(true);
					}
				} catch (NamingException e) {
					throw new BadDataFormatException(
							"LDAP attribute to Clover field transformation exception : Field "
									+ attr.getID() + ".", e);
				}
			}
			return true; // attribute existed
		}

	} //end of class CopyString

	static public class Ldap2JetelByte extends Ldap2JetelData {

		public Ldap2JetelByte() {
			super(null);
		}
		
		/**
		 * This function set the value of the field passed in argument
		 * to the value of the LDAP attr.
		 * @param df the field which has to be set
		 * @param attr the LDAP attribute whose values have to be got 
		 */
		@Override
		public boolean setField(DataField df, Attribute attr) throws BadDataFormatException {
			if (attr == null) { // attr not set in the LDAP directory
				df.setNull(true);
				return false;
			}
			
			if (df.getType() == DataFieldMetadata.BYTE_FIELD 
					|| df.getType() == DataFieldMetadata.BYTE_FIELD_COMPRESSED) {
				
				if (attr.size()<=1 && df.getMetadata().getContainerType()==DataFieldContainerType.SINGLE){
				Object value;
				try {
					value = attr.get(); //only first value is taken into consideration
				} catch (NamingException e) {
					throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + ".", e);
				}
				if (value == null) {
					df.setNull(true);
				} else {
					if (value instanceof byte[]) {
						df.setValue(value);
					} else if (value instanceof String) {
						try {
							df.setValue(((String)value).getBytes("UTF-8"));
						} catch (UnsupportedEncodingException e) {
							throw new RuntimeException("Failed to set String into Byte field", e);
						}
					} else {
						throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + " is not a Byte array or String.");
					}
				}
				}else if (df.getMetadata().getContainerType()==DataFieldContainerType.LIST){
					NamingEnumeration ne=null;
					try{
						ne = attr.getAll();
					} catch (NamingException e) {
						throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + ".", e);
					}
					((ListDataField)df).clear();
					try{
					while(ne.hasMore()){
						Object value=ne.next();
						if (value instanceof byte[]) {
							((ListDataField)df).addField().setValue(value);
						} else if (value instanceof String) {
							try {
								((ListDataField)df).addField().setValue(((String)value).getBytes("UTF-8"));
							} catch (UnsupportedEncodingException e) {
								throw new RuntimeException("Failed to set String into Byte field", e);
							}
						} else {
							throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + " is not a Byte array or String.");
						}
					}
					}catch(NamingException ex){
						throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + ".", ex);
					}
				}
			} else {
				throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + " is not a Byte array or String.");
			}
			return true; // attribute existed
		}

	} //end of class CopyByte
	
	static public class Ldap2JetelMap extends Ldap2JetelData {

		public Ldap2JetelMap(String _multiSeparator) {
			super(_multiSeparator);
		}

		/**
		 * This function set the value of the field passed in argument to the (multi)value of the LDAP attr.
		 * 
		 * @param df
		 *            the field which has to be set
		 * @param attr
		 *            the LDAP attribute whose values have to be got
		 */
		@Override
		public boolean setField(DataField df, Attribute attr) throws BadDataFormatException {

			if (attr == null) { // attr not set in the LDAP directory
				df.setNull(true);
				return false;
			}

			if (df.getType() != DataFieldMetadata.STRING_FIELD || df.getMetadata().getContainerType() != DataFieldContainerType.MAP) {
				throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + " is not a MAP of String.");
			}

			DataField valueField = ((MapDataField)df).getField(attr.getID());
			if (valueField == null) {
				valueField=((MapDataField)df).putField(attr.getID());
			}

			NamingEnumeration ne = null;
			try {
				ne = attr.getAll();
			} catch (NamingException e) {
				throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + ".", e);
			}
			/*
			 * Perhaps the attribute is multivaluated, so we add all values.
			 */
			try {
				if (ne.hasMore()) {
					StringBuilder resString = new StringBuilder("");
					resString.append(ne.next().toString());
					if (!StringUtils.isEmpty(multiSeparator)) {
						while (ne.hasMore()) {
							Object o = ne.next();
							resString.append(this.multiSeparator);
							resString.append(o.toString());
						}
					}
					valueField.setValue(resString.toString());
				} else { // attr exist but value are null.
					valueField.setNull(true);
				}
			} catch (NamingException e) {
				throw new BadDataFormatException("LDAP attribute to Clover field transformation exception : Field " + attr.getID() + ".", e);
			}
			return true; // attribute existed
		}

	} //end of class CopyMap


}