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

import java.util.Map;
import java.util.Map.Entry;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.MapDataField;
import org.jetel.data.StringDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

import com.linagora.ldap.Jetel2LdapData.Jetel2LdapByte;
import com.linagora.ldap.Jetel2LdapData.Jetel2LdapString;

/**
 * This class is used by LdapWriter component to actually
 * create connection and managed update on the LDAP directory
 * and transform CloverETL internal data representation to
 * LDAP objects.
 * @see com.linagora.component.LdapWriter
 * @since september 2006
 */
public class LdapFormatter {

	
	/**
	 * 
	 */
	private static final String LDAP_DN_DISTINGUISHED_NAME = "dn";

	/**
	 * Add a new entry. 
	 * Metadata MUST contains 'dn', 'objectclass' and linked 
	 * mandatory attributes of the LDAP schema 
	 * for the corresponding dn.
	 */
	public static final int ADD_ENTRY = 1;

	/**
	 * Remove an entry. Only 'dn' field is mandatory in metadata, other
	 * metadata field are ignored.
	 */
	public static final int REMOVE_ENTRY = 2;

	/**
	 * Replace attribute of an existing entry. 
	 * 'dn' metadata  is mandatory and MUST
	 * exists on LDAP directory.
	 */
	public static final int REPLACE_ATTRIBUTES = 3;

	/**
	 * Remove attributes on an entry. 
	 * 'dn' is mandatory.
	 */
	public static final int REMOVE_ATTRIBUTES = 4;
	
	/** Metadatas */
	private DataRecordMetadata metadata;
	
	/** required action to execute */
	private int action;
	
	/** URL of the LDAP directory */
	private String ldapUrl; 
	
	/** user DN for an authenticated connection */
	private String user;
	
	/** user password for an authenticated connection */
	private String pwd;
	
	/** Transformation object between Attributes  and DataRecord */
	private Jetel2LdapData[] transMap = null;
	
	/** Object that manage connection and execute commands */
	private LdapManager ldapManager;
	
	/** processing of multivaluated attributes */
	private String multiSeparator = null; //'null' means no multi-values are expected
	
	private Jetel2LdapString string2attribute;
	
	/**
	 * A logger (log4j) for the class
	 */
	static Log logger = LogFactory.getLog(LdapFormatter.class);
	
	/**
	 * Constructor for the class LdapFormatter
	 * The connection to the LDAP server is not set here.
	 * @param ldapUrl
	 * @param action
	 */
	public LdapFormatter(String ldapUrl, int action) {
		this(ldapUrl, action, null, null);
	}
	
	public LdapFormatter(String ldapUrl, int action, String user, String passwd) {
		this.action = action;
		this.ldapUrl = ldapUrl;
		this.user = user;
		this.pwd = passwd;
	}

	/**
	 * This is an init methode : the connection to the LDAP server
	 * is set, the transformation object from Jetel Data to 
	 * Ldap Modification Object is initialised.
	 * @param object not used !
	 * @param metadata describing input data to transform to LDAP object
	 * @throws NamingException 
	 */
	public void open(Object object, DataRecordMetadata metadata) throws BadDataFormatException, NamingException {
		
		this.metadata = metadata;
		
		char dn_type = this.metadata.getFieldType(LDAP_DN_DISTINGUISHED_NAME);
		if(dn_type != DataFieldMetadata.STRING_FIELD) {
			throw new BadDataFormatException("Metadata MUST have a \"dn\" field of type string.");
		}
		
		/* 
		 * create a new LdapManager to do related actions
		 */
		if(null != this.user && null != this.pwd) {
			this.ldapManager = new LdapManager(this.ldapUrl,this.user,this.pwd);
		} else {
			this.ldapManager = new LdapManager(this.ldapUrl);
		}
		
		//Actually try to connect to LDAP directory
		this.ldapManager.openContext();
		
		/*
		 * If the required action is "add", we have to verify that the Metadata
		 * match some rules :
		 * - they must have an objectClass Field
		 * - they must have one field per mandatory attribute find in LDAP schema for
		 *   corresponding objectClass
		 * - all Field name must be marked (at miminum) available in LDAP schema for
		 *   corresponding objectClass
		 * 
		 * But this option is not really cool. Because we have to know at init time(1)
		 * what is the objectClass used, and so, what is the ONLY objectClass used.
		 * 
		 * So. I think it is better to build an helper to show to the user what are 
		 * mandatory/available attributes for an object Class at configuration time,
		 * and then to try and cach exception on bad formatted error.
		 * 
		 * (1) : from a performance point of view, it is not reasonnable to try to verify
		 *       each record at run time.
		 *   
		 */
//		if(this.action == ADD_ENTRY) {
//			this.matchSchemaAndMetadatas();
//		}
		
		/*
		 * Init transformation object between Clover datas and Ldap Objects
		 */
		if (null == this.transMap) {
			this.transMap = new Jetel2LdapData[this.metadata.getNumFields()];
			try {
				this.initTransMap();
			} catch (NamingException e) {
				if (logger.isDebugEnabled()) {
					logger.debug(e);
				}
				throw new BadDataFormatException("Bad metadata name in LdapReader component", e);
			}
		}
		
	}
	
	
	/**
	 * Update or create an LDAP entry from a Jetel record
	 * The "dn" Field MUST exist in the metada and MUST NOT be 
	 * null.
	 * @param record
	 * @throws NamingException 
	 * @throws JetelException 
	 * @throws JetelException 
	 */
	public void write(DataRecord record) throws NamingException, BadDataFormatException {
		boolean dn_exists = false;
		String dn = record.getField(LDAP_DN_DISTINGUISHED_NAME).toString();
		
		if(StringUtils.isEmpty(dn)) {
			if(logger.isDebugEnabled()) {
				logger.debug("<LdapFormatter> guilty record: ");
				logger.debug("<LdapFormatter> " + record.toString());
			}
			throw new BadDataFormatException("Metadatas MUST have a \"dn\" field (non empty).");
		}
		
		/*
		 * Existence check : there is no use to continu if action is
		 * "update" or "delete" and entry does not exist
		 */
		
		if(ldapManager.exists(dn)) {
			dn_exists = true;
		} else if(ADD_ENTRY != this.action) {
			if(logger.isDebugEnabled()) {
				logger.debug("<LdapFormatter> Can not add a new entry in Update or Delete Mode, " +
				"record rejected : ");
				logger.debug("<LdapFormatter> " + record.toString());
			}
			throw new NamingException("Can not add a new entry in Update or Delete Mode, ignore rejected: "
					+ record.toString());
		}
		
		Attributes attrs = new BasicAttributes(true);
		
		for (int i = 0; i < transMap.length; i++) {
			// TODO Labels:
			//String attrId = this.metadata.getField(i).getLabelOrName();
			String attrId = this.metadata.getField(i).getName();
			if(!attrId.equalsIgnoreCase(LDAP_DN_DISTINGUISHED_NAME)) { //ignore dn as an attribute
				Attribute attr = new BasicAttribute(attrId);
				DataField dataField = record.getField(i);
				
				//null values with add_entry action are ignored
				if ((this.action == ADD_ENTRY && !dn_exists) && dataField.isNull()) {
					continue;
				}
				if (dataField.getMetadata().getContainerType() == DataFieldContainerType.MAP){
					fillFromMap(attrs, (MapDataField)dataField);
				}else{
					transMap[i].setAttribute(attr, dataField);
					/*
					 * TODO Hum. Shall we add the attr in every case ? 
					 * For instance, if the value is null or equals to "", should we had
					 * the attribute ? In this case, think about setAttribute(attrs, ...)
					 */
					attrs.put(attr);
				}
				
			}
		}
		
		try {
			switch (this.action) {
			case REPLACE_ATTRIBUTES:
				/*
				 * Update an existing entry
				 * If this action is requested, we *only* take care of existing
				 * entries. 
				 * If an entry (i.e a dn) does not exist, it's an error but the stream
				 * do not have to be interrupted. The record should be broadcasted 
				 * on output 0 to be logged. For now, it is just dropped.
				 */
				ldapManager.updateEntry(dn, attrs);
				break;
			case ADD_ENTRY:
				/*
				 * Add a new entry. 
				 * This action is not permissive. The record MUST have a
				 * correct objectclass hierarchy, and MUST have corresponding
				 * mandatory attribute and MUST NOT have non-authorised attribute.
				 * No checking is made before adding an entry.
				 * If the entry already exist, we try to update the entry.
				 * TODO : add a strict mode to ignore existing entry.
				 * Rejected entry should be broacast on output +1 for
				 * logging purpose, but for now, they just are logged.
				 */
				if(dn_exists) {
					ldapManager.updateEntry(dn, attrs);
				} else {
					ldapManager.addEntry(dn, attrs);
				}
				break;
			case REMOVE_ENTRY:
				/*
				 * Actually, this case remove an *entry*, not attribute. 
				 * TODO : I have to clarify what actions are available and what are 
				 * their goal.
				 */
				ldapManager.deleteEntry(dn);
				break;
			case REMOVE_ATTRIBUTES:
				/*
				 * You can not remove attributes which are mandatory for the
				 * class by the schema. If you try, an exception will be throwed.
				 */
				ldapManager.deleteAttributes(dn,attrs);
				break;
			default:
				throw new JetelException (
						"Unknown specified action :"
						+ this.action +" ");
			}
		} catch (Exception e) {
			if(logger.isDebugEnabled()) {
				logger.debug("<LdapFormatter> guilty record: ");
				logger.debug("<LdapFormatter> " + record.toString());
			}
			throw new BadDataFormatException (
					"Error when trying to update Ldap directory entry :"
					+ dn + " ", e);
		}
		
	}
	
	public void close() {
		if (ldapManager != null) {
			try {
				ldapManager.close();
			} catch (NamingException e) {
				//nothing to do ?
				logger.warn("Error while closing LdapManager", e);
			}
		}
	}
	
	
	private void fillFromMap(Attributes attrs, MapDataField field){
		Map<String,StringDataField> map=(Map<String, StringDataField>) field.getValue();
		
		for(Map.Entry<String,StringDataField> entry: map.entrySet()){
			Attribute attr = new BasicAttribute(entry.getKey());
			string2attribute.setAttribute(attr, entry.getValue());
			attrs.put(attr);
		}
		
	}
	
	private void initTransMap() throws NamingException {
		for (int i = 0; i < this.metadata.getNumFields(); i++) {
			DataFieldMetadata dfm = this.metadata.getField(i);
			
			/*
			 * Is the field type compatible with Ldap Attribute type ?
			 * That is a difficult question, because 
			 * we don't know what is the LDAP object targetted : actually, 
			 * each record may have a different dn...
			 * A workaround shall be to force the user to pass a base
			 * dn in the XML config file that MUST be the root of every
			 * dn. Then, we may read from the LDAP schema the matching
			 * object and its requirement (mandatory or optionnal attributes,
			 * spelling rules, etc.). This way is quite interesting, 
			 * but it seems also quite restrictive. Need some time and
			 * experiment to really juge.
			 * Approximation : Ldap attributes are Strings.
			 * TODO : correct this in the right way, from experiences.
			 */
			if(this.metadata.getField(i).getType() == DataFieldMetadata.STRING_FIELD) {
				transMap[i] = new Jetel2LdapString(this.multiSeparator);
			} else if (this.metadata.getField(i).getType() == DataFieldMetadata.BYTE_FIELD
					|| this.metadata.getField(i).getType() == DataFieldMetadata.BYTE_FIELD_COMPRESSED) {
				transMap[i] = new Jetel2LdapByte();
			} else {
				throw new BadDataFormatException("LDAP intialialisation : Field " + dfm.getName()
						+" has type " + dfm.getType() + " which is not supported." +
				"Only String and Byte array types are supported.");
			}
		}
		// will be used for map data field
		string2attribute=new Jetel2LdapString(this.multiSeparator);
	}
	
	public String getMultiSeparator() {
		return multiSeparator;
	}

	public void setMultiValueSeparator(String multiSeparator) {
		this.multiSeparator = multiSeparator;
	}

}
