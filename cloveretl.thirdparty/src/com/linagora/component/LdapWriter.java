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
package com.linagora.component;

import java.util.ArrayList;
import java.util.List;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

import com.linagora.ldap.LdapFormatter;



/**
 * <h3>LdapWritter Component</h3>
 *
 * <!-- This class is intended to provide a mean to write information 
 * from an LDAP directory  -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>LdapWritter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Provides the logic to update information on an LDAP directory.<br>
 * An operation can be add/delete entries, add/replace/remove attributes.<br>
 * Metadata MUST match LDAP object attributes names.<br>
 * The "dn" (i.e. Distinguished Name) metadata field/attribute is mandatory.<br>
 * To add a new entry,mandatory attribute/field "objectclass" is required in input metadata/record.<br>
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- rejected records. If rejected port connected then input records rejected by LDAP server get copied to output with fields with autofilling "ErrText" populated with error message.<br/><i>optional</i></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>LDAP attributes may be multivaluated. It depends on the input field type how multi values are handled. If
 * Single type, then separator in the field's value may be used. If List, then each item from the list becomes one value
 * of an attribute. 
 * <br>Only String and (C)Byte field types are supported, both in Single & List container types.<br>
 * If input data/record contains Map&lt;String&gt; field, then keys are mapped on attribute names and values become attribute values.
 * In case of value string with "multiValueSeparator" (if defined) then such value is first split into individual items which then become
 * attribute's multivalues. </td></tr>
 * </table>
 *  <br>
 *  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"LDAP_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>ldapUrl</b><td>Ldap url of the directory, on the form "ldap://host:port/"</td>
 *  <tr><td><b>action</b></td><td>Choose one of these options: add_entry, remove_entry, replace_attributes, remove_attributes</td>
 *  <tr><td><b>user</b><br><i>optional</i></td><td>The user DN to used when connecting to directory.</td>
 *  <tr><td><b>password</b><br><i>optional</i></td><td>The password to used when connecting to directory.</td>
 *  <tr><td><b>multiValueSeparator</b><br><i>optional</i></td><td>The character(s) used to delimit multi-values in String fields of Simple container type.</td>
 *  <tr><td><b>ignoreFields</b><br><i>optional</i></td><td>List of input fields to be ignored (i.e. not mapped as attributes for LDAP). For example ignoring field 
 *  which is optionally populated with error message when sent out.</td>
 * <!-- to be added <tr><td><b>DataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td></tr> -->
 *  </table>
 * 
 * <br>Note:
 * 
 * <h4>Example:</h4>
 * <pre>&lt;&gt;</pre>
 * 
 * <h4>Example:</h4>
 * <pre>&lt;&gt;</pre>
 *
 * 
 * @author   Francois Armand, David Pavlis
 * @since    September 2006
 */
public class LdapWriter extends Node {
	/** Component identification name */
	public final static String COMPONENT_TYPE = "LDAP_WRITER";

	/** Input port */
	private final static int READ_FROM_PORT = 0;

	/** rejected record are writed to this port */
	private final static int WRITE_REJECTED_TO_PORT = 0;

	/**	Multi-value separator constant which means no data values are recognized as multi-value. 
	 *  This is necessary to use special constant for null value due backward compatibility.*/
	public final static String NONE_MULTI_VALUE_SEPARATOR = "__none__";
	
	/*
	 * Attributes read from the xml .grf file
	 * TODO : modify ldap url to server (mandatory) + port (default 389)
	 */
	/** The attribute name in grf file used for the LDAP directory's URL */
	private static final String XML_LDAPURL_ATTRIBUTE = "ldapUrl";
	/** The attribute name in grf file used for the requiried action */
	private static final String XML_ACTION_ATTRIBUTE = "action";
	/** The attribute name in grf file used to specify an user */
	private static final String XML_USER_ATTRIBUTE = "user";
	/** The attribute name in grf file used to specify a password */
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	/** The attribute name in grf file used to specify a multi-value separator */
	private static final String XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE = "multiValueSeparator";
	private static final String XML_IGNORE_FIELDS = "ignoreFields";
	private static final String XML_ADDITIONAL_BINARY_ATTRIBUTES = "binaryAttributes";
	private static final String XML_ADDITIONAL_LDAP_ENV = "ldapExtraProperties";
	
	/*
	 * Action values available on xml file.
	 */
	/** Required action : add new entries in LDAP directory */
	private static final String XML_ADD_ENTRY_VALUE = "add_entry";
	/** Required action : remove entries in LDAP directory */
	private static final String XML_REMOVE_ENTRY_VALUE = "remove_entry";
	/** Required action : replace attributes on existing entries in LDAP directory */
	private static final String XML_REPLACE_ATTRIBUTES_VALUE = "replace_attributes";
	/** Required action : remove attributes on existing entries in LDAP directory */
	private static final String XML_REMOVE_ATTRIBUTES_VALUE = "remove_attributes";

	/** URL used to connect to the LDAP directory */
	private String ldapUrl;
	/** Required action, mapped from the name */
	private int action;
	/** LdapFormater, used to perform actions */
	private LdapFormatter formatter;
	/** User name used on authentificated connection */
	private String user;
	/** User password used on authentificated connection */
	private String passwd;
	/** This string is used as a multi-value separator. 
	 *  One jetel field can contain multiple values separated by this string. */
	private String multiValueSeparator = "|";
	
	private String additionalBinaryAttributes;
	private String ldapExtraPropertiesDef;
	
	/**
	 * Input fields to ignore (if any) during processing
	 */
	private String[] ignoreFields;
	
	/** A logger for the class */
	static Log logger = LogFactory.getLog(LdapWriter.class);
	
	private int autoFillingErrorField;

	/**
	 * Default constructor for the LdapWriter component.
	 * @param  id          id of the component in the graph
	 * @param  fileURL     LDAP connection URL
	 * @param  action      required action type 
	 * @since              2006
	 */
	public LdapWriter(String id, String ldapUrl, int action) {
		this(id, ldapUrl, action, null, null);
	}

	public LdapWriter(String id, String ldapUrl, int action, String user, String passwd) {
		super(id);
		this.ldapUrl = ldapUrl;
		this.action = action;
		this.user = user;
		this.passwd = passwd;
	}

	/**
	 * Initialisation methode. 
	 * This method has to be called before the use of the component.
	 * It try to connect to the LDAP directory, check validity of
	 * parameters, etc.
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  September, 2006
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		this.formatter = new LdapFormatter(this.ldapUrl, this.action, this.user, this.passwd);
		this.formatter.setMultiValueSeparator(multiValueSeparator);
		
		if (additionalBinaryAttributes != null && additionalBinaryAttributes.length() > 0){
			formatter.setAdditionalBinaryAttributes(additionalBinaryAttributes);
		}
		if (ldapExtraPropertiesDef != null && ldapExtraPropertiesDef.length() > 0){
			formatter.setLdapExtraPropertiesDef(ldapExtraPropertiesDef);
		}
		
		DataRecordMetadata metadata = getInputPort(READ_FROM_PORT).getMetadata();
		// based on file mask, create/open output file
		try {
			formatter.open(null, metadata);
		} catch (Exception ex) {
			throw new ComponentNotReadyException(getId() + " Error opening LdapFormater", ex);
		}
		
		
		autoFillingErrorField=metadata.findAutoFilledField(AutoFilling.ERROR_MESSAGE);
		// process list of fields to ignore
		if (this.ignoreFields != null) {
			List<Integer> ignoreFieldsIdx = new ArrayList<Integer>();
			for (String field : ignoreFields) {
				int idx = metadata.getFieldPosition(field);
				if (idx >= 0) {
					ignoreFieldsIdx.add(idx);
				}
			}
			if (ignoreFieldsIdx.size() > 0) {
				int[] idxs = new int[ignoreFieldsIdx.size()];
				int i = 0;
				for (Integer idx : ignoreFieldsIdx)
					idxs[i++] = idx;
				this.formatter.setIgnoreFields(idxs);
			}
		}
	}
	

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort rejectedPort=getOutputPort(WRITE_REJECTED_TO_PORT);
		DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		try {
			while (null != inRecord && runIt) {
				try {
					inRecord = inPort.readRecord(inRecord);
					if (null != inRecord) {
						formatter.write(inRecord);
					}
				} catch (NamingException | BadDataFormatException ex) 
				{
					if (rejectedPort!=null){
							if (autoFillingErrorField>=0){
								inRecord.getField(autoFillingErrorField).fromString(
										new StringBuilder(ex.getMessage()));
							}
							rejectedPort.writeRecord(inRecord);
					}else{
						throw ex;
					}
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 *  This method read from an xml tree the description of
	 *  the component and initialize it according to the
	 *  configuration.
	 *  @param graph
	 *  @param nodeXML
	 * @throws AttributeNotFoundException 
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs= new ComponentXMLAttributes(nodeXML, graph);
		LdapWriter aSimpleLdapWriter = null;
		int action = 0;
		String action_value = xattribs.getString(XML_ACTION_ATTRIBUTE, null);
		if(action_value != null && action_value.equalsIgnoreCase(XML_ADD_ENTRY_VALUE)) {
			action = LdapFormatter.ADD_ENTRY;
		} else if(action_value != null && action_value.equalsIgnoreCase(XML_REMOVE_ENTRY_VALUE)) {
			action = LdapFormatter.REMOVE_ENTRY;
		} else if(action_value != null && action_value.equalsIgnoreCase(XML_REPLACE_ATTRIBUTES_VALUE)) {
			action = LdapFormatter.REPLACE_ATTRIBUTES;
		} else if(action_value != null && action_value.equalsIgnoreCase(XML_REMOVE_ATTRIBUTES_VALUE)) {
			action = LdapFormatter.REMOVE_ATTRIBUTES;
		} else {
			StringBuffer msg = new StringBuffer();
			if (action_value == null) {
				msg.append("Missing action specification");
			} else {
				msg.append("Invalid action specification \"").append(action_value).append("\"");
			}
			msg.append(" in component ").append(xattribs.getString(Node.XML_ID_ATTRIBUTE, "unknown ID"));
			msg.append("; defaulting to action \"").append(XML_ADD_ENTRY_VALUE).append("\"");
			logger.warn(msg.toString());
			action = LdapFormatter.ADD_ENTRY;
		}
		

		if(xattribs.exists(XML_USER_ATTRIBUTE) && xattribs.exists(XML_PASSWORD_ATTRIBUTE) ) {
			aSimpleLdapWriter = new LdapWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getStringEx(XML_LDAPURL_ATTRIBUTE, RefResFlag.URL),
					action,
					xattribs.getString(XML_USER_ATTRIBUTE),
					xattribs.getStringEx(XML_PASSWORD_ATTRIBUTE, RefResFlag.PASSWORD));
		} else {
			aSimpleLdapWriter = new LdapWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getString(XML_LDAPURL_ATTRIBUTE),
					action);
			
		}
		if (xattribs.exists(XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE)) {
			aSimpleLdapWriter.setMultiValueSeparator(xattribs.getString(XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE));
		}
		if (xattribs.exists(XML_IGNORE_FIELDS)){
			aSimpleLdapWriter.setIgnoreFields(xattribs.getString(XML_IGNORE_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
		}
		if (xattribs.exists(XML_ADDITIONAL_BINARY_ATTRIBUTES)){
			aSimpleLdapWriter.setAdditionalBinaryAttributes(xattribs.getString(XML_ADDITIONAL_BINARY_ATTRIBUTES));
		}
		if (xattribs.exists(XML_ADDITIONAL_LDAP_ENV)){
			aSimpleLdapWriter.setLdapExtraPropertiesDef(xattribs.getString(XML_ADDITIONAL_LDAP_ENV));
		}
		return aSimpleLdapWriter;
	}

	
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 0, 1);
		
		//TODO Labels:
        //InputPort inputPort = getInputPort(READ_FROM_PORT);
        //if (inputPort != null) {
        //    new UniqueLabelsValidator(status, this).validateMetadata(inputPort.getMetadata());
        //}
        //TODO Labels end

        try {
            init();
        } catch (ComponentNotReadyException e) {
            status.addError(this, null, e);
        } finally {
        	free();
        }
        
        return status;
    }
	
	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		// no operations needed
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	public synchronized void free() {
		super.free();
		if (formatter != null) {
			formatter.close();
		}
	}

	public String getMultiValueSeparator() {
		return multiValueSeparator;
	}

	public void setMultiValueSeparator(String multiValueSeparator) {
		if (!StringUtils.isEmpty(multiValueSeparator) && !multiValueSeparator.equals(NONE_MULTI_VALUE_SEPARATOR)) {
			this.multiValueSeparator = multiValueSeparator;
		} else {
			this.multiValueSeparator = null;
		}
	}

	public String[] getIgnoreFields() {
		return ignoreFields;
	}

	public void setIgnoreFields(String[] ignoreFields) {
		this.ignoreFields = ignoreFields;
	}

	public void setAdditionalBinaryAttributes(String additionalBinaryAttributes) {
		this.additionalBinaryAttributes = additionalBinaryAttributes;
	}

	public void setLdapExtraPropertiesDef(String ldapExtraPropertiesDef) {
		this.ldapExtraPropertiesDef = ldapExtraPropertiesDef;
	}
}
