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

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
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
 * An update can be add/delete entries, add/replace/remove attributes.<br>
 * Metadata MUST match LDAP object attribute name.<br>
 * "DN" metadata attribute is mandatory<br>
 * String is the only metadata type supported.<br>
 * LDAP rules are applied : to add an entry,mandatory attribute (even object classe) 
 * are requiered in metadata.<br>
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- rejected records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td>LDAP attribute may be multivaluated. For now, we handle such case by using 
 * the "|" separator in a value. As a consequence, only string could be correctly handled. 
 * <br>That's why metadata string type is the only one supported.</tr>
 * </table>
 *  <br>
 *  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"LDAP_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>ldapUrl</b><td>Ldap url of the directory, on the form "ldap://host:port/"</td>
 *  <tr><td><b>action</b></td><td>Choose one of these options: add_entry, remove_entry, replace_attributes, remove_attributes</td>
 *  <tr><td><b>user</b><br><i>optional</i></td>The user DN to used when connecting to directory.<td></td>
 *  <tr><td><b>password</b><br><i>optional</i></td>The password to used when connecting to directory.<td></td>
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
 * <h4>Example:</h4>
 * <pre>&lt;&gt;</pre>
 * 
 * <h4>Example:</h4>
 * <pre>&lt;&gt;</pre>
 * 
 * @author   Francois Armand
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
	
	/** A logger for the class */
	static Log logger = LogFactory.getLog(LdapWriter.class);


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
		
		// based on file mask, create/open output file
		try {
			formatter.open(null, getInputPort(READ_FROM_PORT).getMetadata());
		} catch (Exception ex) {
			throw new ComponentNotReadyException(getId() + " Error opening LdapFormater", ex);
		}

	}
	

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort rejectedPort=getOutputPort(WRITE_REJECTED_TO_PORT);

		DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		inRecord.init();
		try {
			while (null != inRecord && runIt) {
				try {
					inRecord = inPort.readRecord(inRecord);
					if (null != inRecord) {
						formatter.write(inRecord);
					}
				} catch (NamingException ne) {
					if (rejectedPort!=null){
							rejectedPort.writeRecord(inRecord);
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
					xattribs.getString(XML_LDAPURL_ATTRIBUTE),
					action,
					xattribs.getString(XML_USER_ATTRIBUTE),
					xattribs.getString(XML_PASSWORD_ATTRIBUTE));
		} else {
			aSimpleLdapWriter = new LdapWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getString(XML_LDAPURL_ATTRIBUTE),
					action);
			
		}
		if (xattribs.exists(XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE)) {
			aSimpleLdapWriter.setMultiValueSeparator(xattribs.getString(XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE));
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
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
    }
	
	/**
	 * return the type (identifier) of the
	 * component
	 */
	@Override
	public String getType(){
		return COMPONENT_TYPE;
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
		this.formatter.close();
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

}
