/* 
*    This file is part of jETeL/Clover - Java based ETL 
*    application framework, Copyright (C) 2002-06  
*    David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation
*    version 2.1 of the License.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*    Copyright (C) 2006 
*    Linagora <http://linagora.com>
*    Francois Armand <farmand@linagora.com>
*/

package com.linagora.component;

import java.io.IOException;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
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
 *  <tr><td><b>base</b></td><td>Base DN used for the LDAP search</td>
 *  <tr><td><b>filter</b></td><td>Filter used for the LDAP connection</td>
 *  <tr><td><b>scope</b><br></td>Scope of the search request, must be one of <b>object</b>, <b>onelevel</b> or <b>subtree</b><td></td>
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
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() != 1) {
			throw new ComponentNotReadyException("One and only on input port has to be defined!");
		}

		this.formatter = new LdapFormatter(this.ldapUrl, this.action, this.user, this.passwd);
		
		// based on file mask, create/open output file
		try {
			formatter.open(null, getInputPort(READ_FROM_PORT).getMetadata());
		}
		catch (Exception ex) {
			if(logger.isDebugEnabled()) {
				ex.printStackTrace();
			}
			throw new ComponentNotReadyException(getId() + "Error: " + ex.getMessage());
		}

	}
	

	
	/**
	 *  Main processing method 
	 */
	public void run() {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort rejectedPort=getOutputPort(WRITE_REJECTED_TO_PORT);

		DataRecord inRecord = new DataRecord(inPort.getMetadata());
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
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
		} catch (Exception ex) {
			ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
		} finally {
			closeAllOutputPorts();
			this.formatter.close();
			broadcastEOF();
			if (resultMsg == null) {
				if (runIt) {
					resultMsg = "OK";
				} else {
					resultMsg = "STOPPED";
				}
				resultCode = Node.RESULT_OK;
			}
		}
		return;
	}

	/**
	 *  This method enable to write the xml description 
	 *  matching the component.
	 *  @param xmlElement the xmlElement to write to.
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_LDAPURL_ATTRIBUTE,this.ldapUrl);
		//add the action
		String xml_action_attribute = null;
		switch (this.action) {
		case LdapFormatter.ADD_ENTRY:
			xml_action_attribute = LdapWriter.XML_ADD_ENTRY_VALUE;
			break;
		case LdapFormatter.REMOVE_ENTRY:
			xml_action_attribute = LdapWriter.XML_REMOVE_ENTRY_VALUE;
			break;
		case LdapFormatter.REPLACE_ATTRIBUTES:
			xml_action_attribute = LdapWriter.XML_REPLACE_ATTRIBUTES_VALUE;
			break;
		case LdapFormatter.REMOVE_ATTRIBUTES:
			xml_action_attribute = LdapWriter.XML_REMOVE_ATTRIBUTES_VALUE;
			break;
		default: // TODO replace by default ?
			xml_action_attribute = LdapWriter.XML_REPLACE_ATTRIBUTES_VALUE;
			break;
		}
		xmlElement.setAttribute(XML_ACTION_ATTRIBUTE,xml_action_attribute);
		if(null != this.user && null != this.passwd) {
			xmlElement.setAttribute(XML_USER_ATTRIBUTE,this.user);
			xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE,this.passwd);
		}
	} 

	
	/**
	 *  This method read from an xml tree the description of
	 *  the component and initialize it according to the
	 *  configuration.
	 *  @param graph
	 *  @param nodeXML
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs= new ComponentXMLAttributes(nodeXML, graph);
		LdapWriter aSimpleLdapWriter = null;
		try{
			int action = 0;
			String action_value = xattribs.getString(XML_ACTION_ATTRIBUTE, null);
			if(action_value.equalsIgnoreCase(XML_ADD_ENTRY_VALUE)) {
				action = LdapFormatter.ADD_ENTRY;
			} else if(action_value.equalsIgnoreCase(XML_REMOVE_ENTRY_VALUE)) {
				action = LdapFormatter.REMOVE_ENTRY;
			} else if(action_value.equalsIgnoreCase(XML_REPLACE_ATTRIBUTES_VALUE)) {
				action = LdapFormatter.REPLACE_ATTRIBUTES;
			} else if(action_value.equalsIgnoreCase(XML_REMOVE_ATTRIBUTES_VALUE)) {
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
		} catch(Exception ex){
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(), ex);
		}
		
		return aSimpleLdapWriter;
	}

	
	public boolean checkConfig(){
		return true;
	}
	
	/**
	 * return the type (identifier) of the
	 * component
	 */
	public String getType(){
		return COMPONENT_TYPE;
	}
	
}
