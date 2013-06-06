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


import java.util.List;

import javax.naming.directory.SearchControls;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

import com.linagora.ldap.LdapManager.AliasHandling;
import com.linagora.ldap.LdapManager.ReferralHandling;
import com.linagora.ldap.LdapParser;

/**
 * <h3>LdapReader Component</h3>
 *
 * <!-- This class is intended to provide a mean to read information 
 * from an LDAP directory. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>LdapReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Provides the logic to extract search result of an LDAP directory
 * and transform them into Jetel Data Records.
 * <br>The metadata provided throuh output port/edge must precisely 
 * describe the structure of read object.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>0</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- output records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>Results of the search must have the same objectClass</td></tr>
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
 * <h4>Example (public LDAP):</h4>
 * <pre>&lt;id="INPUT1" type="LDAP_READER" ldapUrl="ldap://ldap.uninett.no:389/" base="ou=people,dc=uninett,dc=no" filter="uid=*" scope="SUBTREE"&gt;</pre>
 *
 * <h4>Example:</h4>
 * <pre>&lt;id="INPUT1" type="LDAP_READER" ldapUrl="ldap://foobar.com:389/" base="ou=people,dc=foo,dc=bar" filter="uid=*" scope="subtree" user="uid=Manager,dc=foo,dc=bar" password="manager_pass" /&gt;</pre>
 * 
 * 
 * @author Francois Armand - Linagora
 * @since august 2006
 */
public class LdapReader extends Node {

	/*
	 * Things read from the xml .grf file
	 */
	private static final String XML_FILTER_ATTRIBUTE = "filter";
	private static final String XML_BASE_ATTRIBUTE = "base";
	private static final String XML_SCOPE_ATTRIBUTE = "scope";
	private static final String XML_LDAPURL_ATTRIBUTE = "ldapUrl";
	private static final String XML_USER_ATTRIBUTE = "user";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	private static final String XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE = "multiValueSeparator";
	private static final String XML_ALIAS_HANDLING_ATTRIBUTE = "aliasHandling";
	private static final String XML_REFERRAL_HANDLING_ATTRIBUTE = "referralHandling";

	/**
	 * Component type
	 */
	public final static String COMPONENT_TYPE = "LDAP_READER";
	
	
	
	/**
	 * Readers haven't any input port
	 * TODO: how to provide several output port ?
	 */
	private final static int OUTPUT_PORT = 0;
	
	/**
	 * The LDAP parser connected to the directory
	 */
	private LdapParser parser = null;
	private String base;
	private String filter;
	private int scope;
	private String ldapUrl;
	private String user;
	private String passwd;
	/** This string is used as a multi-value separator. 
	 *  One jetel field can contain multiple values separated by this string. */
	private String multiValueSeparator = "|";
	
	private AliasHandling aliasHandling;
	private ReferralHandling referralHandling;

	/**
	 * A logger for the class
	 */
	private static Log logger = LogFactory.getLog(LdapReader.class);

	/**
	 * Autofilling
	 */
    private AutoFilling autoFilling = new AutoFilling();

	/**
	 * Default constructor
	 * @param id of the object in graph
	 * @param ldapUrl to connect to
	 * @param base ldap base 
	 * @param filter filter to use in search request
	 * @param scope of the research TODO : explain the meaning of possible values
	 */
	public LdapReader(String id, String ldapUrl, String base, String filter, int scope) {
		super(id);
		this.base = base;
		this.ldapUrl = ldapUrl;
		this.filter = filter;
		this.scope = scope;
	}

	
	/**
	 * Constructor with authentification params
	 * @param id of the object in graph
	 * @param ldapUrl to connect to
	 * @param base ldap base 
	 * @param filter filter to use in search request
	 * @param scope of the research TODO : explain the meaning of possible values
	 * @param user the user dn to log on as
	 * @param passwd user's password
	 */
	public LdapReader(String id, String ldapUrl, String base, String filter, 
			int scope, String user, String passwd) {
		this(id, ldapUrl, base, filter, scope);
		this.user = user;
		this.passwd = passwd;
	}
	
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		if(this.user != null) {
			this.parser = new LdapParser(getOutputPort(OUTPUT_PORT).getMetadata(), this.ldapUrl, this.base, this.filter, this.scope, this.user, this.passwd);
		} else {
			this.parser = new LdapParser(getOutputPort(OUTPUT_PORT).getMetadata(), this.ldapUrl, this.base, this.filter, this.scope);
		}	
		parser.setMultiValueSeparator(multiValueSeparator);
		parser.setAliasHandling(aliasHandling);
		parser.setReferralHandling(referralHandling);
		
		/*
		 * TODO : well... I don't know how to add LdapConnection node to transformation graphe.
		 * But it will be better if LdapConnection node exist, as it is for DB connection.
		 * For now, there is a connection by parser.
		 */
//		parser.open(getGraph().getLdapConnection(this.ldapConnectionId), getOutputPort(OUTPUT_PORT).getMetadata());
//		LdapConnection ldapCon = new LdapConnection("id_ldapCon",ldapUrl,"","");
		parser.init();
		/*
		 * Well... some other things to do ?
		 */
		List<DataRecordMetadata> lDataRecordMetadata;
    	if ((lDataRecordMetadata = getOutMetadata()).size() > 0) {
    		autoFilling.addAutoFillingFields(lDataRecordMetadata.get(0));
    	}
		autoFilling.setFilename(ldapUrl + ": " + base + ": " + filter);
	}
	
	@Override
	public Result execute() throws Exception {
		// we need to create data record - take the metadata from first output port
		DataRecord record = DataRecordFactory.newRecord(this.getOutputPort(OUTPUT_PORT).getMetadata());
		record.init();
		try {
			// till it reaches end of data or it is stopped from outside
			while ((record = parser.getNext(record)) != null && runIt) {
				//set autofilling fields
		        autoFilling.setAutoFillingFields(record);
		        
				//broadcast the record to all connected Edges
				this.writeRecordBroadcast(record);
			}
		} catch (Exception e) {
			throw e;
		}finally{
			// we are done, close all connected output ports to indicate end of stream
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * Validate the config
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 0, 0);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);

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

	/*
	 * Isn't this function mandatory ??
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		LdapReader aLdapReader = null;
		int i_scope = SearchControls.OBJECT_SCOPE;
		
		String scope = xattribs.getString(XML_SCOPE_ATTRIBUTE, null);
		String sMsg = null;
		if (scope == null) {
			sMsg = "Missing scope specification";
		} else if(scope.equalsIgnoreCase("OBJECT")) {
			// i_scope = SearchControls.OBJECT_SCOPE;	// default value
		} else if(scope.equalsIgnoreCase("ONELEVEL")) {
			i_scope = SearchControls.ONELEVEL_SCOPE;
		} else if(scope.equalsIgnoreCase("SUBTREE")) {
			i_scope = SearchControls.SUBTREE_SCOPE;
		} else {
			sMsg = "Invalid scope specification \"" + scope + "\"";
		}
		if (sMsg != null) {
			StringBuffer msg = new StringBuffer();
			
			msg.append(sMsg);
			msg.append(" in component ").append(xattribs.getString(Node.XML_ID_ATTRIBUTE, "unknown ID"));
			msg.append("; defaulting to scope \"OBJECT\"");
			logger.warn(msg.toString());
		}

		if(xattribs.exists(XML_USER_ATTRIBUTE) && xattribs.exists(XML_PASSWORD_ATTRIBUTE) ) {
			aLdapReader = new LdapReader(
					xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getStringEx(XML_LDAPURL_ATTRIBUTE, RefResFlag.URL),
					xattribs.getString(XML_BASE_ATTRIBUTE),
					xattribs.getString(XML_FILTER_ATTRIBUTE),
					i_scope,
					xattribs.getString(XML_USER_ATTRIBUTE),
					xattribs.getStringEx(XML_PASSWORD_ATTRIBUTE, RefResFlag.SECURE_PARAMATERS));
		} else {
			aLdapReader = new LdapReader(
					xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getStringEx(XML_LDAPURL_ATTRIBUTE, RefResFlag.URL),
					xattribs.getString(XML_BASE_ATTRIBUTE),
					xattribs.getString(XML_FILTER_ATTRIBUTE),
					i_scope);
		}
		if (xattribs.exists(XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE)) {
			aLdapReader.setMultiValueSeparator(xattribs.getString(XML_MULTI_VALUE_SEPARATOR_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ALIAS_HANDLING_ATTRIBUTE)) {
			aLdapReader.setAliasHandling(Enum.valueOf(AliasHandling.class, xattribs.getString(XML_ALIAS_HANDLING_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_REFERRAL_HANDLING_ATTRIBUTE)) {
			aLdapReader.setReferralHandling(Enum.valueOf(ReferralHandling.class, xattribs.getString(XML_REFERRAL_HANDLING_ATTRIBUTE)));
		}
		
		return aLdapReader;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		parser.reset();
		autoFilling.reset();
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	public synchronized void free() {
		super.free();
		parser.close();
	}

	public String getMultiValueSeparator() {
		return multiValueSeparator;
	}

	public void setMultiValueSeparator(String multiValueSeparator) {
		if (!StringUtils.isEmpty(multiValueSeparator) && !multiValueSeparator.equals(LdapWriter.NONE_MULTI_VALUE_SEPARATOR)) {
			this.multiValueSeparator = multiValueSeparator;
		} else {
			this.multiValueSeparator = null;
		}
	}

	public void setAliasHandling(AliasHandling aliasHandling) {
		this.aliasHandling = aliasHandling;
	}

	public void setReferralHandling(ReferralHandling referralHandling) {
		this.referralHandling = referralHandling;
	}

}
