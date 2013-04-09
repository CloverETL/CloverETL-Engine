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

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LdapManager {

	private static final String DEFAULT_CTX = "com.sun.jndi.ldap.LdapCtxFactory";
	private static final String DEREF_ALIASES_ENV_PROPERTY = "java.naming.ldap.derefAliases";

	/** How to handle ldap referrals if unspecified */
	public static final String DEFAULT_REFERRAL_HANDLING = ReferralHandling.IGNORE.name().toLowerCase();
	
	/** @see http://download.oracle.com/javase/jndi/tutorial/ldap/referral/jndi.html */
	public enum ReferralHandling {
		IGNORE,
		FOLLOW,
		THROW;
	}

	/** How to handle ldap aliases if unspecified */
	public static final String DEFAULT_ALIAS_HANDLING = AliasHandling.FINDING.name().toLowerCase();

	/** @see http://download.oracle.com/javase/jndi/tutorial/ldap/misc/aliases.html */
	public enum AliasHandling {
		ALWAYS,
		NEVER,
		FINDING,
		SEARCHING;
	}

	/** Default time limit on search, o means no limit */
	private static final int DEFAULT_SEARCH_TIMEOUT = 0;

	/** Default max number of result returned, 0 means no limit */
	private static final int DEFAULT_SEARCH_LIMIT = 0;
	
	/**
	 * To speed up existance checks, we use a single static constraints object that
	 * never changes.
	 */
	private static final SearchControls existanceSearchControl;
	//static to speed up search
	static {
		existanceSearchControl = new SearchControls();
		existanceSearchControl.setSearchScope(SearchControls.OBJECT_SCOPE);
		existanceSearchControl.setCountLimit(0);
		existanceSearchControl.setTimeLimit(0);
		existanceSearchControl.setReturningAttributes(new String[] { "1.1" });
	}

	/**
	 * this is the environment to pass to the context to open
	 * the connection. 
	 */
	private Hashtable env = null;

	/**
	 * Ldap context : connection
	 */
	private DirContext ctx = null;

	private static Log logger = LogFactory.getLog(LdapManager.class);

	/**
	 * This prepare a jndi connection with a particular set of environment properties.
	 * At this point, the connection is not created, you have to call
	 * openContext to actually create the jndi connection.
	 */
	public LdapManager(Hashtable env) throws NamingException {
		this.env = env;
	}

	/**
	 * This prepares the creation of a simple, 
	 * unauthenticated jndi connection to an ldap url.
	 * Basic properties are set.
	 * Note that you have to call openContext to really create the connection.
	 *
	 * @param url a url of the form ldap://hostname:portnumber.
	 */
	public LdapManager(String url) {
		this.env = new Hashtable(); // an environment for jndi context parameters
		this.setBasicProperties(url); // set up the bare minimum parameters
	}

	/**
	 * This creates a LdapManager object using simple username + password authentication.
	 * @param url    a url of the form ldap://hostname:portnumber.
	 * @param userDN the Manager User's distinguished name (optionally null if not used).
	 * @param pwd    the Manager User's password - (is null if user is not manager).
	 */
	public LdapManager(String url, String userDN, String pwd) {
		this(url);
		setupSimpleSecurityProperties(userDN, pwd); // add the username + password parameters
	}

	public void setReferralHandling(ReferralHandling handling) {
		if (handling != null) {
			env.put(Context.REFERRAL, handling.name().toLowerCase());
		} else {
			env.remove(Context.REFERRAL); // use default -- determined by the service provider
		}
	}
	
	public void setAliasHandling(AliasHandling handling) {
		if (handling != null) {
			env.put(DEREF_ALIASES_ENV_PROPERTY, handling.name().toLowerCase());
		} else {
			env.remove(DEREF_ALIASES_ENV_PROPERTY); // same as "always"
		}
	}
	
	
	/**
	 * This sets the basic environment properties needed for a simple,
	 * unauthenticated jndi connection.  It is used by openBasicContext().
	 * @param url
	 * @param tracing
	 * @param referralType could be: follow, ignore, throw 
	 * @param aliasHandle could be: finding, searching, etc.
	 * @throws NamingException
	 */
	public void setupBasicProperties(String url, boolean tracing,
			String referralType, String aliasHandle) {
		if (tracing)
			this.env.put("com.sun.jndi.ldap.trace.ber", System.err);
		this.env.put("java.naming.ldap.version", "3");
		this.env.put(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_CTX);
		this.env.put("java.naming.ldap.deleteRDN", "false"); // by default, do not delete RDN
		this.env.put(Context.SECURITY_AUTHENTICATION, "none"); // use setSimpleAuthentification to modify that

		this.env.put(Context.REFERRAL, referralType);
		this.env.put(DEREF_ALIASES_ENV_PROPERTY, aliasHandle);
		// the ldap url to connect to, for instance : "ldap://localhost:389"
		
		/*
		 * TODO How can I normalize the URL ? For example, "ldap://localhost:389/dn=People, ou=foo, ou=bar"
		 * is a legal ldap v3 URL, but I do not want to allow it (homogenity...)
		 * I think it has to be restrict at the input level, for example by asking 
		 * "host", "port", "base dn" and not directly "ldap url"
		 */
		this.env.put(Context.PROVIDER_URL, url);

		this.env.put("java.naming.ldap.attributes.binary",
				"photo jpegphoto jpegPhoto"); // special hack to handle non-standard binary atts, from jxplorer
	}

	/**
	 * @param url ldap url to connect to
	 * @throws NamingException
	 */
	public void setBasicProperties(String url) {
		this.setupBasicProperties(url, false, DEFAULT_REFERRAL_HANDLING,
				DEFAULT_ALIAS_HANDLING);
	}

	/**
	 * This sets the environment properties needed for a simple username +
	 * password authenticated jndi connection.
	 * @param userDN
	 * @param pwd
	 */
	public void setupSimpleSecurityProperties(String userDN, String pwd) {
		this.env.put(Context.SECURITY_AUTHENTICATION, "simple"); // 'simple' = username + password
		this.env.put(Context.SECURITY_PRINCIPAL, userDN); // add the full user dn
		this.env.put(Context.SECURITY_CREDENTIALS, pwd);
	}

	/**
	 * @return a newly created DirContext.
	 */
	public void openContext() throws NamingException {
		/* DEBUG : verify parameters passed to the context */
		/*
		 System.out.println("*** environnement properties ***");
		 for (Enumeration e = env.keys() ; e.hasMoreElements() ;) {
		 String key = e.nextElement().toString();
		 String value = env.get(key).toString();
		 if (value.length() > 80) {
		 value = value.substring(0, 77) + "...";
		 }
		 System.out.println(key + "=" + value);
		 }
		 System.out.println("*****************");
		 */
		this.ctx = new InitialDirContext(this.env);

		if (ctx == null)
			throw new NamingException(
					"Internal Error with jndi connection: No Context was returned, however no exception was reported by jndi.");

	}
	
	/**
	 * Checks the existence of a particular DN
	 *
	 * @param nodeDN the DN to check
	 * @return the existence of the nodeDN (or false if an error occurs).
	 */
	public boolean exists(String nodeDN) throws NamingException {
		try {
			ctx.search(nodeDN, "(objectclass=*)", existanceSearchControl);
			return true;
		} catch (NameNotFoundException e) {//it's *BAD*.
			return false;
		}
	}

	/**
	 * Method that calls the actual search on the jndi context.
	 *
	 * @param searchbase       the domain name (relative to initial context in ldap) to search from.
	 * @param filter           the non-null filter to use for the search
	 * @param scope            the scope level of the search, one off "SearchControls.ONELEVEL_SCOPE
	 * @param limit            the maximum number of results to return
	 * @param timeout          the maximum time to wait before abandoning the search
	 * @param returnAttributes an array of strings containing the names of attributes to search. (null = all, empty array = none)
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration search(String searchbase, String filter,
			String[] returnAttributes, int scope, int limit, int timeout)
			throws NamingException {

		SearchControls constraints = new SearchControls();

		if (SearchControls.ONELEVEL_SCOPE == scope) {
			constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
		} else if (SearchControls.SUBTREE_SCOPE == scope) {
			constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
		} else if (SearchControls.OBJECT_SCOPE == scope) {
			constraints.setSearchScope(SearchControls.OBJECT_SCOPE);
		} else {
			throw new NamingException("Unknown search scope: " + scope);
		}

		if (returnAttributes != null && returnAttributes.length == 0)
			returnAttributes = new String[] { "objectClass" };

		constraints.setCountLimit(limit);
		constraints.setTimeLimit(timeout);

		constraints.setReturningAttributes(returnAttributes);

		NamingEnumeration results = ctx.search(searchbase, filter, constraints);

		return results;

	}

	/**
	 * Convenient search fonction with default limit and timeout
	 * @param searchbase
	 * @param filter
	 * @param returnAttributes
	 * @param scope
	 * @return the result of the search
	 * @throws NamingException
	 */
	public NamingEnumeration search(String searchbase, String filter,
			String[] returnAttributes, int scope) throws NamingException {
		return this.search(searchbase, filter, returnAttributes, scope,
				DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_TIMEOUT);
	}

	/**
	 * Convenient search fonction with default limit, timeout and that return all attributes
	 * @param searchbase
	 * @param filter
	 * @param scope
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration search(String searchbase, String filter, int scope)
			throws NamingException {
		return this.search(searchbase, filter, null, scope,
				DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_TIMEOUT);
	}

	/**
	 * 
	 * @param searchbase
	 * @param filter
	 * @param returnAttributes
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration searchOnelevel(String searchbase, String filter,
			String[] returnAttributes) throws NamingException {
		return this.search(searchbase, filter, returnAttributes,
				SearchControls.ONELEVEL_SCOPE);
	}

	/**
	 * 
	 * @param searchbase
	 * @param filter
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration searchOneLevel(String searchbase, String filter)
			throws NamingException {
		return this.searchOnelevel(searchbase, filter, null);
	}

	/**
	 * 
	 * @param searchbase
	 * @param filter
	 * @param returnAttributes
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration searchsubtree(String searchbase, String filter,
			String[] returnAttributes) throws NamingException {
		return this.search(searchbase, filter, returnAttributes,
				SearchControls.SUBTREE_SCOPE);
	}

	/**
	 * 
	 * @param searchbase
	 * @param filter
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration searchSubtree(String searchbase, String filter)
			throws NamingException {
		return this.searchsubtree(searchbase, filter, null);
	}

	/**
	 * 
	 * @param searchbase
	 * @param filter
	 * @param returnAttributes
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration searchObject(String searchbase, String filter,
			String[] returnAttributes) throws NamingException {
		return this.search(searchbase, filter, returnAttributes,
				SearchControls.OBJECT_SCOPE);
	}

	/**
	 * 
	 * @param searchbase
	 * @param filter
	 * @return
	 * @throws NamingException
	 */
	public NamingEnumeration searchObject(String searchbase, String filter)
			throws NamingException {
		return this.searchObject(searchbase, filter, null);
	}


	/**
	 * Gets all the attribute type and values for the given entry.
	 *
	 * @param dn               the ldap string distinguished name of entry to be read
	 * @param returnAttributes a list of specific attributes to return.
	 * @return an 'Attributes' object containing a list of all Attribute
	 *         objects.
	 */
	public synchronized Attributes getAttributes(String dn, String[] returnAttributes) throws NamingException {
		return ctx.getAttributes(dn, returnAttributes);
	}

	/**
	 * Gets all the attribute type and values for the given entry.
	 *
	 * @param dn the ldap string distinguished name of entry to be read
	 * @return an 'Attributes' object containing a list of all Attribute
	 *         objects.
	 */
	public synchronized Attributes getAttributes(String dn) throws NamingException {
		return getAttributes(dn, null);
	}

	public void close() throws NamingException {
		if (ctx == null) return;
//		nameParser = null;
		ctx.close();		
	}


	/**
	 * Modifies an object's attributes, either adding, replacing or
	 * deleting the passed attributes.
	 *
	 * @param dn       distinguished name of object to modify
	 * @param mod_type the modification type to be performed; one of
	 *                 DirContext.REPLACE_ATTRIBUTE, DirContext.DELETE_ATTRIBUTE, or
	 *                 DirContext.ADD_ATTRIBUTE.
	 * @param attr     the new attributes to update the object with.
	 */
	public void modifyAttributes(String dn, int mod_type, Attributes attr)
			throws NamingException {
		ctx.modifyAttributes(dn, mod_type, attr);
	}

	/**
	 * Updates an object with a new set of attributes
	 *
	 * @param dn   distinguished name of object to update
	 * @param atts the new attributes to update the object with.
	 */
	public void updateEntry(String dn, Attributes atts) throws NamingException {
		modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, atts);
	}
	
	/**
	 * deletes a set of attribute-s from an object
	 *
	 * @param dn distinguished name of object
	 * @param a  the Attributes object containing the
	 *           list of attribute-s to delete
	 */
	public void deleteAttributes(String dn, Attributes atts) throws NamingException {
		modifyAttributes(dn, DirContext.REMOVE_ATTRIBUTE, atts);
	}
	
	/**
	 * creates a new object (subcontext) with the given
	 * dn and attributes.
	 *
	 * @param dn   the distinguished name of the new object
	 * @param atts attributes for the new object
	 */
	public void addEntry(String dn, Attributes atts) throws NamingException {
		ctx.createSubcontext(dn, atts);
	}

	/**
	 * deletes a leaf entry (subcontext).  It is
	 * an error to attempt to delete an entry which is not a leaf
	 * entry, i.e. which has children.
	 * @param dn
	 */
	public void deleteEntry(String dn) throws NamingException {
		ctx.destroySubcontext(dn);
	}
	
}
