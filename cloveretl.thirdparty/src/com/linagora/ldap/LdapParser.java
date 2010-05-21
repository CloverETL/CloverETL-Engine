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

package com.linagora.ldap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.SizeLimitExceededException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

import com.linagora.ldap.Ldap2JetelData.Ldap2JetelByte;
import com.linagora.ldap.Ldap2JetelData.Ldap2JetelString;



/**
 * this class is the interface between data from an LDAP directory 
 * and Clover's data internal representation.
 * It gives common hight level method to parse an LDAP response
 * @author Francois Armand
 * @since september, 2006
 */
public class LdapParser implements Parser {

	/** timeout, in mili second. 0 means wait indefinitely */ 
	public static int TIMEOUT = 0;
	
	/** Max number of result, 0 means no limit */
	public static int LIMIT = 0;
	
	protected IParserExceptionHandler exceptionHandler;

	/** Number of record already broadcaster */
	protected int recordCounter;

	/** the ldap connection and action manager */
	private LdapManager ldapManager;
	
	/** Jetel data description : Datarecord and its metadata. */ 
//	private DataRecord outRecord = null;
	private DataRecordMetadata metadata;


	/** List of DN matching the search filter */
	List dnList = null; 
	Iterator resultDn = null;
	
	/** Transformation object between Attributes  and DataRecord */
	private Ldap2JetelData[] transMap = null;

	/** Hack to manage multivaluated attributes */
	private String multiSeparator = null;

	/** Useful constant to connect to the LDAP server and perform the search */
	private String base;
	private String filter;
	private int scope;
	private String ldapUrl;
	private String user;
	private String pwd;


	private static Log logger = LogFactory.getLog(LdapParser.class);

	/**
	 * Minimum information needed to create a new LdapParser
	 * @param ldapUrl : LDAP connection URL, for instance: ldap://localhost:381/
	 * @param base : LDAP base dn, for instance "ou=linagora,ou=com"
	 * @param filter
	 * @param scope one off  SearchControls.OBJECT_SCOPE,
	 *               SearchControls.ONELEVEL_SCOPE, SearchControls.SUBTREE_SCOPE
	 */
	public LdapParser(String ldapUrl, String base, String filter, int scope) {
		this.base = base;
		this.filter = filter;
		this.ldapUrl = ldapUrl;
		this.scope = scope;
	}

	/**
	 * Constructor to create an authentificated connection to server.
	 * @param ldapUrl
	 * @param base
	 * @param filter
	 * @param scope
	 * @param user
	 * @param pwd
	 */
	public LdapParser(String ldapUrl, String base, String filter, int scope, String user, String pwd) {
		this(ldapUrl, base, filter, scope);
		this.user = user;
		this.pwd = pwd;
	}

	
	/**
	 * This function try to contact the LDAP server and perform the search query.
	 * ComponentNotReadyException exception are raised for each caught exceptions.
	 */
	public void init(DataRecordMetadata metadata) 
		throws ComponentNotReadyException {

		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		this.metadata = metadata;


		/* 
		 * create a new LdapManager to do related actions
		 */
		if(user != null) {
			ldapManager = new LdapManager(ldapUrl,user,pwd);
		} else {
			ldapManager = new LdapManager(ldapUrl);
		}
			
		try {
			ldapManager.openContext();
		} catch (NamingException ne) {
			throw new ComponentNotReadyException("LDAP connection failed.", ne);
		}

		/*
		 * This part has to be improve. We must think about large result set,
		 * and we must find a way to paginate the result, or not load every
		 * DN on memory, or something like that.
		 */
		NamingEnumeration ne = null;
		/*
		 * Search for DN matching filter
		 */
		try {
			ne = ldapManager.search(base,filter,new String[] { "1.1" },scope);
		} catch (NamingException e) {
			throw new ComponentNotReadyException(e);
		}
		
		dnList = new ArrayList();
		int i = 0;
		try {
			while (ne.hasMore()) {
				i++;
				String name = ((SearchResult) ne.next()).getName();
				dnList.add(name + (base.length() != 0 && name.length() != 0 ? "," : "") + base);
			}
		} catch (SizeLimitExceededException e) {
			if( LIMIT == 0 || i < LIMIT) {
				if(logger.isInfoEnabled()) {logger.info(" WARNING ! Ldap Search request reach server size limit !"); };
			} else {
				if(logger.isInfoEnabled()) {logger.info(" WARNING ! Ldap Search request reach client size limit !"); };
			}
		} catch (NamingException e1) {
			throw new ComponentNotReadyException(e1);
		} finally {
			try {
				ne.close();
			} catch (NamingException e) {
				// nothing to do
			}
		}
		
		resultDn = dnList.iterator();
		
		if (transMap == null) {
	        transMap = new Ldap2JetelData[this.metadata.getNumFields()];
	        try {
	        	/*
	        	 * We assume that all search result have the same
	        	 * class hierarchy, so we can take an one of them.
	        	 */
	        	if (dnList.size() != 0) {  	        	
	        		initTransMap((String)dnList.get(0));
	        	}
			} catch (Exception e) {
				throw new ComponentNotReadyException("Bad metadata name in LdapReader component");
			}
		} 
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setReleaseDataSource(boolean releaseInputSource)  {
	}
	
    /* (non-Javadoc)
     * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
     */
    public void setDataSource(Object inputDataSource) {
        throw new UnsupportedOperationException();
    }

	public void close() {
		try {
			ldapManager.close();
		} catch (NamingException e) {
			//nothing to do ?
		}
	}

	/**
	 * Gets the Next attribute of the LdapParser object
	 * 
	 * @return The Next value
	 * @exception JetelException
	 *                Description of Exception
	 * @since August 27, 2006
	 */
	public DataRecord getNext() throws JetelException {
		DataRecord localOutRecord = new DataRecord(metadata);
		localOutRecord.init();
		return getNext(localOutRecord);
	}

	/**
	 *  Returs next data record parsed from input data source or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  SQLException  Description of Exception
	 *@since                   Angust 27, 2006
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
        if(exceptionHandler != null ) {  //use handler only if configured
            while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
                record = parseNext(record);
            }
        }
		return record;
	}

	/**
	 * @param record
	 * @return
	 */
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		/*
		 * This method is intersting. How should I transform an "Attributs" object
		 * to a "DataRecord" one and don't forget to use metadata rules...
		 */
		//simplest case...
		if (resultDn.hasNext() == false) {
				return null;
		}

		Attributes attrs = null;
		String dn = (String) resultDn.next();
		try {
			attrs = ldapManager.getAttributes(dn);
			/*
			 * we have to add the dn as a standard attribute
			 */
			attrs.put("dn", dn);
		} catch (NamingException e) {
			throw new JetelException (
					"Error when trying to get datas from Ldap directory entry :"
					+ dn + " ");
		}

		/*
		 * Now that the transmap is inited, populated its fields.
		 */
		for (int i = 0; i < record.getNumFields(); i++) {
			DataField df = record.getField(i);
			if (df.getMetadata().isAutoFilled()) continue;
			try {
				transMap[i].setField(df,attrs.get(df.getMetadata().getName()));
			} catch (BadDataFormatException bdfe) {
				if (exceptionHandler != null) { //use handler only if configured
					exceptionHandler.populateHandler(getErrorMessage(bdfe
							.getMessage(), recordCounter, i), record, recordCounter,
							i, bdfe.getOffendingValue().toString(), bdfe);
				} else {
					throw new RuntimeException(getErrorMessage(bdfe.getMessage(),
							recordCounter, i));
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex.getClass().getName() + ":"
						+ ex.getMessage());
			}
		}
		
		recordCounter++;
		return record;
	}

	protected void initTransMap(String dn) throws BadDataFormatException {
		/*
		 * TODO : we should have two cases : 
		 * - the first one, we can access the schema. In this case, 
		 *   we can retrieve available attribute from objectclass hierarchy,
		 *   and verify that metadata field name match attribute name ;
		 * - the second, we do not have access to the schema. We can not
		 *   make any assumption about attribute related to the dn.
		 *   We initialize by default transmap to string2jetel.
		 * For now, only the second case is done.
		 */
		for (int i = 0; i < metadata.getNumFields(); i++) {
			DataFieldMetadata dfm = this.metadata.getField(i);

			DataFieldMetadata fieldMetadata = this.metadata.getField(i);
			if (fieldMetadata.isAutoFilled()) continue;
			
			if (fieldMetadata.getType() == DataFieldMetadata.STRING_FIELD) {
				transMap[i] = new Ldap2JetelString(multiSeparator);
			} else if (fieldMetadata.getType() == DataFieldMetadata.BYTE_FIELD
					|| fieldMetadata.getType() == DataFieldMetadata.BYTE_FIELD_COMPRESSED) {
				transMap[i] = new Ldap2JetelByte();
			} else {
				throw new BadDataFormatException("LDAP intialialisation : Field " + dfm.getName()
						+ " has type " + dfm.getType() + " which is not supported." 
						+ "Only String and Byte array types are supported.");
			}
		}
	}

	/**
	 * Assembles error message when exception occures during parsing
	 * 
	 * @param exceptionMessage
	 *            message from exception getMessage() call
	 * @param recNo
	 *            recordNumber
	 * @param fieldNo
	 *            fieldNumber
	 * @return error message
	 * @since September 19, 2002
	 */
	protected String getErrorMessage(String exceptionMessage, int recNo,
			int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}

	public IParserExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
	}

	public void setExceptionHandler(IParserExceptionHandler handler) {
		this.exceptionHandler = handler;
	}

	public int skip(int nRec) throws JetelException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	public void reset() {
		resultDn = dnList.iterator();
		recordCounter = 0;
	}

	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	public void movePosition(Object position) {
		// TODO Auto-generated method stub
		
	}

	public String getMultiValueSeparator() {
		return multiSeparator;
	}

	public void setMultiValueSeparator(String multiValueSeparator) {
		this.multiSeparator = multiValueSeparator;
	}

}
