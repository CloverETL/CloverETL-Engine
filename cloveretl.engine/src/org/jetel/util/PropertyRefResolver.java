/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
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
*/
package org.jetel.util;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.graph.TransformationGraph;

/**
 *  Helper class for resolving references within string values to Properties.<br>
 *  The reference is expected in form: ${..property_name..} <br> If property
 *  name is found in properties, then the whole ${...} substring is replaced by the property value.
 *
 * @author      dpavlis
 * @since       12. May 2004
 * @revision    $Revision$
 */
/**
 * @author dpavlis
 * @since  3.8.2004
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class PropertyRefResolver {
	private Matcher regexMatcher;
	private Properties properties;

	private static final int MAX_RECURSION_DEPTH=10;

	Log logger = LogFactory.getLog(PropertyRefResolver.class);
	
	private boolean resolve;
	
	/**Constructor for the PropertyRefResolver object */
	public PropertyRefResolver() {
		properties = TransformationGraph.getReference().getGraphProperties();
		if (properties != null) {
			Pattern pattern = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX);
			regexMatcher = pattern.matcher("");
		}
		resolve=true; //default behaviour is to resolve references
	}


	/**
	 *Constructor for the PropertyRefResolver object
	 *
	 * @param  properties  Description of the Parameter
	 */
	public PropertyRefResolver(Properties properties) {
		this.properties = properties;
		if (this.properties != null) {
			Pattern pattern = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX);
			regexMatcher = pattern.matcher("");
		}
		resolve=true; //default behaviour is to resolve references
	}


	/**
	 *  Adds a feature to the Properties attribute of the PropertyRefResolver object
	 *
	 * @param  properties  The feature to be added to the Properties attribute
	 */
	public void addProperties(Properties properties) {
		this.properties.putAll(properties);
	}


	/**
	 *  Looks for reference to global graph properties within string and
	 *  tries to resolve them - replace by the property's value.<br>
	 *  The format of reference 'call' is denoted by Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX -
	 *  usually in the form ${<i>_property_name_</i>}.
	 *  It can handle nested reference - when global property is referencing another property 
	 *
	 * @param  value  String potentially containing one or more references to property
	 * @return        String with all references resolved
	 * @see           org.jetel.data.Defaults
	 */
	
	public String resolveRef(String value){
		if (resolve){
		    StringBuffer strBuf = new StringBuffer(value);
		    resolveRef2(strBuf);
		    return strBuf.toString();
		}else{
		    return value;
		}
	}
	
	public boolean resolveRef(StringBuffer value){
	    if (resolve){
	      return resolveRef2(value);
	    }else{
	        return true;
	    }
	}
	
	/**
	 * Method which Looks for reference to global graph properties within string -
	 * this is the actual implementation. The public version is only a wrapper.
	 * 
	 * @param value	String buffer containing plain text mixed with references to 
	 * global properties - > reference is in form ${<i>..property_name..</i>}
	 * @return true if at least one reference to global property was found and resolved
	 */
	private boolean resolveRef2(StringBuffer value) {
		String reference;
		String resolvedReference;
		boolean found=false;
		if ((value != null) && (properties != null)) {
			regexMatcher.reset(value);
			while (regexMatcher.find()) {
				found=true;
				reference = regexMatcher.group(1);
				if (logger.isDebugEnabled()) {
					logger.debug("Reference: "+reference);
				}
				resolvedReference = properties.getProperty(reference);
				if (resolvedReference == null) {
				    logger.error("Can't resolve reference to graph property: " + reference);
					throw new RuntimeException("Can't resolve reference to graph property: " + reference);
				}
				value.replace(regexMatcher.start(),regexMatcher.end(),resolvedReference);
				regexMatcher.reset(value);
			}
			return found;
		} else {
			return false;
		}
	}


//	   //Test/Debug code
//	   public static void main(String args[]){
//	   Properties prop=new Properties();
//	   System.out.println("Property name: "+args[0]);
//	   try{
//	   		InputStream inStream = new BufferedInputStream(new FileInputStream(args[0]));
//	   		prop.load(inStream);
//	   }catch(IOException ex){
//	   	ex.printStackTrace();
//	   }
//	   PropertyRefResolver attr=new PropertyRefResolver(prop);
//	   System.out.println("DB driver is: '{${dbDriver}}' ...");
//	   System.out.println(attr.resolveRef("DB driver is: '{${dbDriver}}' ..."));
//	   System.out.println("${user} is user");
//	   System.out.println(attr.resolveRef("${user} is user"));
//	   System.out.println("${user}/${password} is user/password");
//	   System.out.println(attr.resolveRef("${user}/${password} is user/password"));
//	   }
	
	 
    /**
     * Determines whether resolving references is enabled/disabled
     * 
     * @return Returns the resolve value.
     */
    public boolean isResolve() {
        return resolve;
    }
    /**
     * Enables/disables resolving references within string values to Properties
     * 
     * @param resolve true to resolve references
     */
    public void setResolve(boolean resolve) {
        this.resolve = resolve;
    }
}

