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
package org.jetel.util.property;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.string.StringUtils;

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
    private Matcher regexEscapeMatcher;
	private Properties properties;

	private static final int MAX_RECURSION_DEPTH=10;
    private static final boolean DEFAULT_STRICT_OPTION=true;  // default behaviour is to be strict when referencing missing

	Log logger = LogFactory.getLog(PropertyRefResolver.class);
	
	private boolean resolve=true; //default behaviour is to resolve references
    
	
	/**Constructor for the PropertyRefResolver object */
	public PropertyRefResolver(TransformationGraph graph) {
		properties = graph.getGraphProperties();
		if (properties != null) {
			Pattern pattern = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX);
			regexMatcher = pattern.matcher("");
          //  Pattern pattern2 = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_ESCAPE_REGEX);
          //  regexEscapeMatcher = pattern2.matcher("");
		}
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
           // Pattern pattern2 = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_ESCAPE_REGEX);
           // regexEscapeMatcher = pattern2.matcher("");
		}
	}


	/**
	 * Add all properties (key=value pairs) to existing/internal set
     * of properties
	 *
	 * @param  properties  Properties set to be added
	 */
	public void addProperties(Properties properties) {
		this.properties.putAll(properties);
	}
    
    /**
     * Add all key=value pairs to existing/internal set
     * of properties
     * 
     * @param properties Map with properties definitions
     */
    public void addProperties(Map properties){
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
     * @throws AttributeNotFoundException if referenced property does not exist
	 */
	
	public String resolveRef(String value,boolean strict) {
		if (resolve){
		    StringBuffer strBuf = new StringBuffer(value);
		    resolveRef2(strBuf,strict);
		    return StringUtils.stringToSpecChar(strBuf);
		}else{
		    return value;
		}
	}
    
    public String resolveRef(String value) {
        return resolveRef(value,DEFAULT_STRICT_OPTION);
    }
	
    
	/**
     * Looks for reference to global graph properties within string and
     *  tries to resolve them - replace by the property's value.<br>
     *  The format of reference 'call' is denoted by Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX -
     *  usually in the form ${<i>_property_name_</i>}.
     *  It can handle nested reference - when global property is referencing another property 
     * 
	 * @param value
	 * @return value with all references resolved
	 * @throws AttributeNotFoundException @throws AttributeNotFoundException if referenced property does not exist
	 */
	public boolean resolveRef(StringBuffer value,boolean strict) {
	    if (!resolve) {
            return false;
	    }
        boolean result = true;
	    result =  resolveRef2(value,strict);
	    String tmp = StringUtils.stringToSpecChar(value);
	    value.setLength(0);
	    value.append(tmp);
        
	    return result;
	}
	
    public boolean resolveRef(StringBuffer value) {
        return resolveRef(value,DEFAULT_STRICT_OPTION);
    }
    
    
	/**
	 * Method which Looks for reference to global graph properties within string -
	 * this is the actual implementation. The public version is only a wrapper.
	 * 
	 * @param value	String buffer containing plain text mixed with references to 
	 * global properties - > reference is in form ${<i>..property_name..</i>}
     * @param strict    if True, then references to non-existent properties cause AttributeNotFoundException be thrown
	 * @return true if at least one reference to global property was found and resolved
	 */
	private boolean resolveRef2(StringBuffer value,boolean strict)  {
		String reference;
		String resolvedReference;
		boolean found=false;
		if ((value != null) && (properties != null)) {
			// process references
            regexMatcher.reset(value);
			while (regexMatcher.find()) {
				found=true;
				reference = regexMatcher.group(1);
//				if (logger.isDebugEnabled()) {
//					logger.debug("Reference: "+reference);
//				}
				resolvedReference = properties.getProperty(reference);
				if (resolvedReference == null){
					resolvedReference = System.getenv(reference);
				}
				if (resolvedReference == null){
					reference = reference.replace('_','.');
					resolvedReference = System.getProperty(reference);
				}
				if (resolvedReference == null) {
				    logger.warn("Can't resolve reference to graph property: " + reference);
                   // if (strict)
                   //     throw new AttributeNotFoundException(reference,"can't resolve reference to graph property: " + reference);
				}else{
				    value.replace(regexMatcher.start(),regexMatcher.end(),resolvedReference);
                    regexMatcher.reset(value);
                }
			}
//            // process escape 
//           regexEscapeMatcher.reset(value);
//            while (regexEscapeMatcher.find()) {
//                reference = regexEscapeMatcher.group(1);
//               if (logger.isDebugEnabled()) {
//                    logger.debug("Reference: "+reference);
//                }
//                value.replace(regexEscapeMatcher.start(),regexEscapeMatcher.end(),
//                        value.substring(regexEscapeMatcher.start()+2,regexEscapeMatcher.end()-1));
//                regexEscapeMatcher.reset(value);
//            }
//          
			return found;
		} else {
			return false;
		}
	}
	
	/**
	 * This method replaces all parameters by their values
	 * 
	 * @param properties
	 */
	public void resolveAll(Properties properties){
		for (Entry property : properties.entrySet()) {
			properties.setProperty((String)property.getKey(), resolveRef((String)property.getValue()));
		}
	}

	 
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

