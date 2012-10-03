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
package org.jetel.util.property;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.CTLExpressionEvaluator;
import org.jetel.interpreter.ParseException;
import org.jetel.util.string.StringUtils;

/**
 * Helper class for evaluation of CTL expressions and resolving of property references within string values. The CTL
 * expressions present in a string value are evaluated first. Property references present in these CTL expressions are
 * resolved before evaluation. Then the property references are resolved. CTL expressions present in the referenced
 * properties are evaluated after resolving. This mechanism enables arbitrary nesting of CTL expressions and property
 * references within the properties.
 * <p>
 * By default, CTL expressions have to be enclosed within back quotes, i.e. <code>`&lt;ctl_expression&gt;`</code>, and
 * property references use the <code>${}</code> notation, i.e. <code>${property_reference}</code>. Escaped back quote,
 * i.e. <code>\`</code>, within CTL expressions produces a single back quote in the result.
 * This behaviour might be altered by modifying the <code>Defaults.GraphProperties.EXPRESSION_PLACEHOLDER_REGEX</code>
 * and <code>Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX</code> configuration properties. Expression evaluation
 * might be disabled by setting the <code>Defaults.GraphProperties.EXPRESSION_EVALUATION_ENABLED</code> configuration
 * property to <code>false</code>.
 * 
 * NOTE: this class is not thread safe
 * 
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * @author Martin Zatopek, Javlin a.s. &lt;martin.zatopek@javlin.eu&gt;
 *
 * @version 12th November 2009
 * @since 12th May 2004
 */
public class PropertyRefResolver {

	/** the character used to quote CTL expressions */
	private static final char EXPRESSION_QUOTE = Defaults.GraphProperties.EXPRESSION_PLACEHOLDER_REGEX.charAt(
			Defaults.GraphProperties.EXPRESSION_PLACEHOLDER_REGEX.length() - 1);

	/** the logger for this class */
	private static final Log logger = LogFactory.getLog(PropertyRefResolver.class);

	/** properties used for resolving property references */
	private final Properties properties;

	/** the regex pattern used to find CTL expressions */
	private final Pattern expressionPattern = Pattern.compile(Defaults.GraphProperties.EXPRESSION_PLACEHOLDER_REGEX);
	/** the regex pattern used to find property references */
	private static final Pattern propertyPattern = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX);

	/** the CTL expression evaluator used to evaluate CTL expressions */
	private final CTLExpressionEvaluator expressionEvaluator = new CTLExpressionEvaluator();

	/** the set (same errors need to be listed once only) of errors that occurred during evaluation of a single string */
	private final Set<String> errorMessages = new HashSet<String>();

	/** counter for recursion depth - it is necessary to avoid infinite loop of recursion (see #4854) */
	private int recursionDepth;
	
	/** the flag specifying whether the CTL expressions should be evaluated and the property references resolved */
	private boolean resolve = true;

	/**
	 * Constructs a <code>PropertyRefResolver</code> with empty properties. By default, the CTL expressions will
	 * be evaluated and the property references will be resolved. This may be changed by setting the resolve flag
	 * to <code>false</code>.
	 */
	public PropertyRefResolver() {
		this.properties = new Properties();
	}

	/**
	 * Constructs a <code>PropertyRefResolver</code> for the given properties. By default, the CTL expressions will
	 * be evaluated and the property references will be resolved. This may be changed by setting the resolve flag
	 * to <code>false</code>.
	 *
	 * @param properties properties to be used for resolving references
	 */
	public PropertyRefResolver(Properties properties) {
		this.properties = (properties != null) ? properties : new Properties();
	}

	/**
	 * Constructs a <code>PropertyRefResolver</code> for properties of the given graph. By default, the CTL expressions
	 * will be evaluated and the property references will be resolved. This may be changed by setting the resolve flag
	 * to <code>false</code>.
	 *
	 * @param graph a graph with properties to be used for resolving references
	 * @deprecated call new PropertyRefResolver(graph.getGraphProperties()) instead
	 */
	@Deprecated
	public PropertyRefResolver(TransformationGraph graph) {
		this.properties = (graph != null) ? graph.getGraphProperties() : new Properties();
	}
	
	/**
	 * @return properties used for resolving property references
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Adds the given properties (key=value pairs) to the internal set of properties.
	 *
	 * @param properties properties to be added
	 */
	public void addProperties(Properties properties) {
		this.properties.putAll(properties);
	}

    /**
	 * Adds the given map of properties (key=value pairs) to the internal set of properties.
	 *
	 * @param properties a map of properties to be added
     */
    public void addProperties(Map<Object, Object> properties){
        this.properties.putAll(properties);
    }
    
    public void addProperty(String propertyName, String propertyValue) {
    	String valueTmp = this.properties.getProperty(propertyName);
    	if(valueTmp == null) {
    		this.properties.setProperty(propertyName, propertyValue);
    	}
    	
    }

	/**
	 * Sets the flag specifying whether the CTL expressions should be evaluated and the property references resolved.
	 *
	 * @param resolve new flag value
	 */
	public void setResolve(boolean resolve) {
		this.resolve = resolve;
	}

	/**
	 * @return <code>true</code> if the CTL expressions will be evaluated and the property references will be resolved,
	 * <code>false</code> otherwise
	 */
	public boolean isResolve() {
		return resolve;
	}

	/**
	 * If resolving is enabled, evaluates CTL expressions and resolves property references in the given string. After
	 * that, special characters are resolved. Does nothing if resolving is disabled.
	 *
	 * @param value a string containing CTL expressions and property references
	 *
	 * @return the value with CTL expressions evaluated and property references resolved or the same value if resolving
	 * is disabled
	 */
	public String resolveRef(String value) {
		return resolveRef(value, RefResFlag.REGULAR);
	}

	/**
	 * If resolving is enabled, evaluates CTL expressions and resolves property references in the given string. After
	 * that, special characters are resolved only if requested. Does nothing if resolving is disabled.
	 *
	 * @param value a string containing CTL expressions and property references
	 * @param resolveSpecChars flag specifying whether special characters within string values should be resolved and
	 * CTL expressions within the global scope should be evaluated
	 *
	 * @return the value with CTL expressions evaluated and property references resolved or the same value if resolving
	 * is disabled
	 * @deprecated to turn off the special characters resolving call resolveRef(value, RefResOption.ALL_OFF) instead
	 */
	@Deprecated
	public String resolveRef(String value, boolean resolveSpecChars) {
		if (!resolveSpecChars) {
			return resolveRef(value, RefResFlag.ALL_OFF);
		} else {
			return resolveRef(value);
		}
	}

	/**
	 * If resolving is enabled, CTL expressions and special characters resolving is dedicated
	 * by options parameter. Does nothing if resolving is disabled.
	 *
	 * @param value a string containing CTL expressions and property references
	 * @param flag flag specifying whether special characters within string values should be resolved and
	 * CTL expressions within the global scope should be evaluated
	 *
	 * @return the value with CTL expressions evaluated and property references resolved or the same value if resolving
	 * is disabled
	 */
	public String resolveRef(String value, RefResFlag flag) {
		if (value == null || !resolve) {
			return value;
		}

		StringBuilder valueBuffer = new StringBuilder(value);
		try {
			resolveRef(valueBuffer, flag);
		} catch (JetelRuntimeException e) {
			errorMessages.add(e.getMessage() + " " + value); //$NON-NLS-1$
			logger.warn(e.getMessage() + " " + value); //$NON-NLS-1$
		}
		
		return valueBuffer.toString();
	}
	
	/**
	 * If resolving is enabled, evaluates CTL expressions and resolves property references present in the given string
	 * buffer. After that, special characters are resolved. The result is then stored back into the given string buffer.
	 * Does nothing if resolving is disabled.
	 *
	 * @param value a string buffer containing CTL expressions and property references
	 * @param flag flag specifying whether special characters within string values should be resolved and
	 * CTL expressions within the global scope should be evaluated
	 *
	 * @return <code>true</code> if resolving is enabled and at least one CTL expression or property reference was found
	 * and evaluated/resolved, <code>false</code> otherwise
	 */
	private boolean resolveRef(StringBuilder value, RefResFlag flag) {
		// clear error messages before doing anything else
		errorMessages.clear();
		// let's start the recursion depth measuring
		recursionDepth = 0;
		
		if (value == null || !resolve) {
			return false;
		}

		if (flag == null) {
			flag = RefResFlag.REGULAR;
		}
		
		//
		// evaluate CTL expressions and resolve remaining property references
		//

		boolean valueModified = false;

		if (flag.resolveCTLstatements()) {
			valueModified |= evaluateExpressions(value);
		}

		valueModified |= resolveReferences(value, flag.resolveCTLstatements());

		//
		// resolve special characters if desired
		//

		if (flag.resolveSpecCharacters()) {
			String resolvedValue = StringUtils.stringToSpecChar(value);

			value.setLength(0);
			value.append(resolvedValue);
		}

		return valueModified;
	}

	/**
	 * Finds and evaluates CTL expressions present in the given string buffer. Property references present in the CTL
	 * expressions are resolved before evaluation. The result is stored back in the given string buffer.
	 *
	 * @param value a string buffer containing CTL expressions
	 *
	 * @return <code>true</code> if at least one CTL expression was found and evaluated, <code>false</code> otherwise
	 */
	private boolean evaluateExpressions(StringBuilder value) {
		if (!Defaults.GraphProperties.EXPRESSION_EVALUATION_ENABLED) {
			return false;
		}

		boolean anyExpressionEvaluated = false;
		Matcher expressionMatcher = expressionPattern.matcher(value);

		while (expressionMatcher.find()) {
			//aren't we too deep in recursion?
			if (isRecursionOverflowed()) {
				throw new JetelRuntimeException(PropertyMessages.getString("PropertyRefResolver_infinite_recursion_warning")); //$NON-NLS-1$
			}

			String expression = expressionMatcher.group(1);

			if (StringUtils.isEmpty(expression)) {
				// no evaluation is necessary in case of empty CTL expressions
				value.delete(expressionMatcher.start(), expressionMatcher.end());
				expressionMatcher.region(expressionMatcher.start(), value.length());
			} else {
				// resolve property references that might be present in the CTL expression
				StringBuilder resolvedExpression = new StringBuilder(expression);
				resolveReferences(resolvedExpression, true);

				// make sure that expression quotes are unescaped before evaluation of the CTL expression
				StringUtils.unescapeCharacters(resolvedExpression, EXPRESSION_QUOTE);

				try {
					// finally evaluate the CTL expression
					String evaluatedExpression = expressionEvaluator.evaluate(resolvedExpression.toString());

					// update the expression matcher so that find() starts at the correct index
					value.replace(expressionMatcher.start(), expressionMatcher.end(), evaluatedExpression);
					expressionMatcher.region(expressionMatcher.start() + evaluatedExpression.length(), value.length());

					anyExpressionEvaluated = true;
				} catch (ParseException exception) {
					errorMessages.add(MessageFormat.format(PropertyMessages.getString("PropertyRefResolver_invalid_ctl_warning"), resolvedExpression)); //$NON-NLS-1$
					logger.warn(MessageFormat.format(PropertyMessages.getString("PropertyRefResolver_evaluation_failed_warning"), resolvedExpression), exception); //$NON-NLS-1$
				}
			}
		}

		return anyExpressionEvaluated;
	}

	/**
	 * Finds and resolves property references present in the given string buffer. CTL expressions present in the
	 * referenced properties can be evaluated after resolving. The result is stored back in the given string buffer.
	 *
	 * @param value a string buffer containing property references
	 * @param resolveCTLstatements True to resolve CTL statements.
	 *
	 * @return <code>true</code> if at least one property reference was found and resolved, <code>false</code> otherwise
	 */
	private boolean resolveReferences(StringBuilder value, boolean resolveCTLstatements) {
		boolean anyReferenceResolved = false;
		Matcher propertyMatcher = propertyPattern.matcher(value);

		while (propertyMatcher.find()) {
			//aren't we too deep in recursion?
			if (isRecursionOverflowed()) {
				throw new JetelRuntimeException(PropertyMessages.getString("PropertyRefResolver_infinite_recursion_warning")); //$NON-NLS-1$
			}

			// resolve the property reference
			String reference = propertyMatcher.group(1);
			String resolvedReference = properties.getProperty(reference);
			
			if (resolvedReference == null) {
				resolvedReference = System.getenv(reference);
			}
			
			// find properties with '.' and '_' among system properties. If both found, use '.' for backwards compatibility
			if (resolvedReference == null) {
				String preferredReference = reference.replace('_', '.');
				resolvedReference = System.getProperty(preferredReference);
				if (resolvedReference == null) {
					resolvedReference = System.getProperty(reference);
				} else {
					if (!reference.equals(preferredReference) && System.getProperty(reference) != null) {
						logger.warn(new String(MessageFormat.format(PropertyMessages.getString("PropertyRefResolver_preferred_substitution_warning"), reference))); //$NON-NLS-1$
					}
				}
			}

			if (resolvedReference != null) {
				// evaluate the CTL expression that might be present in the property
				StringBuilder evaluatedReference = new StringBuilder(resolvedReference);
				if (resolveCTLstatements) {
					evaluateExpressions(evaluatedReference);
				}
				value.replace(propertyMatcher.start(), propertyMatcher.end(), evaluatedReference.toString());
				propertyMatcher.reset(value);
				if (resolveCTLstatements) {
					anyReferenceResolved = true;
				}
			} else {
				errorMessages.add(MessageFormat.format(PropertyMessages.getString("PropertyRefResolver_property_not_defined_warning"), reference)); //$NON-NLS-1$
				//this warn is turned off since this warning can disturb console log even in case everything is correct
				//see TypedProperties.resolvePropertyReferences() method where a local PropertyRefResolver is used
				//for initial parameters resolution and after that a global PropertyRefResolver is used for unresolved
				//parameters, there is expected that the first initial resolution does not handle all parameters
				//and no warning are desired
				//for detail error reporting PropertyRefResolver.getErrorMessages() method should be used
				//logger.warn("Cannot resolve reference to property: " + reference);
			}
		}

		return anyReferenceResolved;
	}

	/**
	 * Test whether the resolving recursion is not too deep.
	 */
	private boolean isRecursionOverflowed() {
		return recursionDepth++ > Defaults.GraphProperties.PROPERTY_ALLOWED_RECURSION_DEPTH;
	}

	/**
	 * @return <code>true</code> if any error occurred during the last call to the
	 * {@link #resolveRef(StringBuilder, RefResFlag)} method, <code>false</code> otherwise
	 */
	public boolean anyErrorOccured() {
		return !errorMessages.isEmpty();
	}

	/**
	 * @return a read-only list of error messages collected during the last call to the
	 * {@link #resolveRef(StringBuilder, RefResFlag)} method
	 */
	public Set<String> getErrorMessages() {
		return Collections.unmodifiableSet(errorMessages);
	}

	/**
	 * Evaluates CTL expressions and resolves property references present in each property in the given properties.
	 * The result is then stored back into the given properties.
	 *
	 * @param properties properties to be resolved
	 */
	public void resolveAll(Properties properties) {
		for (Entry<Object, Object> property : properties.entrySet()) {
			properties.setProperty((String) property.getKey(), resolveRef((String) property.getValue()));
		}
	}

	/**
	 * Evaluates CTL expressions and resolves property references present in each property in the given properties.
	 * The result is then stored back into the given properties.
	 * 
	 * @param properties
	 * @param refResFlag
	 */
	public void resolveAll(Properties properties, RefResFlag refResFlag) {
		for (Entry<Object, Object> property : properties.entrySet()) {
			properties.setProperty((String) property.getKey(), resolveRef((String) property.getValue(), refResFlag));
		}
	}

	/**
	 * Indicates if given string contains also property reference (that needs to be de-referenced)
	 * @param value value to inspect
	 * @return <code>true</code> if value contains reference to at least one property.
	 */
	public static boolean containsProperty(String value){
		return propertyPattern.matcher(value).find();
	}
}

