/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.util.property;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
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
	private final Pattern propertyPattern = Pattern.compile(Defaults.GraphProperties.PROPERTY_PLACEHOLDER_REGEX);

	/** the CTL expression evaluator used to evaluate CTL expressions */
	private final CTLExpressionEvaluator expressionEvaluator = new CTLExpressionEvaluator();

	/** the set (same errors need to be listed once only) of errors that occurred during evaluation of a single string */
	private final Set<String> errorMessages = new HashSet<String>();

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

		StringBuffer valueBuffer = new StringBuffer(value);
		resolveRef(valueBuffer, flag);

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
	private boolean resolveRef(StringBuffer value, RefResFlag flag) {
		// clear error messages before doing anything else
		errorMessages.clear();

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

		valueModified |= resolveReferences(value);

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
	private boolean evaluateExpressions(StringBuffer value) {
		if (!Defaults.GraphProperties.EXPRESSION_EVALUATION_ENABLED) {
			return false;
		}

		boolean anyExpressionEvaluated = false;
		Matcher expressionMatcher = expressionPattern.matcher(value);

		while (expressionMatcher.find()) {
			String expression = expressionMatcher.group(1);

			if (StringUtils.isEmpty(expression)) {
				// no evaluation is necessary in case of empty CTL expressions
				value.delete(expressionMatcher.start(), expressionMatcher.end());
				expressionMatcher.region(expressionMatcher.start(), value.length());
			} else {
				// resolve property references that might be present in the CTL expression
				StringBuffer resolvedExpression = new StringBuffer(expression);
				resolveReferences(resolvedExpression);

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
					errorMessages.add("CTL expression '" + resolvedExpression + "' is not valid.");
					logger.warn("Cannot evaluate expression: " + resolvedExpression, exception);
				}
			}
		}

		return anyExpressionEvaluated;
	}

	/**
	 * Finds and resolves property references present in the given string buffer. CTL expressions present in the
	 * referenced properties are evaluated after resolving. The result is stored back in the given string buffer.
	 *
	 * @param value a string buffer containing property references
	 *
	 * @return <code>true</code> if at least one property reference was found and resolved, <code>false</code> otherwise
	 */
	private boolean resolveReferences(StringBuffer value) {
		boolean anyReferenceResolved = false;
		Matcher propertyMatcher = propertyPattern.matcher(value);

		while (propertyMatcher.find()) {
			// resolve the property reference
			String reference = propertyMatcher.group(1);
			String resolvedReference = properties.getProperty(reference);

			if (resolvedReference == null) {
				resolvedReference = System.getenv(reference);
			}

			if (resolvedReference == null) {
				reference = reference.replace('_', '.');
				resolvedReference = System.getProperty(reference);
			}

			if (resolvedReference != null) {
				// evaluate the CTL expression that might be present in the property
				StringBuffer evaluatedReference = new StringBuffer(resolvedReference);
				evaluateExpressions(evaluatedReference);

				value.replace(propertyMatcher.start(), propertyMatcher.end(), evaluatedReference.toString());
				propertyMatcher.reset(value);

				anyReferenceResolved = true;
			} else {
				errorMessages.add("Property '" + reference + "' is not defined.");
				logger.warn("Cannot resolve reference to property: " + reference);
			}
		}

		return anyReferenceResolved;
	}

	/**
	 * @return <code>true</code> if any error occurred during the last call to the
	 * {@link #resolveRef(StringBuffer, RefResFlag)} method, <code>false</code> otherwise
	 */
	public boolean anyErrorOccured() {
		return !errorMessages.isEmpty();
	}

	/**
	 * @return a read-only list of error messages collected during the last call to the
	 * {@link #resolveRef(StringBuffer, RefResFlag)} method
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

}

