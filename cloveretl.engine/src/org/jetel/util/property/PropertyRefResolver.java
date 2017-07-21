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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.GraphParameters;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.interpreter.CTLExpressionEvaluator;
import org.jetel.interpreter.ParseException;
import org.jetel.util.MiscUtils;
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
	private final GraphParameters parameters;

	/** the regex pattern used to find CTL expressions */
	private static final Pattern expressionPattern = Pattern.compile(Defaults.GraphProperties.EXPRESSION_PLACEHOLDER_REGEX);
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

	/** AuthorityProxy is used to resolve secure parameters. If proxy is null, one is taken from context. */
	private IAuthorityProxy authorityProxy;
	
	/**
	 * Constructs a <code>PropertyRefResolver</code> with empty properties. By default, the CTL expressions will
	 * be evaluated and the property references will be resolved. This may be changed by setting the resolve flag
	 * to <code>false</code>.
	 */
	public PropertyRefResolver() {
		this.parameters = new GraphParameters();
	}

	/**
	 * Constructs a <code>PropertyRefResolver</code> for the given properties. By default, the CTL expressions will
	 * be evaluated and the property references will be resolved. This may be changed by setting the resolve flag
	 * to <code>false</code>.
	 *
	 * @param properties properties to be used for resolving references
	 */
	public PropertyRefResolver(Properties properties) {
		parameters = new GraphParameters();
		parameters.addProperties(properties);
	}

	public PropertyRefResolver(GraphParameters graphParameters) {
		if (graphParameters != null) {
			this.parameters = graphParameters;
		} else {
			this.parameters = new GraphParameters();
		}
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
		this(graph.getGraphParameters());
	}
	
	/**
	 * Sets authority proxy, which will be used to decrypt secure parameters.
	 * If no proxy is set, {@link ContextProvider#getAuthorityProxy()} is used instead.
	 * @param authorityProxy
	 */
	public void setAuthorityProxy(IAuthorityProxy authorityProxy) {
		this.authorityProxy = authorityProxy;
	}
	
	private IAuthorityProxy getAuthorityProxy() {
		return (authorityProxy != null) ? authorityProxy : ContextProvider.getAuthorityProxy();
	}
	
	public GraphParameters getGraphParameters() {
		return parameters;
	}
	
	/**
	 * Clears properties used by this resolver.
	 */
	public void clear() {
		parameters.clear();
	}
	
	/**
	 * Adds the given properties (key=value pairs) to the internal set of properties.
	 *
	 * @param properties properties to be added
	 */
	public void addProperties(Properties properties) {
		this.parameters.addProperties(properties);
	}

    public void addProperty(String propertyName, String propertyValue) {
    	this.parameters.addGraphParameter(propertyName, propertyValue);
    }

    public void setProperty(String propertyName, String propertyValue) {
    	GraphParameter graphParameter = parameters.getGraphParameter(propertyName);
    	if (!graphParameter.getValue().isEmpty()) {
    		graphParameter.setValue(propertyValue);
    	} else {
    		addProperty(propertyName, propertyValue);
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
		return resolveRef(value, null);
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
	private void resolveRef(StringBuilder value, RefResFlag flag) {
		// clear error messages before doing anything else
		errorMessages.clear();
		// let's start the recursion depth measuring
		recursionDepth = 0;
		
		if (value == null || !resolve) {
			return;
		}

		if (flag == null) {
			flag = RefResFlag.REGULAR;
		}
		
		String formerValue = value.toString();
		try {
			//evaluate CTL expressions
			if (flag.resolveCTLStatements()) {
				evaluateExpressions(value, flag);
			}
	
			//resolve remaining property references
			resolveReferences(value, flag);
		} catch (RecursionOverflowedException e) {
			if (ContextProvider.getRuntimeContext() == null || ContextProvider.getRuntimeContext().isStrictGraphFactorization()) {
				throw e;
			} else {
				logger.warn(e);
				value.setLength(0);
				value.append(formerValue);
			}
		}

		//
		// resolve special characters if desired
		//

		if (flag.resolveSpecCharacters()) {
			String resolvedValue = StringUtils.stringToSpecChar(value);

			value.setLength(0);
			value.append(resolvedValue);
		}
	}

	/**
	 * Finds and evaluates CTL expressions present in the given string buffer. Property references present in the CTL
	 * expressions are resolved before evaluation. The result is stored back in the given string buffer.
	 *
	 * @param value a string buffer containing CTL expressions
	 *
	 * @return <code>true</code> if at least one CTL expression was found and evaluated, <code>false</code> otherwise
	 */
	private void evaluateExpressions(StringBuilder value, RefResFlag flag) {
		if (!Defaults.GraphProperties.EXPRESSION_EVALUATION_ENABLED) {
			return;
		}

		Matcher expressionMatcher = expressionPattern.matcher(value);
		while (expressionMatcher.find()) {
			//aren't we too deep in recursion?
			if (isRecursionOverflowed()) {
				throw new RecursionOverflowedException(PropertyMessages.getString("PropertyRefResolver_infinite_recursion_warning")); //$NON-NLS-1$
			}

			String expression = expressionMatcher.group(1);

			if (StringUtils.isEmpty(expression)) {
				// no evaluation is necessary in case of empty CTL expressions
				value.delete(expressionMatcher.start(), expressionMatcher.end());
				expressionMatcher.region(expressionMatcher.start(), value.length());
			} else {
				// resolve property references that might be present in the CTL expression
				StringBuilder resolvedExpression = new StringBuilder(expression);
				resolveReferences(resolvedExpression, flag);

				// make sure that expression quotes are unescaped before evaluation of the CTL expression
				StringUtils.unescapeCharacters(resolvedExpression, EXPRESSION_QUOTE);

				try {
					// finally evaluate the CTL expression
					String evaluatedExpression = expressionEvaluator.evaluate(resolvedExpression.toString());

					// update the expression matcher so that find() starts at the correct index
					value.replace(expressionMatcher.start(), expressionMatcher.end(), evaluatedExpression);
					expressionMatcher.region(expressionMatcher.start() + evaluatedExpression.length(), value.length());

				} catch (ParseException exception) {
					errorMessages.add(MessageFormat.format(PropertyMessages.getString("PropertyRefResolver_invalid_ctl_warning"), resolvedExpression)); //$NON-NLS-1$
					logger.warn(MessageFormat.format(PropertyMessages.getString("PropertyRefResolver_evaluation_failed_warning"), resolvedExpression), exception); //$NON-NLS-1$
				}
			}
		}
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
	private void resolveReferences(StringBuilder value, RefResFlag flag) {
		Matcher propertyMatcher = propertyPattern.matcher(value);
		int nextStart = 0;
		
		while (propertyMatcher.find(nextStart)) {
			//aren't we too deep in recursion?
			if (isRecursionOverflowed()) {
				throw new RecursionOverflowedException(PropertyMessages.getString("PropertyRefResolver_infinite_recursion_warning")); //$NON-NLS-1$
			}

			// resolve the property reference
			String reference = propertyMatcher.group(1);
			String resolvedReference = null;
			boolean canBeParamaterResolved = true;
			
			if (parameters.hasGraphParameter(reference)) {
				GraphParameter param = parameters.getGraphParameter(reference);
				canBeParamaterResolved = param.canBeResolved();
				if (param.isSecure()) {
					try {
						resolvedReference = getAuthorityProxy().getSecureParamater(param.getName(), param.getValue());
					} catch (Exception e) {
						JetelRuntimeException ee = new JetelRuntimeException("Secure graph parameter '" + param.getName() + "' has invalid value.", e);
						if (ContextProvider.getRuntimeContext() == null || ContextProvider.getRuntimeContext().isStrictGraphFactorization()) {
							throw ee;
						} else {
							logger.warn(ee);
						}
					}
				} else {
					resolvedReference = param.getValue();
				}
			} else {
				if (resolvedReference == null) {
					resolvedReference = MiscUtils.getEnvSafe(reference);
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
			}
			
			if (resolvedReference != null) {
				// evaluate the CTL expression that might be present in the property
				StringBuilder evaluatedReference = new StringBuilder(resolvedReference);
				if (flag.resolveCTLStatements()) {
					evaluateExpressions(evaluatedReference, flag);
				}
				value.replace(propertyMatcher.start(), propertyMatcher.end(), evaluatedReference.toString());
				if (!canBeParamaterResolved && containsProperty(resolvedReference)) {
					nextStart = propertyMatcher.start() + evaluatedReference.length();
				}
			} else {
				nextStart = propertyMatcher.end();
				
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
		resolveAll(properties, null);
	}

	/**
	 * Evaluates CTL expressions and resolves property references present in each property in the given properties.
	 * The result is then stored back into the given properties.
	 * 
	 * @param properties
	 * @param refResFlag
	 */
	public void resolveAll(Properties properties, RefResFlag refResFlag, String... excludedProperties) {
		List<String> excludedPropertiesList = Arrays.asList(excludedProperties);
		for (Entry<Object, Object> property : properties.entrySet()) {
			if (!excludedPropertiesList.contains((String) property.getKey())) {
				properties.setProperty((String) property.getKey(), resolveRef((String) property.getValue(), refResFlag));
			}
		}
	}

	/**
	 * @param propertyName value of this property name is returned
	 * @param refResFlag flag which is used by resolver
	 * @return resolved value of given property name
	 */
	public String getResolvedPropertyValue(String propertyName, RefResFlag refResFlag) {
		if (!StringUtils.isEmpty(propertyName)) {
			if (parameters.hasGraphParameter(propertyName)) {
				StringBuilder propertyReference = new StringBuilder();
				propertyReference.append("${").append(propertyName).append('}');
				String propertyReferenceStr = propertyReference.toString();
				return resolveRef(propertyReferenceStr, refResFlag);
			} else {
				return null;
			}
		} else {
			throw new IllegalArgumentException("empty property name");
		}
	}
	
	/**
	 * Indicates if given string contains also property reference (that needs to be de-referenced)
	 * @param value value to inspect
	 * @return <code>true</code> if value contains reference to at least one property.
	 */
	public static boolean containsProperty(String value){
		if (!StringUtils.isEmpty(value)) {
			return propertyPattern.matcher(value).find();
		} else {
			return false;
		}
	}

	/**
	 * @param value tested string
	 * @return true if the given string is represents reference to property, for example "${abc}"; false otherwise 
	 */
	public static boolean isPropertyReference(String value){
		if (!StringUtils.isEmpty(value)) {
			return propertyPattern.matcher(value).matches();
		} else {
			return false;
		}
	}

	/**
	 * This method extract name of parameter from the given value,
	 * which has to be 'property reference', see {@link #isPropertyReference(String)}.
	 * Null value is returned if the value is not simple property reference.
	 * 
	 * For example:
	 * ${param} -> param
	 * abc${param} -> null
	 * ${param1}${param2} -> null
	 * 
	 * @param value
	 * @return
	 */
	public static String getReferencedProperty(String value) {
		if (!StringUtils.isEmpty(value)) {
			Matcher matcher = propertyPattern.matcher(value);
			if (matcher.matches()) {
				return matcher.group(1);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
	
	/**
	 * @param value tested string
	 * @return true if the given string represents CTL expression, for example "`today()`; false otherwise 
	 */
	public static boolean isCTLExpression(String value){
		if (!StringUtils.isEmpty(value)) {
			return expressionPattern.matcher(value).matches();
		} else {
			return false;
		}
	}
	
	/**
	 * @return list of unresolved parameter references in the given string
	 */
	public static List<String> getUnresolvedProperties(String value){
		if (!StringUtils.isEmpty(value)) {
			List<String> result = new ArrayList<String>();
			Matcher matcher = propertyPattern.matcher(value);
			while (matcher.find()) {
				result.add(matcher.group(1));
			}
			return result;
		} else {
			return Collections.emptyList();
		}
	}

	/**
	 * This exception is thrown by {@link PropertyRefResolver} whenever unlimited recursion
	 * in resolution process is detected. 
	 */
	public static class RecursionOverflowedException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		
		public RecursionOverflowedException(String message) {
			super(message);
		}
	}
	
}

