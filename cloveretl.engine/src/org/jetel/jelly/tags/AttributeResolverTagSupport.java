package org.jetel.jelly.tags;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.jelly.JellyTagException;
import org.apache.commons.jelly.MapTagSupport;
import org.apache.commons.jelly.MissingAttributeException;
import org.apache.commons.jelly.NamespaceAwareTag;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Supports attribute processing on the current tag. Defined attributes are used to set
 * property values of the implemented instance and the ignored attributes are checked
 * for the feature to map record fields as attributes of mapped element. Defined attribute
 * is associated with the field of currently processed record by specifying fieldName 
 * in http://cloverETL.org/ns/jelly/ namespace as attribute value.
 * @author Pavel Pospichal
 *
 */
public abstract class AttributeResolverTagSupport extends MapTagSupport implements NamespaceAwareTag {

	private static final Log logger = LogFactory.getLog(AttributeResolverTagSupport.class);
	
	public static final String CLOVER_MAPPING_NS = "http://cloverETL.org/ns/jelly/";
	
	private Map<String, String> cloverFieldAsAttributeMap = new HashMap<String, String>();
	private Map<String, String> contextNS = new HashMap<String, String>();
	
	protected void resolveAttributes() throws MissingAttributeException, JellyTagException {
		
		try {
			BeanUtils.populate(this, getAttributes());
			
			// get properties defined only by current instance
			PropertyDescriptor[] propertyDescs = Introspector.getBeanInfo(this.getClass(), AttributeResolverTagSupport.class).getPropertyDescriptors();
			for (PropertyDescriptor propertyDesc : propertyDescs) {
				getAttributes().remove(propertyDesc.getName());
			}
			
			if (!contextNS.values().contains(CLOVER_MAPPING_NS)) {
				if (getAttributes().size() != 0 && logger.isWarnEnabled()) {
					logger.warn("The tag defines attributes[" + getAttributes().size() + "] that are not resolved!");
				}
				return;
			}
			
			String cloverMappingPrefix = null;
			for (Map.Entry<String, String> nsEntry: contextNS.entrySet()) {
				if (CLOVER_MAPPING_NS.equals(nsEntry.getValue())) {
					cloverMappingPrefix = nsEntry.getKey();
					break;
				}
			}
			
			// store attributes with values representing potential field of currently processed record
			for (Object attributeName : getAttributes().keySet()) {
				String attributeValue = (String)getAttributes().get(attributeName);
				if (attributeValue.startsWith(cloverMappingPrefix+":")) {
					// TODO: regular expression may be more suitable
					int startIndex = attributeValue.indexOf(":")+1;
					if (startIndex < attributeValue.length()) {
						String cloverFieldName = attributeValue.substring(startIndex);
						cloverFieldAsAttributeMap.put((String)attributeName, cloverFieldName);
					}
				}
			}

		} catch (Exception e) {
			throw new JellyTagException("Unable to resolve associated attributes.", e);
		}
	}

	public void setNamespaceContext(Map contextNamespaces) {
		contextNS.putAll(contextNamespaces);
	}

	public Map<String, String> getCloverFieldAsAttributeMap() {
		return cloverFieldAsAttributeMap;
	}

	public Map<String, String> getContextNS() {
		return contextNS;
	}
	
	
}
