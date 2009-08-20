package org.jetel.jelly.tags;

import java.util.Map;

import org.apache.commons.jelly.JellyTagException;
import org.apache.commons.jelly.MissingAttributeException;
import org.apache.commons.jelly.XMLOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.xsd.ConvertorRegistry;
import org.jetel.data.xsd.IGenericConvertor;
import org.jetel.exception.DataConversionException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Maps the field value of currently processed clover data record into the generated XML document.
 * The name of record's field is specified by the 'fieldName' attribute.  
 * @author Pavel Pospichal
 *
 */
public class FieldMappingTag extends AttributeResolverTagSupport {

	private static final Log logger = LogFactory.getLog(FieldMappingTag.class);
	
	private String fieldName;
	private String elementName;
	private String prefix;
	private String xsdType;
	private DataRecord currentRecord;
	
	public void doTag(XMLOutput output) throws MissingAttributeException, JellyTagException {
		resolveAttributes();
		checkSetting();
		
		MappingTag recordMapping = (MappingTag)findAncestorWithClass(MappingTag.class);
		if (recordMapping == null) {
			throw new JellyTagException("Field mapping can be defined only as part of the cloverETL record mapping.");
		}
		currentRecord = recordMapping.getCurrentlyProcessedRecord();
		
		if (elementName == null) {
			elementName = fieldName;
		}
		
        if (prefix == null) {
        	prefix = recordMapping.getFieldsNamespacePrefix();
        }
        
		serialize(output);
		
	}
	
	private void checkSetting() throws MissingAttributeException {
		if (fieldName == null || fieldName.length() == 0) {
			throw new MissingAttributeException("fieldName");
		}
	}
	
	private void serialize(XMLOutput output) throws JellyTagException {
		AttributesImpl atts = new AttributesImpl();
		
		DataField field = currentRecord.getField(fieldName);
        if (field == null) {
			throw new JellyTagException("Field [" + fieldName + "] is not defined for currently proccesed cloverETL record.");
        }
        
        try {
        	for (Map.Entry<String, String> cloverFieldAsAttributeEntry : getCloverFieldAsAttributeMap().entrySet()) {
        		String attrFieldName = cloverFieldAsAttributeEntry.getValue();
        		DataField attrField = currentRecord.getField(attrFieldName);
                if (attrField == null) {
        			throw new JellyTagException("Field [" + attrFieldName + "] is not defined for currently proccesed cloverETL record.");
                }
                
                // if there is no value specified, output XML document should not contain the attribute
                if (attrField.getValue() == null) {
                	continue;
                }
                
                // value conversion
                IGenericConvertor convertor = ConvertorRegistry.getConvertor(attrField.getMetadata().getTypeAsString());
                if (convertor == null) {
    				throw new JellyTagException("No converter found for the cloverETL data type " + attrField.getMetadata().getTypeAsString());
                }
                
                String attrValue = null;
    			try {
    				attrValue = convertor.print(attrField.getValue());
    			} catch (DataConversionException dce) {
    				throw new JellyTagException("Unable to convert value '" + attrField.getValue() + "' to " + convertor.getClass().getName(), dce);
    			}
    			
    			atts.addAttribute("", "", cloverFieldAsAttributeEntry.getKey(), "CDATA", attrValue);
            }
        } catch(Exception e) {
        	throw new JellyTagException("Unable to serialize field of currently processed record as attribute for mapped element.", e);
        }
        
        try {
        	
        	// if there is no value specified, output XML document should not contain the element
        	// or there have to be xsi:nill attribute specified based on the XSD model defined.
        	if (field.getValue() == null) {
        		return;
        	}
        	
        	if (prefix != null) {
        		elementName = prefix + ":" + elementName;
    		}
            output.startElement("", "", elementName, atts);

            // there should be specific serializers for each registered data type
            String value = null;

            // value conversion
            IGenericConvertor convertor = ConvertorRegistry.getConvertor(field.getMetadata().getTypeAsString(), xsdType);
            if (convertor == null) {
				throw new JellyTagException("No converter found for the cloverETL data type " + field.getMetadata().getTypeAsString());
            }
            
			try {
				value = convertor.print(field.getValue());
			} catch (DataConversionException dce) {
				throw new JellyTagException("Unable to convert value '" + field.getValue() + "' to " + convertor.getClass().getName(), dce);
			}
            
            output.characters(value.toCharArray(), 0, value.length());
            
            output.endElement("", "", elementName);
        } catch(Exception e) {
        	throw new JellyTagException("Unable to serialize field of currently processed record to XML document.", e);
        }
		
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String elementName) {
		this.elementName = elementName;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getXsdType() {
		return xsdType;
	}

	public void setXsdType(String xsdType) {
		this.xsdType = xsdType;
	}

}
