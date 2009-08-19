
package org.jetel.jelly.tags;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.jelly.JellyContext;
import org.apache.commons.jelly.JellyTagException;
import org.apache.commons.jelly.MissingAttributeException;
import org.apache.commons.jelly.TagSupport;
import org.apache.commons.jelly.XMLOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.jelly.CloverTagLibrary;
import org.jetel.metadata.DataRecordMetadata;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author Pavel Pospichal
 */
public class MappingTag extends TagSupport {

    private static final Log logger = LogFactory.getLog(MappingTag.class);

    private final static Pattern NAMESPACE = Pattern.compile("(.+)[=]([\"]|['])(.+)([\"]|['])$");
    // non-england numbers and symbols are not considered
    private final static Pattern XML_PREFIXED_NAME = Pattern.compile("^([_A-Za-z][-._A-Za-z0-9]*):([_A-Za-z][-._A-Za-z0-9]*)$");

    // Name of element with optional prefix representing CloverETL record in out XML file
    private String element;
    // Index of input file, where will be records read from
    private int inPort = -1;
    // Comma separated list of fields which represent relation key of this input port.
    private String key;
    // Comma separated list of fields from parent port, which will be mapped
    // to key fields from this input port
    private String parentKey;
    // Defines way how records fields will be generated to XML out file.
    // There are two possible values:“elements”, “attributes”
    private FieldAsType fieldsAs;
    // Comma separated list of fields which won't be generated in a way
    // specified by fieldsAs attribute.
    private String fieldsAsExcept;
    // Comma separated list of fields which will be completely excluded
    // from out XML file
    private String fieldsIgnore;
    // List of pairs prefix-URI
    private String namespaces;
    // URI of default namespace for this element.
    private String defaultNamespace;
    // Namespace prefix which will be used for fields' elements or attributes. 
    private String fieldsNamespacePrefix;

    private PortDefinition portDefinition;

    private DataRecord currentlyProcessedRecord = null;

    private enum FieldAsType {

        ELEMENTS("elements"), ATTRIBUTES("attributes");
        private final String value;

        FieldAsType(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static FieldAsType fromValue(String v) {
            for (FieldAsType c : FieldAsType.values()) {
                if (c.value.equals(v)) {
                    return c;
                }
            }
            throw new IllegalArgumentException(v);
        }
    };

    public void doTag(XMLOutput output) throws MissingAttributeException, JellyTagException {
        JellyContext jContext = getContext();

        if (inPort < 0) {
            throw new MissingAttributeException("inPort");
        }

        // retrieve port definition from runtime context
        Object cloverObject = jContext.getVariable(CloverTagLibrary.CLOVER_NS_IN_JELLY + inPort);
        if (cloverObject == null) {
            throw new JellyTagException("No content in Clover namespace defined for Jelly context.");
        }
        if (!(cloverObject instanceof PortDefinition)) {
            throw new JellyTagException("Found unrecognised content in Clover namespace defined for Jelly context.");
        }
        portDefinition = (PortDefinition) cloverObject;
        
        prepareEnvironment();
        
        try {
            serializeRecords(output);
        } catch(Exception e) {
            throw new JellyTagException(e);
        }

    }

    private void prepareEnvironment() throws MissingAttributeException, JellyTagException {
        // setting parameters
        try {
            //portDefinition.parent = parentPort;
            //portDefinition.portIndex = inPort; It looks up portDefinition with portIndex
            portDefinition.element = element;
            if (portDefinition.element == null || portDefinition.element.length() == 0) {
                throw new MissingAttributeException("element");
            }
            portDefinition.keysAttr = (key == null) ? "" : key;
            //portDefinition.keysDeprecatedAttr = portAttribs.getString(XML_KEYS_ATTRIBUTE, null);
            portDefinition.parentKeysAttr = (parentKey == null) ? "" : parentKey;

            //portDefinition.keysToParentDeprecatedAttr = portAttribs.getString(XML_RELATION_KEYS_TO_PARENT_ATTRIBUTE, null);
            //portDefinition.keysFromParentDeprecatedAttr = portAttribs.getString(XML_RELATION_KEYS_FROM_PARENT_ATTRIBUTE, null);

            portDefinition.namespaces = (namespaces == null) ? new HashMap<String, String>() : getNamespaces(namespaces);
            portDefinition.defaultNamespace = defaultNamespace;
            portDefinition.fieldsNamespacePrefix = fieldsNamespacePrefix;

            portDefinition.fieldsAsAttributes = (FieldAsType.ATTRIBUTES == fieldsAs);
            portDefinition.fieldsIgnore = parseFields(fieldsIgnore);
            portDefinition.fieldsAsExcept = parseFields(fieldsAsExcept);

            if (portDefinition.keysAttr != null) {
                portDefinition.keys = portDefinition.keysAttr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            }
            if (portDefinition.parentKeysAttr != null) {
                portDefinition.parentKeys = portDefinition.parentKeysAttr.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            }

            if (portDefinition.keysAttr != null) {
                portDefinition.relationKeysStrings.add(portDefinition.keysAttr);
                portDefinition.relationKeysArrays.add(portDefinition.keys);
            }
            if (portDefinition.parentKeysAttr != null) {
                portDefinition.relationKeysStrings.add(portDefinition.parentKeysAttr);
                portDefinition.relationKeysArrays.add(portDefinition.parentKeys);
            }

            // Actual mapping is not conscious about nested mappings. Nested
            // mappings have to contact its ancestor.
            //portData.children


            // separate attribute fields of records from fields that will
            // be represented as elements
            DataRecordMetadata recordMetadata = portDefinition.metadata;
            int fieldsCnt = recordMetadata.getNumFields();
            List<Integer> fieldsAsAttributesIndexes = new ArrayList<Integer>();
            List<Integer> fieldsAsElementsIndexes = new ArrayList<Integer>();

            next: for (int i = 0; i < fieldsCnt; i++) {
                String fieldName = recordMetadata.getField(i).getName();

                // the field is ignored
                if (portDefinition.fieldsIgnore != null && portDefinition.fieldsIgnore.contains(fieldName)) {
                    continue;
                }
                
                if (portDefinition.fieldsAsExcept != null && portDefinition.fieldsAsExcept.size() > 0
                        && Collections.binarySearch(portDefinition.fieldsAsExcept, fieldName, null) > -1) {
                    // found in exception list
                    if (portDefinition.fieldsAsAttributes) {
                        fieldsAsElementsIndexes.add(i);
                    } else {
                        fieldsAsAttributesIndexes.add(i);
                    }
                } else {
                    // NOT found in exception list
                    if (portDefinition.fieldsAsAttributes) {
                        fieldsAsAttributesIndexes.add(i);
                    } else {
                        fieldsAsElementsIndexes.add(i);
                    }
                }
            }

            portDefinition.fieldsAsAttributesIndexes = fieldsAsAttributesIndexes.toArray(
                    new Integer[fieldsAsAttributesIndexes.size()]);
            portDefinition.fieldsAsElementsIndexes = fieldsAsElementsIndexes.toArray(
                    new Integer[fieldsAsElementsIndexes.size()]);

            // TODO: pridat klice datovych entit -> PortDefinition

            if (logger.isDebugEnabled()) {
                logger.debug("portDefinition.element:'" + portDefinition.element + "'");
                logger.debug("portDefinition.keysAttr:'" + portDefinition.keysAttr + "'");
                logger.debug("portDefinition.parentKeysAttr:'" + portDefinition.parentKeysAttr + "'");
                logger.debug("portDefinition.namespaces:'" + portDefinition.namespaces + "'");
                logger.debug("portDefinition.defaultNamespace:'" + portDefinition.defaultNamespace + "'");
                logger.debug("portDefinition.fieldsNamespacePrefix:'" + portDefinition.fieldsNamespacePrefix + "'");
                logger.debug("portDefinition.fieldsAsAttributes:'" + portDefinition.fieldsAsAttributes + "'");
                logger.debug("portDefinition.fieldsIgnore:'" + portDefinition.fieldsIgnore + "'");
                logger.debug("portDefinition.fieldsAsExcept:'" + portDefinition.fieldsAsExcept + "'");
                logger.debug("portDefinition.keys:'" + portDefinition.keys + "'");
                logger.debug("portDefinition.parentKeys:'" + portDefinition.parentKeys + "'");
                logger.debug("portDefinition.relationKeysStrings:'" + portDefinition.relationKeysStrings + "'");
                logger.debug("portDefinition.relationKeysArrays:'" + portDefinition.relationKeysArrays + "'");
                logger.debug("portDefinition.fieldsAsAttributesIndexes:'" + portDefinition.fieldsAsAttributesIndexes + "'");
                logger.debug("portDefinition.fieldsAsElementsIndexes:'" + portDefinition.fieldsAsElementsIndexes + "'");
            }
        } catch(MissingAttributeException me) {
            throw me;
        } catch (Exception e) {
            throw new JellyTagException("Unable to prepare processing environment for port "
                    + inPort + ".", e);
        }
    }

    private void serializeRecords(XMLOutput output) throws JellyTagException {
        AttributesImpl atts = new AttributesImpl();

        // look for the mapping ancestor, which embraces current mapping
        MappingTag ancestorMappingTag = (MappingTag) findAncestorWithClass(MappingTag.class);
        List<DataRecord> recordsToSerialization = portDefinition.dataRecords;
        
        if (ancestorMappingTag != null) {
            // only records compliant with ancestor's key value are allowed to
            // be part of its content
            if (portDefinition.parentKeysAttr != null) {
                DataRecordMetadata ancestorMetadata = ancestorMappingTag.getMetadata();
                DataRecord ancestorRecord = ancestorMappingTag.getCurrentlyProcessedRecord();

                if (ancestorMetadata == null) {
                    throw new JellyTagException("Unable to retrieve metadata definition of mapping ancestor.");
                }
                if (ancestorRecord == null) {
                    throw new JellyTagException("Unable to retrieve currently processed record by mapping ancestor.");
                }

                RecordKey recKey = new RecordKey(portDefinition.parentKeys, ancestorMetadata);
                HashKey key = new HashKey(recKey, ancestorRecord);
                PortDefinition.TreeRecord tr = portDefinition.getTreeRecord(portDefinition.keysAttr, key);
                if (tr != null && tr.records != null) {
                   recordsToSerialization = tr.records;
                } else {
                    // no records will be processed
                    recordsToSerialization = new ArrayList<DataRecord>();
                }
            } else {
                // current mapping has no references to its ancestor,
                // no records will be part of the ancestor's content
                recordsToSerialization = new ArrayList<DataRecord>();
                logger.warn("Current mapping of port " + inPort + " has no key references to its ancestor.");
            }
        }

        // records are processed in order they where received on the port -> LinkedList
        for (DataRecord processedRecord : recordsToSerialization) {
            currentlyProcessedRecord = processedRecord;
            
            try {

                for (Map.Entry<String, String> namespaceDef : portDefinition.namespaces.entrySet()) {
                    // if there are no key references to ancestor mapping add default namespace xmlns
                    output.startPrefixMapping(namespaceDef.getKey(), namespaceDef.getValue());
                }

                atts.clear();
                // serialize fields mapped as attributes
                for (int x = 0; x < portDefinition.fieldsAsAttributesIndexes.length; x++) {
                    int i = portDefinition.fieldsAsAttributesIndexes[x];
                    DataField field = processedRecord.getField(i);
                    // TODO: better value serialization based on specific data type
                    String value = field.toString();
                    String name = processedRecord.getMetadata().getField(i).getName();
                    if (portDefinition.fieldsNamespacePrefix != null) {
                        name = portDefinition.fieldsNamespacePrefix + ":" + name;
                    }
                    // The attribute type is one of the strings "CDATA", "ID",
                    // "IDREF", "IDREFS", "NMTOKEN", "NMTOKENS", "ENTITY", "ENTITIES",
                    // or "NOTATION" (always in upper case).    
                    atts.addAttribute("", "", name, "CDATA", value);
                }

                // Any or all of these may be provided, depending on the values of the http://xml.org/sax/features/namespaces
                // and the http://xml.org/sax/features/namespace-prefixes  properties:
                // + the Namespace URI and local name are required when the namespaces property is true (the default),
                // and are optional when the namespaces property is false (if one is specified, both must be);
                // + the qualified name is required when the namespace-prefixes property is true, and is optional
                // when the namespace-prefixes property is false (the default).
                Matcher qNameMatcher = XML_PREFIXED_NAME.matcher(portDefinition.element);
                String namespaceURI = (portDefinition.defaultNamespace == null) ? "" : portDefinition.defaultNamespace;
                String localName = portDefinition.element;
                if (qNameMatcher.find()) {
                    String prefix = qNameMatcher.group(1);
                    // TODO: determine namespace uri from available namespaces based on known prefix
                    localName = qNameMatcher.group(2);
                }
                output.startElement(portDefinition.defaultNamespace, "", portDefinition.element, atts);
                
                // process the content of the mapping to be append as the next child
                // recursivelly render related children ports and add it to document

                invokeBody(output);

                output.endElement(portDefinition.defaultNamespace, "", portDefinition.element);

                for (Map.Entry<String, String> namespaceDef : portDefinition.namespaces.entrySet()) {
                    // if there are no key references to ancestor mapping add default namespace xmlns
                    output.endPrefixMapping(namespaceDef.getKey());

                }
            } catch (SAXException saxe) {
                // all records have to be serialized -> inform superior
                throw new JellyTagException("Unable to serialize record >" + processedRecord.getMetadata().getName() + "<", saxe);
            }
        }

        currentlyProcessedRecord = null;
    }

    private static Map<String, String> getNamespaces(String namespacePaths) throws IllegalArgumentException {
		logger.debug("processing namespace paths: "+namespacePaths);
        Map<String, String> namespaces = new HashMap<String, String>();
		if (namespacePaths == null) return namespaces;
		String ns;
		String path;
		for (String namespacePath: namespacePaths.split(";")) {
            logger.debug("processing namespace path: "+namespacePath);
			Matcher matcher = NAMESPACE.matcher(namespacePath);
			if (!matcher.find()) throw new IllegalArgumentException("The namespace expression '"+ namespacePath +"' is not valid.");
			if ((ns = matcher.group(1)) != null && (path = matcher.group(3)) != null) {
				namespaces.put(ns, path);
			}
		}
		return namespaces;
	}

    private List<String> parseFields(String field) {
        List<String> fields = new ArrayList<String>();
        if (field == null) return fields;
        String[] ss = field.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        Collections.addAll(fields, ss);

        return fields;
    }

    public DataRecordMetadata getMetadata(){
        return portDefinition.metadata;
    }

    public DataRecord getCurrentlyProcessedRecord() {
        return currentlyProcessedRecord;
    }

    public String getElement() {
        return element;
    }

    public void setElement(String element) {
        this.element = element;
    }

    public String getFieldsIgnore() {
        return fieldsIgnore;
    }

    public void setFieldsIgnore(String fieldsIgnore) {
        this.fieldsIgnore = fieldsIgnore;
    }
    
    public String getFieldsAsExcept() {
        return fieldsAsExcept;
    }

    public void setFieldsAsExcept(String fieldsAsExcept) {
        this.fieldsAsExcept = fieldsAsExcept;
    }

    public String getDefaultNamespace() {
        return defaultNamespace;
    }

    public void setDefaultNamespace(String defaultNamespace) {
        this.defaultNamespace = defaultNamespace;
    }

    public String getFieldsAs() {
        return fieldsAs.value();
    }

    public void setFieldsAs(String fieldsAs) {
        try {
            this.fieldsAs = FieldAsType.fromValue(fieldsAs);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Cannot recognize #fieldsAs# attribute value:\"" + fieldsAs + "\" for MappingTag component.");
        }
        
    }

    public String getFieldsNamespacePrefix() {
        return fieldsNamespacePrefix;
    }

    public void setFieldsNamespacePrefix(String fieldsNamespacePrefix) {
        this.fieldsNamespacePrefix = fieldsNamespacePrefix;
    }

    public int getInPort() {
        return inPort;
    }

    public void setInPort(int inPort) {
        this.inPort = inPort;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(String namespaces) {
        this.namespaces = namespaces;
    }

    public String getParentKey() {
        return parentKey;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

}
