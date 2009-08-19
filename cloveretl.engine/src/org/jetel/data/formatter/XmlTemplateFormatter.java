
package org.jetel.data.formatter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.jelly.JellyContext;
import org.apache.commons.jelly.JellyException;
import org.apache.commons.jelly.XMLOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Namespace;
import org.dom4j.Node;
import org.dom4j.QName;
import org.dom4j.io.DocumentSource;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.jelly.CloverTagLibrary;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author Pavel Pospichal
 */
public class XmlTemplateFormatter implements BatchPortDefinitionFormatter {

    private final static Log logger = LogFactory.getLog(XmlTemplateFormatter.class);

    public static final String DEFAULT_ROOT_ELEMENT = "root";
    public static final String DEFAULT_RECORD_ELEMENT = "record";
    private static final String XML_MAPPING_ELEMENT = "Mapping";
    private static final String XML_FIELD_MAPPING_ELEMENT = "fieldMapping";
    public static final String ATTRIBUTE_COMPONENT_ID = "component";
    public static final String ATTRIBUTE_GRAPH_NAME = "graph";
    public static final String ATTRIBUTE_CREATED = "created";

    // jelly-specific stuff is better to separate to a stadalone builder
    private static final String NS_URI_JELLY_CORE = "jelly:core";
    private static final String NS_URI_JELLY_HTML = "jelly:html";
    private static final String NS_URI_JELLY_XML = "jelly:xml";
    private static final String NS_URI_JELLY_CLOVER = "jelly:" + CloverTagLibrary.class.getCanonicalName();

    private String rootElement;
    private boolean omitNewLines = false;
    private String charset = null;
    private String dtdPublicId;
	private String dtdSystemId;
    private boolean useRootElement = true;
	private Map<String, String> namespaces = new HashMap<String, String>();
    private String xsdSchemaLocation;
    private String rootDefaultNamespace;
    private String graphName;
    private String componentId;

    private boolean noWhiteSpace = true;

    private Document xmlTemplate;
    private OutputStream os = null;
    private JellyContext jContext;

    private TransformerHandler th = null;

    public XmlTemplateFormatter(final Document xmlTemplate) {
        this.xmlTemplate = xmlTemplate;
        jContext = new JellyContext();
    }

    public void init(List<DataRecordMetadata> _metadatas) throws ComponentNotReadyException {
        Document jellyScript = DocumentHelper.createDocument();

        Namespace coreNS = DocumentHelper.createNamespace("j", NS_URI_JELLY_CORE);
        Namespace cloverNS = DocumentHelper.createNamespace("clover", NS_URI_JELLY_CLOVER);

        Element jellyElement = jellyScript.addElement(new QName("jelly", coreNS));
        jellyElement.addAttribute("trim", (noWhiteSpace) ? "true" : "false");
        jellyElement.add(cloverNS);
        jellyElement.appendContent(xmlTemplate);

        // select all mapping elements (without NS) and associate with jelly namespace
        List<Node> mappingElements = jellyScript.selectNodes("//" + XML_MAPPING_ELEMENT);
        if (logger.isDebugEnabled()) {
            logger.debug("Found " + mappingElements.size() + " " + XML_MAPPING_ELEMENT + " definitions.");
        }
        for (Node mappingNode : mappingElements) {
            if (mappingNode.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            Element mappingElement = (Element) mappingNode;
            mappingElement.setQName(new QName(XML_MAPPING_ELEMENT, cloverNS));
        }
        
        // select all fieldMapping elements (without NS) and associate with jelly namespace
        List<Node> fieldMappingElements = jellyScript.selectNodes("//" + XML_FIELD_MAPPING_ELEMENT);
        if (logger.isDebugEnabled()) {
            logger.debug("Found " + fieldMappingElements.size() + " " + XML_FIELD_MAPPING_ELEMENT + " definitions.");
        }
        for (Node fieldMappingNode : fieldMappingElements) {
            if (fieldMappingNode.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            Element fieldMappingElement = (Element) fieldMappingNode;
            fieldMappingElement.setQName(new QName(XML_FIELD_MAPPING_ELEMENT, cloverNS));
        }
        
        xmlTemplate = jellyScript;

    }

    public void reset() {
        
    }

    public void setDataTarget(Object outputDataTarget) {
        assert(outputDataTarget != null);
        
        close();
        WritableByteChannel channel = null;
        if (outputDataTarget instanceof WritableByteChannel) {
            channel = (WritableByteChannel) outputDataTarget;
            os = Channels.newOutputStream(channel);
        } else if (outputDataTarget instanceof OutputStream) {
            os = (OutputStream) outputDataTarget;
        } else {
            throw new IllegalArgumentException("Data target " + outputDataTarget.getClass().getName() + " is not supported.");
        }
    }

    public void close() {
        if (os == null) {
            return;
        }

        try {
            flush();
            os.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        os = null;
    }

    public int write(Collection<PortDefinition> portDefinitions) throws IOException {
        if (th == null) {
            writeHeader();
        }

        // setup environment for tag elements
        if (portDefinitions.size() == 0) {
            logger.warn("No record data are bind to Jelly script execution environment.");
        }
        Iterator<PortDefinition> portDefinitionItr = portDefinitions.iterator();
        while (portDefinitionItr.hasNext()) {
            PortDefinition portDefinition = portDefinitionItr.next();
            jContext.setVariable(CloverTagLibrary.CLOVER_NS_IN_JELLY
                    + portDefinition.portIndex, portDefinition);
        }

        try {
            // run constructed jelly script
            XMLOutput xmlOutput = XMLOutput.createDummyXMLOutput();
            xmlOutput.setContentHandler(th);
            xmlOutput.setLexicalHandler(th);
            
            DocumentSource dSource = new DocumentSource(xmlTemplate);
            InputSource templateSource = DocumentSource.sourceToInputSource(dSource);

            jContext.runScript(templateSource, xmlOutput);
            xmlOutput.flush();
        } catch (JellyException je) {
            throw new IOException("Unable to process XML template with Clover mapping defintions: " + je.getMessage());
        } catch (Exception e) {
            throw new IOException("Unable to process XML template with Clover mapping defintions: " + e.getMessage());
        }

        return 0;
    }

    public int writeHeader() throws IOException {
        try {
            th = createHeader(os);
        } catch (Exception e) {
            logger.error("error header", e);
        }
        return 0;
    }

    public int writeFooter() throws IOException {
        try {
            if (th == null) {
                writeHeader();
            }
            createFooter(os, th);
        } catch (Exception e) {
            logger.error("error footer", e);
        }
        th = null;
        os = null;
        return 0;
    }

    public void flush() throws IOException {
        if (os != null) {
            os.flush();
        }
    }

    public void finish() throws IOException {
        if (th != null) {
            writeFooter();
        }
    }

    private TransformerHandler createHeader(OutputStream os) throws FileNotFoundException, TransformerConfigurationException, SAXException {
		StreamResult streamResult = new StreamResult(os);

		SAXTransformerFactory tf = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
		// SAX2.0 ContentHandler.
		TransformerHandler hd = tf.newTransformerHandler();
		Transformer serializer = hd.getTransformer();

		serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
		serializer.setOutputProperty(OutputKeys.ENCODING, this.charset);
        //serializer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		//serializer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,"users.dtd");
		if (omitNewLines)
			serializer.setOutputProperty(OutputKeys.INDENT,"no");
		else
			serializer.setOutputProperty(OutputKeys.INDENT,"yes");

		hd.setResult(streamResult);
		hd.startDocument();

		String root = (rootElement!=null && rootElement.length()>0) ? rootElement : DEFAULT_ROOT_ELEMENT;

		if (useRootElement && dtdPublicId != null && dtdPublicId.trim().length()>0 && dtdSystemId != null && dtdSystemId.trim().length()>0){
			hd.startDTD(root, dtdPublicId, dtdSystemId);
			hd.endDTD();
		}

		//if (recordsPerFile!=1){
		if (this.useRootElement){
			 AttributesImpl atts = new AttributesImpl();
			 atts.addAttribute( "", "", ATTRIBUTE_COMPONENT_ID, "CDATA", componentId);
			 atts.addAttribute( "", "", ATTRIBUTE_GRAPH_NAME, "CDATA", graphName);
			 atts.addAttribute( "", "", ATTRIBUTE_CREATED, "CDATA", (new Date()).toString());
			 if (!StringUtils.isEmpty(xsdSchemaLocation)) {
				 atts.addAttribute( "", "", "xsi:schemaLocation", "CDATA", this.xsdSchemaLocation);
			 }

			 for (String prefix : namespaces.keySet()){
				 String uri = namespaces.get(prefix);
				 hd.startPrefixMapping(prefix, uri);
			 }
			 hd.startElement(rootDefaultNamespace, "", root, atts);
		}
		return hd;
	}

    private void createFooter(OutputStream os, TransformerHandler hd) throws TransformerConfigurationException, SAXException, IOException {
		try {
			//if (recordsPerFile!=1){
			if (this.useRootElement){
				 String root = (rootElement!=null && rootElement.length()>0) ? rootElement : DEFAULT_ROOT_ELEMENT;
				 hd.endElement(rootDefaultNamespace, "", root);
				 for (String prefix : namespaces.keySet())
					 hd.endPrefixMapping(prefix);
			}
			hd.endDocument();
		} finally {
			 os.close();
		}
	}

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public void setDtdPublicId(String dtdPublicId) {
        this.dtdPublicId = dtdPublicId;
    }

    public void setDtdSystemId(String dtdSystemId) {
        this.dtdSystemId = dtdSystemId;
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    public void setNamespaces(Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    public void setRootDefaultNamespace(String rootDefaultNamespace) {
        this.rootDefaultNamespace = rootDefaultNamespace;
    }

    public void setRootElement(String rootElement) {
        this.rootElement = rootElement;
    }

    public void setUseRootElement(boolean useRootElement) {
        this.useRootElement = useRootElement;
    }

    public void setXsdSchemaLocation(String xsdSchemaLocation) {
        this.xsdSchemaLocation = xsdSchemaLocation;
    }
}
