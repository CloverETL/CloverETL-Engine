
package org.jetel.component.ws.util.nsmap;
 
import org.apache.axis2.description.java2wsdl.Java2WSDLConstants;
import org.apache.axis2.wsdl.WSDLConstants;
import org.apache.axis2.namespace.Constants;
import org.apache.ws.commons.schema.utils.NamespaceMap;

/**
 *
 * @author Pavel Pospíchal
 */
public class WSDLExtensionNamespace extends NamespaceMap {

    private static final String DEFAULT_PREFIX_BASE = "ns";
    
    /*
     * Prefix and namespace declaration of XML elements used witin WSDL definition.
     * The constants are based on axis2's binding definition.
     * Namespaces are specified for WSDL version 1.1. 
     */ 
    public static final String NS_PREFIX_WSDL = Constants.NS_PREFIX_WSDL;
    public static final String NS_URI_WSDL11 = Constants.NS_URI_WSDL11;
    public static final String NS_URI_WSDL20 = Constants.NS_URI_WSDL20;    
    //public static final String NS_PREFIX_WSDL_SOAP = Constants.NS_PREFIX_WSDL_SOAP;
    public static final String NS_PREFIX_XML = Constants.NS_PREFIX_XML;
    public static final String NS_URI_XML = Constants.NS_URI_XML;
    public static final String NS_URI_XMLNS = Constants.NS_URI_XMLNS;
    
    public static final String NS_PREFIX_SOAP11 = Java2WSDLConstants.SOAP11_PREFIX;
    // org.apache.axis2.jaxws.util.Constants.URI_WSDL_SOAP11
    public static final String NS_URI_WSDL_SOAP11 = Java2WSDLConstants.URI_WSDL11_SOAP;
    public static final String NS_PREFIX_SOAP12 = Java2WSDLConstants.SOAP12_PREFIX;
    // org.apache.axis2.jaxws.util.Constants.URI_WSDL_SOAP12
    public static final String NS_URI_WSDL_SOAP12 = Java2WSDLConstants.URI_WSDL12_SOAP;
    public static final String NS_PREFIX_HTTP = Java2WSDLConstants.HTTP_PREFIX;
    public static final String NS_URI_WSDL_HTTP = Java2WSDLConstants.HTTP_NAMESPACE;
    public static final String NS_PREFIX_MIME = Java2WSDLConstants.MIME_PREFIX;
    public static final String NS_URI_WSDL_MIME = Java2WSDLConstants.MIME_NAMESPACE;
    
    public static final String NS_URI_SOAP11_BINDING_HTTP_TRANSPORT = "http://schemas.xmlsoap.org/soap/http";
    public static final String NS_URI_SOAP12_BINDING_HTTP_TRANSPORT = "http://www.w3.org/2003/05/soap/bindings/HTTP/";
    public static final String SOAP_BINDING_STYLE = WSDLConstants.STYLE_DOC;
    public static final String SOAP_BODY_USE = WSDLConstants.WSDL_USE_LITERAL;

    public static final String NS_URI_WS_POLICY_v1_2 = "http://schemas.xmlsoap.org/ws/2004/09/policy";
    public static final String NS_URI_WS_POLICY_v1_5 = "http://www.w3.org/ns/ws-policy";

    public static final String NS_URI_WS_ADDRESSING_v1_0 = "http://www.w3.org/2005/08/addressing";
    public static final String NS_PREFIX_WS_ADDRESSING_v1_0 = "wsa";

    public static final String NS_URI_SUN_RM = "http://sun.com/2006/03/rm";
    public static final String NS_URI_WS_RM = "http://schemas.xmlsoap.org/ws/2005/02/rm/policy";

    public static final String NS_URI_WS_ADDRESSING_v1_0_WSDL_BINDING = "http://www.w3.org/2006/05/addressing/wsdl";
    public static final String NS_PREFIX_WS_ADDRESSING_v1_0_WSDL_BINDING = "wsaw";

    private long prefixId = 1;
    private String prefixBaseName = DEFAULT_PREFIX_BASE;
    
    public WSDLExtensionNamespace() {
        init();
    }
    
    /**
     * Default namespace deklarations added to namespace WSDL 1.1 context
     * are SOAP 1.1, SOAP 1.2, HTTP and MIME.
     */
    private void init() {
        this.add(NS_PREFIX_SOAP11, NS_URI_WSDL_SOAP11);
        this.add(NS_PREFIX_SOAP12, NS_URI_WSDL_SOAP12);
        this.add(NS_PREFIX_HTTP, NS_URI_WSDL_HTTP);
        this.add(NS_PREFIX_MIME, NS_URI_WSDL_MIME);
        
        prefixId = 1;
    }
    
    @Override
    public void clear()
    {
        super.clear();
        init();
    }
    
    /**
     * Retrieve prefix for specified namespace or create unique one. 
     * The prefix name is based on 'prefixBaseName' property value. 
     * @param namespace
     * @return
     */
    @Override
    public String getPrefix(String namespace) {
        String prefixName = super.getPrefix(namespace);
        if (prefixName == null) {
            do {
                prefixName = prefixBaseName + prefixId;
                prefixId++;
            } while (super.containsKey(prefixName));
            add(prefixName, namespace);
        }
            
        return prefixName;    
    }
        
    public String getPrefixBaseName() {
        return prefixBaseName;
    }

    public void setPrefixBaseName(String prefixBaseName) {
        this.prefixBaseName = prefixBaseName;
    }
}
