package org.jetel.component.ws.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.wsdl.Binding;
import javax.wsdl.BindingInput;
import javax.wsdl.BindingOperation;
import javax.wsdl.BindingOutput;
import javax.wsdl.Definition;
import javax.wsdl.Input;
import javax.wsdl.Message;
import javax.wsdl.Operation;
import javax.wsdl.Output;
import javax.wsdl.Part;
import javax.wsdl.Port;
import javax.wsdl.PortType;
import javax.wsdl.Service;
import javax.wsdl.Types;
import javax.wsdl.WSDLException;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.ExtensionRegistry;
import javax.wsdl.extensions.schema.Schema;
import javax.wsdl.extensions.soap.SOAPAddress;
import javax.wsdl.extensions.soap.SOAPBinding;
import javax.wsdl.extensions.soap.SOAPOperation;
import javax.wsdl.extensions.soap12.SOAP12Address;
import javax.wsdl.extensions.soap12.SOAP12Binding;
import javax.wsdl.extensions.soap12.SOAP12Operation;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;
import javax.xml.namespace.QName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.neethi.Constants;
import org.apache.neethi.Policy;
import org.apache.neethi.PolicyReference;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.jetel.component.ws.exception.AmbiguousOperationException;
import org.jetel.component.ws.exception.AmbiguousPortException;
import org.jetel.component.ws.exception.WSDLAnalyzeException;
import org.jetel.component.ws.util.extensions.PolicyExtension;
import org.jetel.component.ws.util.extensions.PolicyExtensionDeserializer;
import org.jetel.component.ws.util.extensions.PolicyExtensionImpl;
import org.jetel.component.ws.util.extensions.PolicyExtensionSerializer;
import org.jetel.component.ws.util.extensions.PolicyReferenceExtension;
import org.jetel.component.ws.util.extensions.PolicyReferenceExtensionDeserializer;
import org.jetel.component.ws.util.extensions.PolicyReferenceExtensionImpl;
import org.jetel.component.ws.util.extensions.PolicyReferenceExtensionSerializer;
import org.jetel.component.ws.util.nsmap.WSDLExtensionNamespace;
import org.w3c.dom.Element;

/**
 * The intend of the WSDLAnalyzer class is to simplify access to different
 * information set in the WSDL document. Supports only document style WSDL notation.
 * @author Pavel Pospíchal
 */
public class WSDLAnalyzer {

    private static final Log logger = LogFactory.getLog(WSDLAnalyzer.class);

    public static enum SOAPVersion {

        SOAP11, SOAP12
    };

    public static enum MEP {
        InOut, InOnly, RobustInOnly
    }
    
    private static final String soap11HTTPTransportNamespace = WSDLExtensionNamespace.NS_URI_SOAP11_BINDING_HTTP_TRANSPORT;
    private static final String soap12HTTPTransportNamespace = WSDLExtensionNamespace.NS_URI_SOAP12_BINDING_HTTP_TRANSPORT;

    private static final boolean PERFORM_WSDL_VALIDATION_DEFAULT = false;

    private Definition wsdlDefinition;
    private String targetNamespace;
    private SOAPVersion preferedSOAPVersion = SOAPVersion.SOAP12;

    private Map<String, Policy> localPolicyDefinitions;

    public WSDLAnalyzer(URL wsdlLocation) throws WSDLAnalyzeException {
        try {
            localPolicyDefinitions = new HashMap<String, Policy>();

            WSDLFactory wsdlFactory = WSDLFactory.newInstance();
            WSDLReader wsdlReader = wsdlFactory.newWSDLReader();
            ExtensionRegistry extRegistry = wsdlFactory.newPopulatedExtensionRegistry();
            wsdlReader.setExtensionRegistry(extRegistry);

            registerWSDLExtensions(wsdlReader);
            logger.info("Registry of WSDL extensions initialized.");

            wsdlDefinition = wsdlReader.readWSDL(wsdlLocation.toString());
            targetNamespace = wsdlDefinition.getTargetNamespace();
            logger.info("WSDL document from location " + wsdlLocation + " loaded.");

            lookUpLocalPolicyDefinitions();
            if (logger.isInfoEnabled()) {
                logger.info("Policy exressions at WSDL definitions element " + wsdlDefinition.getQName() + " located.");
            }


        } catch (WSDLException wsdle) {
            throw new WSDLAnalyzeException("Unable to read WSDL file from location '" + wsdlLocation.toString() + "'.", wsdle);
        }
    }

    /**
     * Registr WSDL extensions serializer/deserializer support classes with
     * appropriate element type or attribute type.
     * @param wsdlReader
     */
    private static void registerWSDLExtensions(WSDLReader wsdlReader) {
        // Retrieve ExtensionRegistry from WSDLReader
        ExtensionRegistry registry = wsdlReader.getExtensionRegistry();

        // QName to represent top level Policy Element.of WS-Policy
        //QName policyType = new QName(WSDLExtensionNamespace.NS_URI_WS_POLICY_v1_2, "Policy");
        QName policyType = new QName(Constants.URI_POLICY_NS, Constants.ELEM_POLICY);

        // register the ExtensionSerializer for WS-Policy
        registry.registerSerializer(Definition.class, policyType, new PolicyExtensionSerializer());
        // register the ExtensionDeserializer for WS-Policy
        registry.registerDeserializer(Definition.class, policyType, new PolicyExtensionDeserializer());
        // register the ExtElementType for WS-Policy
        registry.mapExtensionTypes(Definition.class, policyType, PolicyExtensionImpl.class);

        logger.debug("WSDL extension support for " + policyType + " of WS-Policy registered.");
        // QName to represent top level Policy Reference Element.of WS-Policy
        QName policyRefType = new QName(Constants.URI_POLICY_NS, Constants.ELEM_POLICY_REF);

        PolicyReferenceExtensionSerializer policyRefExtSerial = new PolicyReferenceExtensionSerializer();
        PolicyReferenceExtensionDeserializer policyRefExtDeserial = new PolicyReferenceExtensionDeserializer();

        // register the WS-Policy Policy Reference for Service Policy Subject
        registry.registerSerializer(Service.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Service.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Service.class, policyRefType, PolicyReferenceExtensionImpl.class);

        // register the WS-Policy Policy Reference for Endpoint Policy Subject
        registry.registerSerializer(Port.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Port.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Port.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(Binding.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Binding.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Binding.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(PortType.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(PortType.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(PortType.class, policyRefType, PolicyReferenceExtensionImpl.class);

        // register the WS-Policy Policy Reference for Operation Policy Subject
        registry.registerSerializer(BindingOperation.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(BindingOperation.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(BindingOperation.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(Operation.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Operation.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Operation.class, policyRefType, PolicyReferenceExtensionImpl.class);

        // register the WS-Policy Policy Reference for Message Policy Subject
        registry.registerSerializer(BindingInput.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(BindingInput.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(BindingInput.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(Input.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Input.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Input.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(BindingOutput.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(BindingOutput.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(BindingOutput.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(Output.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Output.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Output.class, policyRefType, PolicyReferenceExtensionImpl.class);

        registry.registerSerializer(Message.class, policyRefType, policyRefExtSerial);
        registry.registerDeserializer(Message.class, policyRefType, policyRefExtDeserial);
        registry.mapExtensionTypes(Message.class, policyRefType, PolicyReferenceExtensionImpl.class);

        // support for wsdl11:Fault is not considered

        logger.debug("WSDL extension support for " + policyRefType + " of WS-Policy registered.");
    }

    /**
     * According Web Services Policy 1.2 - Attachment recommendation policy
     * expressions wsp:Policy are defined at wsdl11:Definitions element. Load all
     * available Policy expressions to local repository.
     */
    private void lookUpLocalPolicyDefinitions() throws WSDLAnalyzeException {
        List extensibilityElements = wsdlDefinition.getExtensibilityElements();

        try {
            for (Object extensibilityElementsItem : extensibilityElements) {
                if (!(extensibilityElementsItem instanceof PolicyExtension)) {
                    continue;
                }
                PolicyExtension policyExtension = (PolicyExtension) extensibilityElementsItem;
                Policy policy = policyExtension.getPolicy();

                logger.debug("Policy expression with id '" + policy.getId() + "' found.");
                localPolicyDefinitions.put(policy.getId(), policy);
            }
        } catch (Exception e) {
            throw new WSDLAnalyzeException("Unable to get a policy expression for endpoint policy subject.", e);
        }
    }

    /**
     * Determine the location address of endpoint (EPR) that services the specified
     * operation.
     * @param operationName
     * @return
     * @throws org.jetel.ws.exception.WSDLAnalyzeException
     */
    public URL getEndpointReference(String operationName) throws WSDLAnalyzeException {
        URL endpointReference = null;

        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }

            Binding binding = getHTTPBinding(portType);
            if (binding == null) {
                throw new WSDLAnalyzeException("No HTTP binding found for WSDL operation '" + operationName + "'.");
            }

            String portLocation = getPortLocationByBinding(binding);
            endpointReference = new URL(portLocation);

        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        } catch (AmbiguousPortException ape) {
            throw new WSDLAnalyzeException(ape);
        } catch (MalformedURLException urle) {
            throw new WSDLAnalyzeException("Invalid URL location of Web Services port for operation '" + operationName + "'.");
        }

        return endpointReference;
    }

    //public URL getEndpointReference(String portTypeName, String operationName) {
    //}
    
    /**
     * Determine the qualified name of service that provides the specified
     * operation.
     * @param operationName
     * @return
     * @throws org.jetel.ws.exception.WSDLAnalyzeException
     */
    public QName getServiceQName(String operationName) throws WSDLAnalyzeException {
        QName serviceQName = null;

        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }

            Binding binding = getHTTPBinding(portType);
            if (binding == null) {
                throw new WSDLAnalyzeException("No HTTP binding found for WSDL operation '" + operationName + "'.");
            }

            Service service = getServiceByBinding(binding);
            serviceQName = service.getQName();

        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        } catch (AmbiguousPortException ape) {
            throw new WSDLAnalyzeException(ape);
        }

        return serviceQName;
    }

    /**
     * Determine the qualified name of root element in the request message
     * that is part of the specified service operation.
     * @param operationName
     * @return
     * @throws org.jetel.ws.exception.WSDLAnalyzeException
     */
    public QName getOperationRequestQName(String operationName) throws WSDLAnalyzeException {
        QName operationRequestQName = null;
        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }
            Operation operation = portType.getOperation(operationName, null, null);
            Input input = operation.getInput();
            if (input == null) {
                throw new WSDLAnalyzeException("Input of operation '" + operationName + "' is not defined.");
            }
            Message message = input.getMessage();

            Map partsMap = message.getParts();
            if (partsMap.size() != 1) {
                throw new WSDLAnalyzeException("Input message of operation '" + operationName + "' does not support document style.");
            }

            Part wrappedPart = (Part) partsMap.values().iterator().next();
            operationRequestQName = wrappedPart.getElementName();
            if (operationRequestQName == null) {
                throw new WSDLAnalyzeException("Operation '" + operationName + "' does not support document style.");
            }
        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        } catch (NullPointerException npe) {
            throw new WSDLAnalyzeException("WSDL document does not support document style.", npe);
        }

        return operationRequestQName;
    }

    /**
     * Determine the qualified name of root element in the response message
     * that is part of the specified service operation.
     * @param operationName
     * @return
     * @throws org.jetel.ws.exception.WSDLAnalyzeException
     */
    public QName getOperationResponseQName(String operationName) throws WSDLAnalyzeException {
        QName operationResponseQName = null;
        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }
            Operation operation = portType.getOperation(operationName, null, null);
            Output output = operation.getOutput();
            // check for In only operation MEP
            if (output == null) {
                return null;
            }
            Message message = output.getMessage();

            Map partsMap = message.getParts();
            if (partsMap.size() != 1) {
                throw new WSDLAnalyzeException("Output message of operation '" + operationName + "' does not support document style.");
            }

            Part wrappedPart = (Part) partsMap.values().iterator().next();
            operationResponseQName = wrappedPart.getElementName();
            if (operationResponseQName == null) {
                throw new WSDLAnalyzeException("Operation '" + operationName + "' does not support document style.");
            }
        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        } catch (NullPointerException npe) {
            throw new WSDLAnalyzeException("WSDL document does not support document style.", npe);
        }

        return operationResponseQName;
    }

    public QName getPortQName(String operationName) throws WSDLAnalyzeException {
        QName portQName = null;

        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }

            Binding binding = getHTTPBinding(portType);
            if (binding == null) {
                throw new WSDLAnalyzeException("No HTTP binding found for WSDL operation '" + operationName + "'.");
            }

            Port port = getPortByBinding(binding);
            portQName = new QName(targetNamespace, port.getName());

        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        } catch (AmbiguousPortException ape) {
            throw new WSDLAnalyzeException(ape);
        }

        return portQName;
    }

    public String getTargetNamespace() {
        return wsdlDefinition.getTargetNamespace();
    }

    public String getSOAPAction(String operationName) throws WSDLAnalyzeException {
        String soapAction = "";

        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }
            Binding binding = getHTTPBinding(portType);
            if (binding == null) {
                throw new WSDLAnalyzeException("No HTTP binding found for WSDL operation '" + operationName + "'.");
            }

            BindingOperation bindingOperation = binding.getBindingOperation(operationName, null, null);
            List extensibilityElements = bindingOperation.getExtensibilityElements();

            for (Object extensibilityElementsItem : extensibilityElements) {
                // used version of SOAP is selected by WSDL binding
                if (extensibilityElementsItem instanceof SOAPOperation) {
                    SOAPOperation soapOperation = (SOAPOperation) extensibilityElementsItem;
                    soapAction = soapOperation.getSoapActionURI();
                    break;
                } else if (extensibilityElementsItem instanceof SOAP12Operation) {
                    SOAP12Operation soapOperation = (SOAP12Operation) extensibilityElementsItem;
                    soapAction = soapOperation.getSoapActionURI();
                    break;
                }
            }

        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        }

        if (soapAction == null || (soapAction.length() == 0)) {
            soapAction = "\"\"";
        }

        return soapAction;
    }

    /**
     * Find the version of SOAP protocol declared in the binding section of WSDL document.
     * Only the section binding portType with specified operation is considered. In case
     * there are several SOAP versions supported by operation, the selection is based 
     * on the prefered.
     * @param operationName
     * @return
     */
    public SOAPVersion getSOAPVersion(String operationName) throws WSDLAnalyzeException {
        SOAPVersion soapVersionSupport = null;

        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }
            Binding binding = getHTTPBinding(portType);
            if (binding == null) {
                throw new WSDLAnalyzeException("No HTTP binding found for WSDL operation '" + operationName + "'.");
            }

            List extensibilityElements = binding.getExtensibilityElements();
            for (Object extensibilityElementsItem : extensibilityElements) {
                if (extensibilityElementsItem instanceof SOAPBinding) {
                    soapVersionSupport = SOAPVersion.SOAP11;
                    break;
                } else if (extensibilityElementsItem instanceof SOAP12Binding) {
                    soapVersionSupport = SOAPVersion.SOAP12;
                    break;
                }
            }
        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        }

        return soapVersionSupport;
    }

    public XmlSchema getTypesXMLSchema(QName rootElementQName) {
        Types types = wsdlDefinition.getTypes();
        List extensibilityElements = types.getExtensibilityElements();
        XmlSchema schema = null;

        XmlSchemaCollection schemaCollection = new XmlSchemaCollection();
        
        for (Object extensibilityElementsItem : extensibilityElements) {
            Schema extensionSchema = (Schema) extensibilityElementsItem;
            Element schemaElement = extensionSchema.getElement();

            // schemaElement contains schema with elements defined in targetNamespace
            schema = schemaCollection.read(schemaElement);

            // check schema definition of element
            XmlSchemaElement rootElement = schema.getElementByName(rootElementQName);
            if (rootElement != null) {
                break;
            }
        }

        return schema;
    }

    public List<Policy> getServiceEffectivePolicies(String serviceName) throws WSDLAnalyzeException {
        List<Policy> serviceEffectivePolicies = new ArrayList<Policy>();

        try {
            // retrieve Service References from wsdl:Service
            QName serviceQName = new QName(targetNamespace, serviceName);
            Service service = wsdlDefinition.getService(serviceQName);
            List extensibilityElements = service.getExtensibilityElements();
            List<Policy> servicePolicies = getPolicies(extensibilityElements, serviceQName);
            serviceEffectivePolicies.addAll(servicePolicies);

        } catch (Exception e) {
            throw new WSDLAnalyzeException("Unable to get a policy expression for Service Policy Subject.", e);
        }


        return serviceEffectivePolicies;
    }

    public List<Policy> getEndpointEffectivePolicies(QName portQName) throws WSDLAnalyzeException {
        List<Policy> endpointEffectivePolicies = new ArrayList<Policy>();

        try {
            // retrieve Policy References from wsdl:Port
            Port port = getPort(portQName.getLocalPart());
            List extensibilityElements = port.getExtensibilityElements();
            List<Policy> portPolicies = getPolicies(extensibilityElements, new QName(port.getName()));
            endpointEffectivePolicies.addAll(portPolicies);

            // retrieve Policy References from wsdl:Binding
            Binding binding = port.getBinding();
            extensibilityElements = binding.getExtensibilityElements();
            List<Policy> bindingPolicies = getPolicies(extensibilityElements, binding.getQName());
            endpointEffectivePolicies.addAll(bindingPolicies);
            
            // retrieve Policy References from wsdl:PortType
            PortType portType = binding.getPortType();
            extensibilityElements = portType.getExtensibilityElements();
            List<Policy> portTypePolicies = getPolicies(extensibilityElements, portType.getQName());
            endpointEffectivePolicies.addAll(portTypePolicies);
            
        } catch(Exception e) {
            throw new WSDLAnalyzeException("Unable to get a policy expression for Endpoint Policy Subject.", e);
        }

        return endpointEffectivePolicies;
    }

    public List<Policy> getOperationEffectivePolicies(String bindingName, String operationName) throws WSDLAnalyzeException {
        List<Policy> operationEffectivePolicies = new ArrayList<Policy>();

        try {
            // retrieve Policy References from wsdl:Binding/wsdl:Operation
            QName bindingQName = new QName(targetNamespace, bindingName);
            Binding binding = wsdlDefinition.getBinding(bindingQName);
            BindingOperation bindingOperation = binding.getBindingOperation(operationName, null, null);
            List extensibilityElements = bindingOperation.getExtensibilityElements();
            List<Policy> bindingOperationPolicies = getPolicies(extensibilityElements, new QName(bindingOperation.getName()));
            operationEffectivePolicies.addAll(bindingOperationPolicies);

            // retrieve Policy References from wsdl:PortType/wsdl:Operation
            PortType portType = binding.getPortType();
            Operation portTypeOperation = portType.getOperation(operationName, null, null);
            extensibilityElements = portTypeOperation.getExtensibilityElements();
            List<Policy> portTypeOperationPolicies = getPolicies(extensibilityElements, new QName(portTypeOperation.getName()));
            operationEffectivePolicies.addAll(portTypeOperationPolicies);

        } catch(Exception e) {
            throw new WSDLAnalyzeException("Unable to get a policy expression for Endpoint Policy Subject.", e);
        }

        return operationEffectivePolicies;
    }

    public List<Policy> getMessageEffectivePolicies(String bindingName, String operationName, String messageName) throws WSDLAnalyzeException {
        List<Policy> messageEffectivePolicies = new ArrayList<Policy>();

        return messageEffectivePolicies;
    }

    public MEP getMEPType(String operationName) throws WSDLAnalyzeException {
        MEP mepType = null;
        // in WSDL 1.1 there is no mep attribute on operation like in WSDL 2.0
        // MEP type is determined based on attached messages
        try {
            PortType portType = getPortTypeByOperation(operationName);
            if (portType == null) {
                throw new WSDLAnalyzeException("WSDL operation '" + operationName + "' is not defined.");
            }

            Operation operation = portType.getOperation(operationName, null, null);
            Input input = operation.getInput();
            Output output = operation.getOutput();
            Map faults = operation.getFaults();

            // In-Only MEP has only input message
            if (input != null) {
                mepType = MEP.InOnly;
            }

            // In-Out MEP has both input and output messages; faults are arbitrary
            if (input != null && output != null) {
                mepType = MEP.InOut;
            }

            // Robust In-Only MEP has input message and faults messages
            if (input != null && output == null && faults != null) {
                mepType = MEP.RobustInOnly;
            }

            if (mepType == null) {
                throw new WSDLAnalyzeException("Unknown MEP type found.");
            }
        } catch (AmbiguousOperationException aoe) {
            throw new WSDLAnalyzeException(aoe);
        }
            
        return mepType;
    }

    private PortType getPortTypeByOperation(String operationName) throws AmbiguousOperationException {
        Map portTypesMap = wsdlDefinition.getPortTypes();
        Collection portTypes = portTypesMap.values();
        PortType operationPortType = null;

        for (Object portTypesItem : portTypes) {
            PortType portType = (PortType) portTypesItem;
            Operation operation = portType.getOperation(operationName, null, null);
            
            if (operation != null) {
                if (operationPortType != null) {
                    throw new AmbiguousOperationException("WSDL operation '" + operationName + "' is not unique.");
                }

                operationPortType = portType;
            }
        }

        return operationPortType;
    }

    private Binding getHTTPBinding(PortType portType) {
        Map bindingsMap = wsdlDefinition.getBindings();
        Collection bindings = bindingsMap.values();
        Binding soap11Binding = null;
        Binding soap12Binding = null;

        for (Object bindingsItem : bindings) {
            Binding binding = (Binding) bindingsItem;
            if (portType != binding.getPortType()) {
                continue;
            }

            List extensibilityElements = binding.getExtensibilityElements();
            for (Object extensibilityElementsItem : extensibilityElements) {
                if (extensibilityElementsItem instanceof SOAPBinding) {
                    SOAPBinding extensionBinding = (SOAPBinding) extensibilityElementsItem;
                    // check HTTP transport
                    if (soap11HTTPTransportNamespace.equals(extensionBinding.getTransportURI())) {
                        soap11Binding = binding;
                        break;
                    }
                } else if (extensibilityElementsItem instanceof SOAP12Binding) {
                    SOAP12Binding extensionBinding = (SOAP12Binding) extensibilityElementsItem;
                    // check HTTP transport
                    if (soap12HTTPTransportNamespace.equals(extensionBinding.getTransportURI())) {
                        soap12Binding = binding;
                        break;
                    }
                }
            }
        }

        if (soap12Binding != null && preferedSOAPVersion == SOAPVersion.SOAP12) {
            return soap12Binding;
        }

        if (soap11Binding != null && preferedSOAPVersion == SOAPVersion.SOAP11) {
            return soap11Binding;
        }

        if (soap12Binding != null) {
            return soap12Binding;
        }

        return soap11Binding;
    }

    private Service getServiceByBinding(Binding binding) throws AmbiguousPortException {
        Map servicesMap = wsdlDefinition.getServices();
        Collection services = servicesMap.values();
        Service operationService = null;

        for (Object servicesItem : services) {
            Service service = (Service) servicesItem;
            Map portsMap = service.getPorts();
            Collection ports = portsMap.values();

            for (Object portsItem : ports) {
                Port port = (Port) portsItem;
                if (binding != port.getBinding()) {
                    continue;
                }

                if (operationService != null) {
                    throw new AmbiguousPortException("WSDL port for binding '" + binding.getQName() + "' is not unique.");
                }

                operationService = service;
            }
        }

        return operationService;
    }

    private Port getPortByBinding(Binding binding) throws AmbiguousPortException {
        Map servicesMap = wsdlDefinition.getServices();
        Collection services = servicesMap.values();
        Port bindingPort = null;

        for (Object servicesItem : services) {
            Service service = (Service) servicesItem;
            Map portsMap = service.getPorts();
            Collection ports = portsMap.values();

            for (Object portsItem : ports) {
                Port port = (Port) portsItem;
                if (binding != port.getBinding()) {
                    continue;
                }

                if (bindingPort != null) {
                    throw new AmbiguousPortException("WSDL port for binding '" + binding.getQName() + "' is not unique.");
                }

                bindingPort = port;
            }
        }

        return bindingPort;
    }

    private String getPortLocationByBinding(Binding binding) throws AmbiguousPortException {
        String portLocation = null;

        Port port = getPortByBinding(binding);
        if (port == null) {
            return portLocation;
        }

        List extensibilityElements = port.getExtensibilityElements();
        for (Object extensibilityElementsItem : extensibilityElements) {
            // used version of SOAP is selected by WSDL binding
            if (extensibilityElementsItem instanceof SOAPAddress) {
                SOAPAddress extensionAddress = (SOAPAddress) extensibilityElementsItem;
                portLocation = extensionAddress.getLocationURI();
            } else if (extensibilityElementsItem instanceof SOAP12Address) {
                SOAP12Address extensionAddress = (SOAP12Address) extensibilityElementsItem;
                portLocation = extensionAddress.getLocationURI();
            }
        }

        return portLocation;
    }

    private Port getPort(String portName) throws AmbiguousPortException {
        Map servicesMap = wsdlDefinition.getServices();
        Collection<Service> services = servicesMap.values();
        Port servicePort = null;

        for (Service service : services) {
            Map portsMap = service.getPorts();
            Collection<Port> ports = portsMap.values();

            for (Port port : ports) {
                if (!portName.equals(port.getName())) {
                    continue;
                }
                if (servicePort != null) {
                    throw new AmbiguousPortException("WSDL port '" + portName + "' is not unique.");
                }
                servicePort = port;
            }
        }

        return servicePort;
    }

    private List<Policy> getPolicies(List<ExtensibilityElement> extensibilityElements, QName parentName) throws Exception {
        List<Policy> pickOutPolicies = new ArrayList<Policy>();

        for (Object extensibilityElementsItem : extensibilityElements) {
            if (!(extensibilityElementsItem instanceof PolicyReferenceExtension)) {
                continue;
            }
            PolicyReferenceExtension policyRefExtension = (PolicyReferenceExtension) extensibilityElementsItem;
            PolicyReference policyRef = policyRefExtension.getPolicyReference();

            /* Policy Reference references to local-defined Policy expression
             * (URI starts with character '#') or remote-defined Policy expression.
             */
            String policyLocation = policyRef.getURI();
            if ((policyLocation == null) || (policyLocation.length() == 0)) {
                logger.warn("Policy Reference at '" + parentName + "' has invalid URI value of Policy reference.");
                throw new WSDLAnalyzeException("Policy Reference at '" + parentName + "' has invalid URI value of Policy reference.");
            }
            Policy policy = null;

            if (policyLocation.startsWith("#")) {
                String policyId = policyLocation.substring(1);
                if (!localPolicyDefinitions.containsKey(policyId)) {
                    logger.warn("Policy expression with id '" + policyId + "' is not registered.");
                }
                policy = localPolicyDefinitions.get(policyId);
            } else {
                policy = policyRef.getRemoteReferencedPolicy(policyRef.getURI());
            }

            if (policy == null) {
                throw new WSDLAnalyzeException("Unable to get Policy expression from URI location '" + policyLocation + "'.");
            }

            pickOutPolicies.add(policy);
        }

        return pickOutPolicies;
    }

    public SOAPVersion getPreferedSOAPVersion() {
        return preferedSOAPVersion;
    }

    public void setPreferedSOAPVersion(SOAPVersion preferedSOAPVersion) {
        this.preferedSOAPVersion = preferedSOAPVersion;
    }

    public Definition getWsdlDefinition() {
        return wsdlDefinition;
    }

    public static void main(String[] args) {
        try {
            //URL wsdlLocation = new URL("file:WebServiceGateway.wsdl");
            //URL wsdlLocation = new URL("http://soap.amazon.com/schemas2/AmazonWebServices.wsdl");
            //String operationName = "KeywordSearchRequest";

            URL wsdlLocation = new URL("http://localhost:8080/CloverETLWSExamples/CloverETLDataProviderService?wsdl");
            String operationName = "getCustomers";

            WSDLAnalyzer analyzer = new WSDLAnalyzer(wsdlLocation);
            analyzer.setPreferedSOAPVersion(SOAPVersion.SOAP12);
            SOAPVersion prefSoapVersion = analyzer.getPreferedSOAPVersion();
            System.out.print("Prefered SOAP version is: ");
            if (prefSoapVersion == SOAPVersion.SOAP11) {
                System.out.println("SOAP11");
            } else if (prefSoapVersion == SOAPVersion.SOAP12) {
                System.out.println("SOAP12");
            }

            URL endpointReference = analyzer.getEndpointReference(operationName);
            System.out.println("For operation '" + operationName + "' is endpoint reference " + endpointReference);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
