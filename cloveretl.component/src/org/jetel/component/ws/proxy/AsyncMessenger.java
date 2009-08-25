/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
package org.jetel.component.ws.proxy;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.wsdl.Definition;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.impl.util.OMSerializerUtil;
import org.apache.axiom.soap.SOAP11Constants;
import org.apache.axiom.soap.SOAP12Constants;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.client.Options;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.client.Stub;
import org.apache.axis2.client.async.AxisCallback;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ConfigurationContextFactory;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.AxisEndpoint;
import org.apache.axis2.description.AxisModule;
import org.apache.axis2.description.PolicySubject;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.engine.ListenerManager;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.http.HttpTransportProperties;
import org.apache.axis2.transport.http.HttpTransportProperties.ProxyProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.neethi.Policy;
import org.apache.sandesha2.client.SandeshaClientConstants;
import org.apache.sandesha2.util.SandeshaUtil;
import org.apache.ws.commons.schema.XmlSchema;
import org.jetel.component.ws.exception.MessageValidationException;
import org.jetel.component.ws.exception.ResourceException;
import org.jetel.component.ws.exception.SendingMessegeException;
import org.jetel.component.ws.exception.WSMessengerConfigurationException;
import org.jetel.component.ws.exception.WSDLAnalyzeException;
import org.jetel.component.ws.util.WSDLAnalyzer;
import org.jetel.component.ws.util.WSDLBasedPolicyProcessor;
import org.jetel.component.ws.util.XmlMessageValidator;
import org.jetel.component.ws.util.axis2.Axis2Utils;
import org.jetel.component.ws.util.nsmap.WSDLExtensionNamespace;
import org.jetel.plugin.Plugins;

/**
 * AsyncMessenger provides support for communication based on SOAP protocol
 * with remote endpoint. It supports one-way and InOut message exchange 
 * pattern (MEP). To ensure reliable and secure message delivery it
 * implements WS-ReliableMessaging and WS-Security specifications. Transport
 * and messaging issues are comprehensively configured based on used WSDL
 * document.
 * @author Pavel Pospichal
 */
public class AsyncMessenger extends Stub {

    private static final Log defaultLogger = LogFactory.getLog(AsyncMessenger.class);

    public static final long DEFAULT_TIME_OUT_IN_MILLISECONDS = 10*60*1000;
    public static final long DEFAULT_RESPONSE_CHECK_UP_TIME_IN_MILLISECONDS = 500;

    private static final String SANDESHA_AXIS2_MODULE_NAME = "sandesha2";
    private static final String WS_ADRESSING_AXIS2_MODULE_NAME = "addressing";

    private static final String CLIENT_AXIS2_XML_RESOURCE = "client_axis2.xml";
    private static final String AXIS2_HOME_SYSTEM_PROPERTY = "AXIS2_HOME";
    private static final String AXIS2_REPOSITORY_NAME = "repository";

    private Log logger = defaultLogger;
    private WSDLAnalyzer wsdlAnalyzer;
    private ConfigurationContext configurationContext;
    private QName operationQName;
    private QName serviceQName;
    private QName portQName;
    private URL wsdlLocation;

    private WSDLAnalyzer.MEP operationMEP;
    
    private long timeOutInMilliSeconds = DEFAULT_TIME_OUT_IN_MILLISECONDS;
    private long responseCheckUpTime = DEFAULT_RESPONSE_CHECK_UP_TIME_IN_MILLISECONDS;

    private boolean isInitilized = false;
    private boolean interrupted = false;

    private String proxyHostLocation = null;
    private int proxyHostPort;

    // axis2 modules section
    private boolean reliableMessagingEnabled = false;
    private boolean addressingEnabled = false;

    private Map<String, AxisModule> engagedModules = new HashMap<String, AxisModule>();

    private boolean validateMessageOnRequest = false;
    private boolean validateMessageOnResponse = false;

    // asynchronous communication on transport level; separate protocol transports will be created
    private boolean asynchronousTransport = false;

    // HTTP authentication credentials
    String httpUsername = null;
    String httpPassword = null;
    String authDomain = null;
    String authRealm = null;
    
    // java keystorage information
    String trustJKSLocation = null;
    String trustJKSPassword = null;
    String JKSLocation = null;
    String JKSPassword = null;
    
    /**
     * Each messenger is associated with the WSDL document specifying format
     * of messages, communication restrictions and transport configuration.
     * @param wsdlLocation
     * @throws org.jetel.ws.exception.WSMessengerConfigurationException
     */
    public AsyncMessenger(URL wsdlLocation) throws WSMessengerConfigurationException {
    	 this.wsdlLocation = wsdlLocation;
    }

    /**
     * Initializes all necessary resources for successful processing. Self-acting
     * messenger configuration with user-specific preferences is performed based
     * on specified WSDL document.
     * @throws org.jetel.ws.exception.WSMessengerConfigurationException
     */
    public void init() throws WSMessengerConfigurationException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        
        try {
            // configure key storage for certificates and keys security artifacts
            // for SSLSocketFactory
            if (trustJKSLocation != null) {
            	System.setProperty("javax.net.ssl.trustStore", trustJKSLocation);
            	logger.debug("The location of JKS with trusted security artifacts: " + trustJKSLocation);
            }
            if (trustJKSPassword != null) {
        		System.setProperty("javax.net.ssl.trustStorePassword", trustJKSPassword);
        		logger.debug("The password for JKS with trusted security artifacts applied.");
        	}
        	// for SSL mutual authentication
        	if (JKSLocation != null) {
        		System.setProperty("javax.net.ssl.keyStore", JKSLocation);
        		logger.debug("The location of JKS with client security artifacts: " + JKSLocation);
        	}
        	if (JKSPassword != null) {
        		System.setProperty("javax.net.ssl.keyStorePassword", JKSPassword);
        		logger.debug("The password for JKS with client security artifacts applied.");
        	}
        	
            wsdlAnalyzer = new WSDLAnalyzer(wsdlLocation);
            
        } catch (WSDLAnalyzeException ae) {
            throw new WSMessengerConfigurationException("Unable to establish WSDL analyzer.", ae);
        } catch (Exception e) {
            throw new WSMessengerConfigurationException("Unable to establish WSDL analyzer.", e);
        }
        
    	try {
        	ClassLoader pluginClassLoader = Plugins.getPluginDescriptor("org.jetel.component").getClassLoader();
        	Thread.currentThread().setContextClassLoader(pluginClassLoader);
        	
            operationQName = correctNamespace(operationQName);

            //populateAxisService();

            Definition wsdlDefinition = wsdlAnalyzer.getWsdlDefinition();

            if (serviceQName == null) {
                serviceQName = wsdlAnalyzer.getServiceQName(operationQName.getLocalPart());
            } else {
                serviceQName = correctNamespace(serviceQName);
            }

            if (portQName == null) {
                portQName = wsdlAnalyzer.getPortQName(operationQName.getLocalPart());
            } else {
                portQName = correctNamespace(portQName);
            }

            // determine the message exchange pattern
            operationMEP = wsdlAnalyzer.getMEPType(operationQName.getLocalPart());

            configurationContext = getConfigurationContext();
            logger.info("Messenger configuration context loaded.");

            /* This is WSDL4J based constructor to configure the Service Client;
             * It's not yet policy-aware.
             */
            _serviceClient = new ServiceClient(configurationContext, wsdlDefinition, serviceQName, portQName.getLocalPart());
            _service = _serviceClient.getAxisService();
            logger.info("Axis2-specific client dispatcher established for service >" + _service.getName() + "< based on WSDL document.");

            configureAxis2Modules(_serviceClient.getAxisConfiguration());
            logger.info("Axis2 modules engaged for client dispatcher.");

            attachPoliciesToPolicySubjects();

            WSDLBasedPolicyProcessor policyProcessor = new WSDLBasedPolicyProcessor(configurationContext);
            Map<String, AxisModule> modules = policyProcessor.configureEndpointPolices(_service.getEndpoint(_service.getEndpointName()));
            engagedModules.putAll(modules);
            modules = policyProcessor.configureOperationPolices(_service.getOperation(operationQName));
            engagedModules.putAll(modules);
            logger.info("WS-Policy expressions processed.");

            if (engagedModules.containsKey(WS_ADRESSING_AXIS2_MODULE_NAME)) {
                addressingEnabled = true;
                logger.info("Support for WS-Addressing enabled.");
            }
            if (engagedModules.containsKey(SANDESHA_AXIS2_MODULE_NAME)) {
                reliableMessagingEnabled = true;
                logger.info("Support for WS-ReliableMessaging enabled.");
            }

            configureMessenger();
            
        } catch (AxisFault af) {
            throw new WSMessengerConfigurationException("Unable to establish Web Service messenger.", af);
        } catch (WSDLAnalyzeException ae) {
            throw new WSMessengerConfigurationException("Unable to register Web Service operation.", ae);
        } catch (Exception e) {
            throw new WSMessengerConfigurationException("Unable to establish Web Service messenger.", e);
        } finally {
        	Thread.currentThread().setContextClassLoader(originalClassLoader);
        }

        isInitilized = true;
        if (logger.isInfoEnabled()) {

            logger.info("Message validation on request is "+((validateMessageOnRequest)?"enabled.":"disabled."));
            logger.info("Message validation on response is "+((validateMessageOnResponse)?"enabled.":"disabled."));

            logger.info("Asynchronous messenger initilized for operation " + operationQName + " [service: " + serviceQName + "].");
        }
    }

    /**
     * Establish Axis2-specific configuration context. As an baseline the Xml
     * configuration file is used. It's assumed the basic configuration without
     * any module engagement in outside configuration or the additional steps
     * are necessary.
     * @return
     * @throws java.lang.Exception
     */
    private ConfigurationContext getConfigurationContext() throws ResourceException {
        ConfigurationContext cc = null;

		String clientAxis2XmlLocation = AsyncMessenger.class.getPackage()
				.getName().replaceAll("\\.", File.separator);
		
		URL axis2URL = AsyncMessenger.class.getClassLoader().getResource(clientAxis2XmlLocation
				+ File.separator + CLIENT_AXIS2_XML_RESOURCE);
		
        if (axis2URL == null) {
            throw new ResourceException("Unable to locate axis.xml configuration for client.");
        }

        String axis2Home = System.getenv(AXIS2_HOME_SYSTEM_PROPERTY);
        if (axis2Home == null || axis2Home.length() == 0) {
            throw new ResourceException("Unable to locate AXIS2 home directory with system property '" + AXIS2_HOME_SYSTEM_PROPERTY + "'.");
        }

        // loaded modules have to be written in the modules.list file
        String repositoryLocation = axis2Home + File.separator + AXIS2_REPOSITORY_NAME;
		try {
			cc = ConfigurationContextFactory
					.createConfigurationContextFromURIs(axis2URL, new URL(
							"file", "localhost", repositoryLocation));
		} catch (Exception e) {
			throw new ResourceException(
					"Unable to locate additional modules from repository directory.");
		}
        
        logger.debug("The baseline of configuration context is loaded from '" + axis2URL + "'.");
        logger.debug("The location of module repository '" + repositoryLocation + "'.");
        //cc.setProperty(SandeshaClientConstants.RM_SPEC_VERSION, Sandesha2Constants.SPEC_VERSIONS.v1_1);
        
        return cc;
    }

    /**
     * User-specific configuration of service messeneger and custom Axis2 module
     * options.
     * @throws org.jetel.ws.exception.WSMessengerConfigurationException
     */
    private void configureMessenger() throws WSMessengerConfigurationException {
        Options options = _serviceClient.getOptions();
        if (options == null) {
            options = new Options();
        }

        // configure PROXY settings
        if (proxyHostLocation != null) {
            ProxyProperties pp = new ProxyProperties();
            pp.setProxyName(proxyHostLocation);
            pp.setProxyPort(proxyHostPort);
            options.setProperty(HTTPConstants.PROXY, pp);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Using proxy host " + proxyHostLocation + ":" + proxyHostPort + " for message transmission.");
            }
        }

        // configure HTTP authentication access
        if (httpUsername != null && httpPassword != null) {
            HttpTransportProperties.Authenticator httpProperties = new HttpTransportProperties.Authenticator();
            // set realm, domain
            httpProperties.setUsername(httpUsername);
            httpProperties.setPassword(httpPassword);
            if (authDomain != null) httpProperties.setDomain(authDomain);
            if (authRealm != null) httpProperties.setRealm(authRealm);
            // use default authentication schema priority
            //httpProperties.setPreemptiveAuthentication(false);
            //httpProperties.setAuthSchemes(Collections.singletonList(HttpTransportProperties.Authenticator.DIGEST));
            
            options.setProperty(HTTPConstants.AUTHENTICATE, httpProperties);
            
            if (logger.isDebugEnabled()) {
				logger.debug("Setting the credentials username=\"" + httpUsername
						+ "\";password=\"" + httpPassword
						+ "\" for HTTP authentication schemas.");
            }
        }

        try {

            if (reliableMessagingEnabled) {
                options.setProperty(SandeshaClientConstants.OFFERED_SEQUENCE_ID, SandeshaUtil.getUUID());
            }
            
            options.setUseSeparateListener(asynchronousTransport);
            
            options.setTransportInProtocol(Constants.TRANSPORT_HTTP);
            options.setTimeOutInMilliSeconds(timeOutInMilliSeconds);
            //options.setExceptionToBeThrownOnSOAPFault(true);

            WSDLAnalyzer.SOAPVersion soapVersionSupported = wsdlAnalyzer.getSOAPVersion(operationQName.getLocalPart());
            if (soapVersionSupported == WSDLAnalyzer.SOAPVersion.SOAP12) {
                options.setSoapVersionURI(SOAP12Constants.SOAP_ENVELOPE_NAMESPACE_URI);
                logger.debug("SOAP version used for message definition: 1.2");
            } else {
                options.setSoapVersionURI(SOAP11Constants.SOAP_ENVELOPE_NAMESPACE_URI);
                logger.debug("SOAP version used for message definition: 1.1");
            }

            String action = wsdlAnalyzer.getSOAPAction(operationQName.getLocalPart());
            options.setAction(action);
            if (logger.isDebugEnabled()) {
                logger.debug("SOAP action used for operation " + operationQName + ": " + action);
            }
        } catch (WSDLAnalyzeException ae) {
            throw new WSMessengerConfigurationException("Unable to configure Web Service messenger with the information based on WSDL document.", ae);
        }

        _serviceClient.setOptions(options);

    }

    /**
     * Additional configuration of Axis2 modules to customize theirs behaviour
     * due to correct processing of async messenger. Support for extra namespaces
     * is provided.
     * @param axisConf
     */
    private void configureAxis2Modules(AxisConfiguration axisConf) {
        Collection<AxisModule> modules = axisConf.getModules().values();
        /*
         * Axis2 Addressing module doesn't support WS Addressing Policy
         * expressions hence any policy expressions regarding WS Addressing don't get
         * processed.
         */

        for (AxisModule module : modules) {
            List<String> extraPolicyNamespaces;

            String[] supportedPolicyNamespaces = module.getSupportedPolicyNamespaces();
            if (supportedPolicyNamespaces == null || supportedPolicyNamespaces.length == 0) {
                extraPolicyNamespaces = new ArrayList<String>();
            } else {
                List<String> policyNamespaces = Arrays.asList(module.getSupportedPolicyNamespaces());
                extraPolicyNamespaces = new ArrayList<String>(policyNamespaces.size());
                extraPolicyNamespaces.addAll(policyNamespaces);
            }

            if (SANDESHA_AXIS2_MODULE_NAME.equals(module.getName())) {
                extraPolicyNamespaces.add(WSDLExtensionNamespace.NS_URI_WS_RM);
                extraPolicyNamespaces.add(WSDLExtensionNamespace.NS_URI_SUN_RM);
            } else if (WS_ADRESSING_AXIS2_MODULE_NAME.equals(module.getName())) {
                extraPolicyNamespaces.add(WSDLExtensionNamespace.NS_URI_WS_ADDRESSING_v1_0_WSDL_BINDING);
                extraPolicyNamespaces.add(WSDLExtensionNamespace.NS_URI_WS_POLICY_v1_5);
            }
            
            module.setSupportedPolicyNamespaces(extraPolicyNamespaces.toArray(new String[extraPolicyNamespaces.size()]));
        }

        if (logger.isDebugEnabled()) {
            Axis2Utils.logSupportedNSByModules(axisConf);
        }
    }

    /**
     * Attach Policy expressions specified for Service Policy Subject, Endpoint Policy Subject,
     * Operation Policy Subject and Message Policy Subject to appropriate Axis2
     * representation of processed Web Service.
     */
    private void attachPoliciesToPolicySubjects() throws WSDLAnalyzeException {
        // Service Policy Subject
        List<Policy> servicePolicies = wsdlAnalyzer.getServiceEffectivePolicies(serviceQName.getLocalPart());
        PolicySubject policySubject = _service.getPolicySubject();
        for (Policy policy: servicePolicies) {
            policySubject.attachPolicy(policy);
            if (logger.isDebugEnabled()) {
                logger.debug("Policy expression with id $"+policy.getId()+"$ attached to Service Policy Subject.");
            }
        }
        
        // Endpoint Policy Subject
        List<Policy> endpointPolicies = wsdlAnalyzer.getEndpointEffectivePolicies(portQName);
        String endpointName = _service.getEndpointName();
        if (endpointName == null) {
            throw new WSDLAnalyzeException("No endpoint definition used for service " + _service.getName());
        }

        AxisEndpoint endpoint = _service.getEndpoint(endpointName);
        policySubject = endpoint.getPolicySubject();
        for (Policy policy: endpointPolicies) {
            policySubject.attachPolicy(policy);
            if (logger.isDebugEnabled()) {
                logger.debug("Policy expression with id $"+policy.getId()+"$ attached to Endpoint Policy Subject.");
            }
        }

        // Operation Policy Subject
        String bindingName = endpoint.getBinding().getName().getLocalPart();
        List<Policy> operationPolicies = wsdlAnalyzer.getOperationEffectivePolicies(bindingName,operationQName.getLocalPart());
        policySubject = _service.getOperation(operationQName).getPolicySubject();
        for (Policy policy: operationPolicies) {
            policySubject.attachPolicy(policy);
            if (logger.isDebugEnabled()) {
                logger.debug("Policy expression with id $"+policy.getId()+"$ attached to Operation Policy Subject.");
            }
        }

        // Message Policy Subject
    }

    public List<OMElement> sendPayloadElements(List<OMElement> payload) throws SendingMessegeException, MessageValidationException {
        List<OMElement> response = new ArrayList<OMElement>();
        QName requstQName = null;
        try {
            requstQName = wsdlAnalyzer.getOperationRequestQName(operationQName.getLocalPart());
        } catch (WSDLAnalyzeException ae) {
            throw new SendingMessegeException("Unable to identify request part for operation " + operationQName + ".", ae);
        }
        OMElement requestElement = createEmptyElement(requstQName);
        if (payload != null) {
            for (OMElement payloadItem : payload) {
                requestElement.addChild(payloadItem);
            }
        }
        logger.debug("Invoking operation >" + operationQName + "< in In-Out MEP.");
        OMElement responseElement = sendInOutMessage(requestElement);
        response.add(responseElement);

        return response;
    }

    public OMElement sendPayload(OMElement payload) throws SendingMessegeException, MessageValidationException {
        OMElement responseElement = null;
        OMElement requestElement = payload;
        
        if (payload == null) {
            throw new MessageValidationException("The request message is not specified.");
        }

        QName requestQName = null;
        try {
            requestQName = wsdlAnalyzer.getOperationRequestQName(operationQName.getLocalPart());
        } catch (WSDLAnalyzeException ae) {
            throw new SendingMessegeException("Unable to identify request part for operation " + operationQName + ".", ae);
        }
        
        // check the root element
        if (!requestQName.equals(payload.getQName())) {

            // create wrapper message
            NamespaceContext nsContext = payload.getXMLStreamReader().getNamespaceContext();
            String requestNSPrefix = nsContext.getPrefix(requestQName.getNamespaceURI());

            OMFactory omFactory = OMAbstractFactory.getOMFactory();
            if (requestNSPrefix == null) {
                requestNSPrefix = OMSerializerUtil.getNextNSPrefix();
            }
            OMNamespace requestNS = omFactory.createOMNamespace(requestQName.getNamespaceURI(), requestNSPrefix);
            requestElement = omFactory.createOMElement(requestQName.getLocalPart(), requestNS);
            requestElement.declareNamespace(requestNS);
            requestElement.addChild(payload);
            logger.debug("Input payload wrapped by request message.");
        }

        switch (operationMEP) {
            case InOut:
                logger.debug("Invoking operation >" + operationQName + "< in In-Out MEP.");
                responseElement = sendInOutMessage(requestElement);

                break;
            default:
                throw new SendingMessegeException("Invoking operation in not supported MEP type.");
        }

        return responseElement;
    }

    private OMElement sendInOutMessage(OMElement payload) throws SendingMessegeException, MessageValidationException {

        if (!isInitilized) {
            throw new SendingMessegeException("Web Service messenger is no initilized.");
        }

        OMElement response = null;
        try {
            if (reliableMessagingEnabled) {
                _serviceClient.getOptions().setProperty(SandeshaClientConstants.LAST_MESSAGE, "true");
            }
            // validate request message
            if (validateMessageOnRequest) {
                // retrieve the qualified name of message request element for current operation
                QName requestQName = wsdlAnalyzer.getOperationRequestQName(operationQName.getLocalPart());
                // retrieve schema definition with the message request element for current operation
                logger.debug("requestQName: "+requestQName);
                XmlSchema schema = wsdlAnalyzer.getTypesXMLSchema(requestQName);
                logger.debug("schema: "+schema+"; payload: "+payload);
                XmlMessageValidator.validateMessage(payload, schema);
                logger.debug("Request message for operation >" + operationQName + "< is valid.");
            }

            PureXMLCallbackHandler callback = new PureXMLCallbackHandler();
            
            _serviceClient.sendReceiveNonBlocking(operationQName, payload, callback);

            while (!interrupted && !callback.isCompleted()) {
                Thread.sleep(responseCheckUpTime);
            }

            if (interrupted) {
                // terminate connection
                logger.warn("Process of sending message was interrupted.");
                return null;
            }

            Exception e = callback.getException();
            if (e != null) {
                throw new SendingMessegeException(e);
            }
            response = callback.getPayload();

            if (response == null) {
                throw new SendingMessegeException("No response message received for InOut MEP.");
            }

            // validate response message
            if (validateMessageOnResponse && response != null) {
                // retrieve the qualified name of message response element for current operation
                QName responseQName = wsdlAnalyzer.getOperationResponseQName(operationQName.getLocalPart());
                // retrieve schema definition with the message response element for current operation
                XmlSchema schema = wsdlAnalyzer.getTypesXMLSchema(responseQName);

                XmlMessageValidator.validateMessage(response, schema);
                logger.debug("Response message for operation >" + operationQName + "< is valid.");
            }
        // TODO: implement transactions for rollback if exception thrown
        } catch (AxisFault af) {
            throw new SendingMessegeException("Unable to send messege to remote Web Service endpoint.", af);
        } catch(WSDLAnalyzeException ae) {
            throw new SendingMessegeException("Unable to send messege to remote Web Service endpoint.", ae);
        } catch(InterruptedException ie) {
            throw new SendingMessegeException("Unable to send messege to remote Web Service endpoint.", ie);
        }

        return response;
    }

    /**
     * Release all collected resource and terminate keeping connectios.
     * @throws org.jetel.ws.exception.WSMessengerConfigurationException
     */
    public void terminate() throws WSMessengerConfigurationException {

        if (configurationContext == null) {
            return;
        }
        
        try {
            ListenerManager listenerManager = configurationContext.getListenerManager();
            if (!listenerManager.isStopped()) {
                listenerManager.stop();
            }

            if (logger.isDebugEnabled()) {
                String listenerStatus = "running";
                if (listenerManager.isStopped()) {
                    listenerStatus = "stopped";
                }
                logger.debug("Transport listener manager status: " + listenerStatus);
            }

            _serviceClient.cleanup();
            logger.debug("Axis2 configured client was cleaned up.");
            
        } catch(AxisFault af) {
            throw new WSMessengerConfigurationException("Unable to terminate WS communication.", af);
        } finally {
            configurationContext = null;
        }
    }

    private QName correctNamespace(QName qName) {
        assert (qName != null);
        assert (wsdlAnalyzer != null);

        QName correctedQName = qName;
        if (qName.getNamespaceURI() == null || qName.getNamespaceURI().length() == 0) {
            String targetNamespace = wsdlAnalyzer.getTargetNamespace();
            correctedQName = new QName(targetNamespace, qName.getLocalPart());
        }

        return correctedQName;
    }

    public QName getOperationQName() {
        return operationQName;
    }

    public void setOperationQName(QName operationQName) {
        this.operationQName = operationQName;
        isInitilized = false;
    }

    private static OMElement createEmptyElement(QName elementQName) {
        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMElement element = factory.createOMElement(elementQName);

        return element;
    }

    public boolean isReliableMessagingEnabled() {
        return reliableMessagingEnabled;
    }

    public boolean isValidateMessageOnRequest() {
        return validateMessageOnRequest;
    }

    public void setValidateMessageOnRequest(boolean validateMessageOnRequest) {
        this.validateMessageOnRequest = validateMessageOnRequest;
    }

    public boolean isValidateMessageOnResponse() {
        return validateMessageOnResponse;
    }

    public void setValidateMessageOnResponse(boolean validateMessageOnResponse) {
        this.validateMessageOnResponse = validateMessageOnResponse;
    }

    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }

    public String getProxyHostLocation() {
        return proxyHostLocation;
    }

    public void setProxyHostLocation(String proxyHostLocation) {
        this.proxyHostLocation = proxyHostLocation;
    }

    public int getProxyHostPort() {
        return proxyHostPort;
    }

    public void setProxyHostPort(int proxyHostPort) {
        this.proxyHostPort = proxyHostPort;
    }

    public boolean isAsynchronousTransport() {
        return asynchronousTransport;
    }

    public void setAsynchronousTransport(boolean asynchronousTransport) {
        this.asynchronousTransport = asynchronousTransport;
    }

    public String getHttpUsername() {
		return httpUsername;
	}

	public void setHttpUsername(String httpUsername) {
		this.httpUsername = httpUsername;
	}

	public String getHttpPassword() {
		return httpPassword;
	}

	public void setHttpPassword(String httpPassword) {
		this.httpPassword = httpPassword;
	}

	public String getAuthDomain() {
		return authDomain;
	}

	public void setAuthDomain(String authDomain) {
		this.authDomain = authDomain;
	}

	public String getAuthRealm() {
		return authRealm;
	}

	public void setAuthRealm(String authRealm) {
		this.authRealm = authRealm;
	}

	public String getTrustJKSLocation() {
		return trustJKSLocation;
	}

	public void setTrustJKSLocation(String trustJKSLocation) {
		this.trustJKSLocation = trustJKSLocation;
	}

	public String getTrustJKSPassword() {
		return trustJKSPassword;
	}

	public void setTrustJKSPassword(String trustJKSPassword) {
		this.trustJKSPassword = trustJKSPassword;
	}

	public String getJKSLocation() {
		return JKSLocation;
	}

	public void setJKSLocation(String jKSLocation) {
		JKSLocation = jKSLocation;
	}

	public String getJKSPassword() {
		return JKSPassword;
	}

	public void setJKSPassword(String jKSPassword) {
		JKSPassword = jKSPassword;
	}
}

class PureXMLCallbackHandler implements AxisCallback {

    private boolean completed = false;
    private OMElement payload;
    private Exception exception = null;
    
    /*
     * onComplete is not called after onMessage or onFault due to bug;
     * it was fixed but did not show up in Axis2 1.4.1
     */
    public void onComplete() {
        synchronized (PureXMLCallbackHandler.this) {
            completed = true;
        }
    }

    public void onError(Exception e) {
        exception = e;
        synchronized (PureXMLCallbackHandler.this) {
            completed = true;
        }
    }

    public void onFault(MessageContext mContext) {
        exception = mContext.getFailureReason();
    }

    public void onMessage(MessageContext messageContext) {
        SOAPEnvelope envelope = messageContext.getEnvelope();
        payload = envelope.getBody().getFirstElement();

        synchronized (PureXMLCallbackHandler.this) {
            completed = true;

        }
    }

    public synchronized boolean isCompleted() {
        return completed;
    }

    public OMElement getPayload() {
        return payload;
    }

    public Exception getException() {
        return exception;
    }
}
