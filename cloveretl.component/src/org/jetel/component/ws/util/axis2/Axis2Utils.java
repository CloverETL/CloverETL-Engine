
package org.jetel.component.ws.util.axis2;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.AxisBinding;
import org.apache.axis2.description.AxisBindingMessage;
import org.apache.axis2.description.AxisBindingOperation;
import org.apache.axis2.description.AxisDescription;
import org.apache.axis2.description.AxisEndpoint;
import org.apache.axis2.description.AxisMessage;
import org.apache.axis2.description.AxisModule;
import org.apache.axis2.description.AxisOperation;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.wsdl.WSDLConstants;
import org.apache.axis2.wsdl.WSDLUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.neethi.Assertion;
import org.apache.neethi.Constants;
import org.apache.neethi.Policy;
import org.apache.neethi.PolicyComponent;

/**
 *
 * @author Pavel Pospichal
 */
public class Axis2Utils {

    private static final Log logger = LogFactory.getLog(Axis2Utils.class);

    public static void logWSPolicyOccurence(AxisService service, QName operationQName) throws Exception {
        assert (service != null);
        assert (operationQName != null);

        Collection<PolicyComponent> policyComponents = service.getPolicySubject().getAttachedPolicyComponents();
        if (policyComponents != null) {
            logger.debug("WS-Policy definitions for service #" + service.getName() + "#");
            logWSPolicyComponents(policyComponents);
        }

        AxisOperation selectedOperation = service.getOperation(operationQName);
        if (selectedOperation == null) {
            logger.warn("Operation is not defined #" + operationQName + "#");
            return;
        }
        policyComponents = selectedOperation.getPolicySubject().getAttachedPolicyComponents();
        if (policyComponents != null) {
            logger.debug("WS-Policy definitions for operation #" + selectedOperation.getName() + "#");
            logWSPolicyComponents(policyComponents);
        }
        
        AxisMessage messageOut = selectedOperation.getMessage(WSDLConstants.MESSAGE_LABEL_OUT_VALUE);
        if (messageOut != null) {
            policyComponents = messageOut.getPolicySubject().getAttachedPolicyComponents();
            if (policyComponents != null) {
                logger.debug("WS-Policy definitions for out message #" + messageOut.getName() + "#");
                logWSPolicyComponents(policyComponents);
            }
        }

        AxisMessage messageIn = selectedOperation.getMessage(WSDLConstants.MESSAGE_LABEL_IN_VALUE);
        if (messageIn != null) {
            policyComponents = messageIn.getPolicySubject().getAttachedPolicyComponents();
            if (policyComponents != null) {
                logger.debug("WS-Policy definitions for in message #" + messageIn.getName() + "#");
                logWSPolicyComponents(policyComponents);
            }
        }

        AxisMessage messageFault = selectedOperation.getMessage(WSDLConstants.MESSAGE_LABEL_FAULT_VALUE);
        if (messageFault != null) {
            policyComponents = messageFault.getPolicySubject().getAttachedPolicyComponents();
            if (policyComponents != null) {
                logger.debug("WS-Policy definitions for fault message #" + messageFault.getName() + "#");
                logWSPolicyComponents(policyComponents);
            }
        }
            
    }

    private static void logWSPolicyComponents(Collection<PolicyComponent> policyComponents) throws Exception {
        assert (policyComponents != null);

        StringBuffer logBuffer = new StringBuffer();
        ByteArrayOutputStream output = null;
        try {
            output = new ByteArrayOutputStream();
            XMLStreamWriter xmlWriter = prepareXMLStreamWriter(output);

            for (PolicyComponent policyComponent : policyComponents) {

                switch (policyComponent.getType()) {
                    case Constants.TYPE_ASSERTION:
                        logBuffer.append("\nPolicy component of type ASSERTION found.");
                        break;
                    case Constants.TYPE_POLICY:
                        logBuffer.append("\nPolicy component of type POLICY found.");
                        break;
                    case Constants.TYPE_POLICY_REF:
                        logBuffer.append("\nPolicy component of type POLICY_REF found.");
                        break;
                    case Constants.TYPE_ALL:
                        logBuffer.append("\nPolicy component of type ALL found.");
                        break;
                    case Constants.TYPE_EXACTLYONE:
                        logBuffer.append("\nPolicy component of type EXACTLYONE found.");
                        break;
                    default:
                        logger.warn("Unknown Policy component of class " + policyComponent.getClass().getName() + " found.");
                        break;
                }
                policyComponent.serialize(xmlWriter);
                logBuffer.append("\nDefinition:\n" + output.toString());
                output.reset();
            }
        } finally {
            if (output != null) {
                output.close();
            }
        }
        logger.debug(logBuffer.toString());
    }

    private static XMLStreamWriter prepareXMLStreamWriter(OutputStream outputStream) throws Exception {
        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        
        XMLStreamWriter writer = outputFactory.createXMLStreamWriter(outputStream);

        return writer;
    }
    
    public static void logModulesEngagement(AxisService service, QName operationQName) {
        
    }

    private static void logModulesEngagementForComponent(AxisDescription description) {
        assert (description != null);

        StringBuffer buffer = new StringBuffer();
        Collection<AxisModule> availableModules = description.getAxisConfiguration().getModules().values();
        for (AxisModule availableModule : availableModules) {
            buffer.append("Axis2 module " + availableModule.getName() + "(" + availableModule.getVersion() + "): ");
            if (description.isEngaged(availableModule)) {
                buffer.append("ENGAGED");
            } else {
                buffer.append("DISENGAGED");
            }
        }

        logger.debug(buffer.toString());
    }

    public static void logSupportedNSByModules(AxisConfiguration configuration) {
        assert (configuration != null);

        StringBuffer buffer = new StringBuffer();

        Collection<AxisModule> modules = configuration.getModules().values();
        for (AxisModule module : modules) {
            buffer.append("\nAxis2 module " + module.getName() + "(" + module.getVersion() + ") supports NS:");

            String[] supportedPolicyNamespaces = module.getSupportedPolicyNamespaces();
            if (supportedPolicyNamespaces != null) {
                for (int i = 0; i < supportedPolicyNamespaces.length; i++) {
                    buffer.append("\n" + supportedPolicyNamespaces[i]);
                }
            } else {
                buffer.append("\n<none>");
            }
        }

        logger.debug(buffer.toString());
    }

    public static Policy getApplicablePolicy(AxisDescription axisDescription) {
        if (axisDescription instanceof AxisMessage) {
            AxisMessage axisMessage = (AxisMessage) axisDescription;
            AxisOperation axisOperation = axisMessage.getAxisOperation();
            if (axisOperation != null) {
                AxisService axisService = (AxisService) axisOperation.getAxisService();
                if (axisService != null) {
                    if (axisService.getEndpointName() != null) {
                        AxisEndpoint axisEndpoint = axisService.getEndpoint(axisService.getEndpointName());
                        if (axisEndpoint != null) {
                            AxisBinding axisBinding = axisEndpoint.getBinding();
                            AxisBindingOperation axisBindingOperation = (AxisBindingOperation) axisBinding.getChild(axisOperation.getName());
                            String direction = axisMessage.getDirection();
                            AxisBindingMessage axisBindingMessage = null;
                            if (WSDLConstants.WSDL_MESSAGE_DIRECTION_IN.equals(direction) && WSDLUtil.isInputPresentForMEP(axisOperation.getMessageExchangePattern())) {
                                axisBindingMessage = (AxisBindingMessage) axisBindingOperation.getChild(WSDLConstants.MESSAGE_LABEL_IN_VALUE);
                                return axisBindingMessage.getEffectivePolicy();

                            } else if (WSDLConstants.WSDL_MESSAGE_DIRECTION_OUT.equals(direction) && WSDLUtil.isOutputPresentForMEP(axisOperation.getMessageExchangePattern())) {
                                axisBindingMessage = (AxisBindingMessage) axisBindingOperation.getChild(WSDLConstants.MESSAGE_LABEL_OUT_VALUE);
                                return axisBindingMessage.getEffectivePolicy();
                            }
                        }

                    }
                }
            }
            return ((AxisMessage) axisDescription).getEffectivePolicy();
        }
        return null;
    }

    private void engageModulesForPolicy(AxisDescription axisDescription,
            Policy policy, AxisConfiguration axisConfiguration)
            throws AxisFault {
        Iterator iterator = policy.getAlternatives();
        if (!iterator.hasNext()) {
            throw new AxisFault(
                    "Policy doesn't contain any policy alternatives");
        }

        List assertionList = (List) iterator.next();

        Assertion assertion;
        String namespaceURI;
        List moduleList;

        for (Iterator assertions = assertionList.iterator(); assertions.hasNext();) {
            assertion = (Assertion) assertions.next();
            namespaceURI = assertion.getName().getNamespaceURI();
            System.out.println("assertion namespaceURI: " + namespaceURI);
            System.out.println("assertion localPart: " + assertion.getName().getLocalPart());

            moduleList = axisConfiguration.getModulesForPolicyNamesapce(namespaceURI);

            if (moduleList == null) {
                logger.debug("can't find any module to process " + assertion.getName() + " type assertions");
                continue;
            }

            for (Object modulesItem : moduleList) {
                AxisModule module = (AxisModule) modulesItem;
                System.out.println("module for NS: " + module.getName());
            }
        }

    }
}
