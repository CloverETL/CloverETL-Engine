package org.jetel.component.ws.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.namespace.QName;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.AxisDescription;
import org.apache.axis2.description.AxisEndpoint;
import org.apache.axis2.description.AxisModule;
import org.apache.axis2.description.AxisOperation;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.modules.Module;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.neethi.Assertion;
import org.apache.neethi.Policy;
import org.apache.neethi.PolicyComponent;

/**
 * Axis2-specific WS-Policy processor for engaging appropriate Axis2 modules.
 * Each Axis2 module is associated with various namespaces of policy assertions,
 * which the module offers support for. Policy assertions are defined at service,
 * operation and message level, so the module is engaged at level, where policy
 * assertion is found.
 * @author Pavel Pospichal
 */
public class WSDLBasedPolicyProcessor {

    private static final Log logger = LogFactory.getLog(WSDLBasedPolicyProcessor.class);
    
    private HashMap<String, List<AxisModule>> ns2AxisModules = new HashMap<String, List<AxisModule>>();

    public WSDLBasedPolicyProcessor(ConfigurationContext configurationCtx) {
        AxisConfiguration axisConfiguration = configurationCtx.getAxisConfiguration();
        for (Iterator<AxisModule> iterator = axisConfiguration.getModules().values().iterator(); iterator.hasNext();) {
            AxisModule axisModule = iterator.next();
            String[] namespaces = axisModule.getSupportedPolicyNamespaces();

            if (namespaces == null) {
                continue;
            }

            for (int i = 0; i < namespaces.length; i++) {
                List<AxisModule> moduleList = null;
                List<AxisModule> registeredAxisModules = ns2AxisModules.get(namespaces[i]);
                if (registeredAxisModules == null) {
                    moduleList = new ArrayList<AxisModule>(5);
                    ns2AxisModules.put(namespaces[i], moduleList);
                } else {
                    moduleList = registeredAxisModules;
                }
                moduleList.add(axisModule);

            }
        }
    }

    public void configureServicePolices(AxisService axisService) throws AxisFault {
        Iterator operations = axisService.getOperations();
        while (operations.hasNext()) {
            AxisOperation axisOp = (AxisOperation) operations.next();
            // TODO we support only operation level Policy now
            configureOperationPolices(axisOp);
        }
    }

    public Map<String, AxisModule> configureEndpointPolices(AxisEndpoint axisEndpoint) throws AxisFault {
        Collection<PolicyComponent> policyComponents = axisEndpoint.getPolicySubject().getAttachedPolicyComponents();
        /**
         * Due to Axis2 partial support for WS-Policy there is no way how to enagage
         * modules for Endpoint Policy Subject
         */
        AxisService axisService = axisEndpoint.getAxisService();

        return applyPolicy(axisService, policyComponents);
    }

    public Map<String, AxisModule> configureOperationPolices(AxisOperation axisOperation) throws AxisFault {
        Collection<PolicyComponent> policyComponents = axisOperation.getPolicySubject().getAttachedPolicyComponents();
        return applyPolicy(axisOperation, policyComponents);
    }

    private Map<String, AxisModule> applyPolicy(AxisDescription policySubject, Collection<PolicyComponent> policyComponents) throws AxisFault {
        Map<String, AxisModule> engagedModules = new HashMap<String, AxisModule>();

        for (PolicyComponent policyComponent : policyComponents) {
            if (!(policyComponent instanceof Policy)) {
                continue;
            }

            Policy attachedPolicy = (Policy) policyComponent;

            if (logger.isDebugEnabled()) {
                logger.debug("Applying WS-Policy [" + attachedPolicy.getId() + "] on " + policySubject.getClass().getName());
            }
            for (Iterator<List> alternativesIter = attachedPolicy.getAlternatives(); alternativesIter.hasNext();) {
                List<String> assertionNamespaces = new ArrayList<String>();
                Assertion assertion;

                /*
                 * Fist we compute the set of distinct namespaces of assertions
                 * of this particular policy alternative.
                 */
                for (Iterator<Assertion> assertionsIter = alternativesIter.next().iterator(); assertionsIter.hasNext();) {

                    assertion = assertionsIter.next();
                    QName name = assertion.getName();
                    String namespaceURI = name.getNamespaceURI();
                    logger.debug("Assertion found: " + assertion.getName());

                    if (!assertionNamespaces.contains(namespaceURI)) {
                        assertionNamespaces.add(namespaceURI);
                    }
                }
                logger.debug("Assertion namespaces: " + assertionNamespaces);

                /*
                 * Now we compute all the modules that are are involved in
                 * process assertions that belongs to any of the namespaces of
                 * list.
                 */
                List<AxisModule> modulesToEngage;

                for (String assertionNamespace : assertionNamespaces) {
                    modulesToEngage = ns2AxisModules.get(assertionNamespace);

                    if (modulesToEngage == null) {
                        /*
                         * If there isn't a single module that is not interested
                         * of assertions that belongs to a particular namespace,
                         * we simply ignore it.
                         */
                        logger.warn("Unable to find any module to process " + assertionNamespace + " type assertions");
                        // TODO: Log this ..
                        continue;

                    } else {
                        engageModulesToAxisDescription(modulesToEngage, policySubject);
                        for (AxisModule module: modulesToEngage) {
                            if (engagedModules.containsKey(module.getName())) {
                                continue;
                            }
                            engagedModules.put(module.getName(), module);
                        }
                    }
                }

            }

        }

        return engagedModules;
    }

    /**
     * Engages the list of Modules to the specified AxisDescription.
     */
    private void engageModulesToAxisDescription(List<AxisModule> modulesToEngage, AxisDescription axisDescription) throws AxisFault {
        String moduleName;
        Module module;

        for (AxisModule availableModule : modulesToEngage) {
            moduleName = availableModule.getName();
            module = availableModule.getModule();

            if (!axisDescription.isEngaged(moduleName)) {
                axisDescription.engageModule(availableModule);

                if (module != null) {
                    module.engageNotify(axisDescription);
                }
                
                if (logger.isDebugEnabled()) {
                    logger.debug("Module :" + availableModule.getName() + ": engaged for " + axisDescription.getClass().getName());
                }
            }
        }
    }

    private boolean canSupportAssertion(Assertion assertion, List<AxisModule> moduleList) {

        for (AxisModule axisModule : moduleList) {


            Module module = axisModule.getModule();

            if (!(module == null || module.canSupportAssertion(assertion))) {
                //logger.debug(axisModule.getName() + " says it can't support " + assertion.getName());
                return false;
            }
        }

        return true;
    }
}


