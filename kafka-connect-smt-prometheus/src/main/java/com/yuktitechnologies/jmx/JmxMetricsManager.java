package com.yuktitechnologies.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class JmxMetricsManager {

    private static final Logger logger = LoggerFactory.getLogger(JmxMetricsManager.class);

    private final MBeanServer mbeanServer;
    private ObjectName jmxObjectName;

    public JmxMetricsManager() {
        this.mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    public void registerMetrics(PiiMetrics metrics, String instanceId) {
        String typeName = instanceId.contains("PROTOBUF") ? "ProtobufMetrics" : "JsonMetrics";
        String mbeanName = String.format("com.yuktitechnologies:type=%s,name=SmtLatencyTracker,id=%s",
                typeName, instanceId);

        try {
            this.jmxObjectName = new ObjectName(mbeanName);
            if (!mbeanServer.isRegistered(jmxObjectName)) {
                mbeanServer.registerMBean(metrics, jmxObjectName);
                logger.info("Successfully registered JMX MBean [{}] for instance [ID: {}]", mbeanName, instanceId);
            }
        } catch (MalformedObjectNameException e) {
            logger.error("[ALERT_JMX_FAILURE] Invalid JMX ObjectName format: {}. " +
                    "Monitoring disabled for instance [ID: {}].", mbeanName, instanceId, e);
        } catch (InstanceAlreadyExistsException e) {
            logger.warn("JMX MBean [{}] already registered. Bypassing registration for instance [ID: {}].",
                    mbeanName, instanceId);
        } catch (MBeanRegistrationException | NotCompliantMBeanException e) {
            logger.error("[ALERT_JMX_FAILURE] Failed to register JMX MBean [{}]. Monitoring disabled.", mbeanName, e);
        }
    }

    public void unregisterMetrics() {
        if (jmxObjectName != null) {
            try {
                if (mbeanServer.isRegistered(jmxObjectName)) {
                    mbeanServer.unregisterMBean(jmxObjectName);
                    logger.info("Successfully unregistered JMX MBean: {}", jmxObjectName);
                }
            } catch (InstanceNotFoundException e) {
                logger.warn("Attempted to unregister JMX MBean [{}], but it was not found.", jmxObjectName);
            } catch (MBeanRegistrationException e) {
                logger.error("[ALERT_JMX_LEAK] Failed to unregister JMX MBean [{}]. " +
                        "Potential memory leak on connector reload.", jmxObjectName, e);
            }
        }
    }
}