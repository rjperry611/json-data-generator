package net.acesinc.data.json.generator.log;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nats.client.Nats;
import nats.client.NatsConnector;
import net.acesinc.data.json.util.JsonUtils;

/**
 * Created by betselot on 6/27/16.
 */
public class NatsLogger implements EventLogger {
    private static final Logger log = LogManager.getLogger(NatsLogger.class);

    public static final String NATS_SERVER_PROP_NAME = "broker.server";
    public static final String NATS_PORT_PROP_NAME = "broker.port";

    private String topic;
    private boolean sync;
    private boolean flatten;
    private JsonUtils jsonUtils;
    private Nats nats;
    private final NatsConnector natsConnector = new NatsConnector();
    StringBuilder natsURL = new StringBuilder("nats://");

    public NatsLogger() {
        super();
    }

    @Override
    public void setLoggerProps(Map<String, Object> props) {
        String brokerHost = (String) props.get(NATS_SERVER_PROP_NAME);
        Integer brokerPort = (Integer) props.get(NATS_PORT_PROP_NAME);

        this.natsURL.append(brokerHost);
        this.natsURL.append(":");
        this.natsURL.append(brokerPort);

        this.nats = this.natsConnector.addHost(this.natsURL.toString()).connect();

        this.topic = props.get("topic").toString();
        this.sync = (Boolean) props.get("sync");
        this.flatten = (Boolean) props.get("flatten");
        this.jsonUtils = new JsonUtils();

    }

    @Override
    public void logEvent(String event, Map<String, Object> producerConfig) {
        logEvent(event);
    }

    private void logEvent(String event) {
        String output = event;
        if (this.flatten) {
            try {
                output = this.jsonUtils.flattenJson(event);
            } catch (IOException ex) {
                log.error("Error flattening json. Unable to send event [ " + event + " ]", ex);
                return;
            }
        }

        log.debug("Sending event to [" + this.topic + "] on gnatsd: [ " + output + " ]");
        this.nats.publish(this.topic, output);
    }

    @Override
    public void shutdown() {
        this.nats.close();
    }

    @Override
    public String getName() {
        return "nats";
    }
}
