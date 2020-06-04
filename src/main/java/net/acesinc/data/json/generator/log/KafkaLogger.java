/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.data.json.generator.log;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.acesinc.data.json.util.JsonUtils;

/**
 * @author andrewserff
 */
public class KafkaLogger implements EventLogger {

	private static final Logger log = LogManager.getLogger(KafkaLogger.class);
	public static final String BROKER_SERVER_PROP_NAME = "broker.server";
	public static final String BROKER_PORT_PROP_NAME = "broker.port";
	public static final String KERBEROS_CONF = "kerberos";

	private KafkaProducer<String, String> producer;
	private String topic;
	private boolean sync;
	private boolean flatten;
	private final Properties props = new Properties();
	private JsonUtils jsonUtils;

	public KafkaLogger() {
		super();
	}

	@Override
	public void setLoggerProps(Map<String, Object> props) {
		String brokerHost = (String) props.get(BROKER_SERVER_PROP_NAME);
		Integer brokerPort = (Integer) props.get(BROKER_PORT_PROP_NAME);

		this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		String bootstrapServerAsString = "";

		if (props.get(KERBEROS_CONF) != null) {
			Map<String, String> kerberosConf = (Map<String, String>) props.get(KERBEROS_CONF);

			bootstrapServerAsString = kerberosConf.get("kafka.brokers.servers");
			this.props.put("security.protocol", kerberosConf.get("kafka.security.protocol"));
			this.props.put("sasl.kerberos.service.name", kerberosConf.get("kafka.service.name"));
			System.setProperty("java.security.auth.login.config", kerberosConf.get("kafka.jaas.file"));
			System.setProperty("java.security.krb5.conf", kerberosConf.get("kerberos.conf.file"));
		} else {
			bootstrapServerAsString = brokerHost + ":" + brokerPort.toString();
		}
		this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAsString);

		this.producer = new KafkaProducer<>(this.props);

		this.topic = (String) props.get("topic");
		if (props.get("sync") != null) {
			this.sync = (Boolean) props.get("sync");
		} else {
			this.sync = false;
		}

		if (props.get("flatten") != null) {
			this.flatten = (Boolean) props.get("flatten");
		} else {
			this.flatten = false;
		}

		this.jsonUtils = new JsonUtils();
	}

	@Override
	public void logEvent(String event, Map<String, Object> producerConfig) {
		logEvent(event);
	}

	private void logEvent(String event) {
		boolean sync = false;

		String output = event;
		if (this.flatten) {
			try {
				output = this.jsonUtils.flattenJson(event);
			} catch (IOException ex) {
				log.error("Error flattening json. Unable to send event [ " + event + " ]", ex);
				return;
			}
		}

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topic, output);
		if (sync) {
			try {
				this.producer.send(producerRecord).get();
			} catch (InterruptedException | ExecutionException ex) {
				// got interrupted while waiting
				log.warn("Thread interrupted while waiting for synchronous response from producer", ex);
			}
		} else {
			log.debug("Sending event to Kafka: [ " + output + " ]");
			this.producer.send(producerRecord);
		}
	}

	@Override
	public void shutdown() {
		this.producer.close();
	}

	@Override
	public String getName() {
		return "kafka";
	}

}
