/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.data.json.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.acesinc.data.json.generator.config.JSONConfigReader;
import net.acesinc.data.json.generator.config.SimulationConfig;
import net.acesinc.data.json.generator.log.EventLogger;
import net.acesinc.data.json.generator.log.EventLoggerFactory;

/**
 *
 * @author andrewserff
 */
public class JsonDataGenerator {

	private static final Logger log = LogManager.getLogger(JsonDataGenerator.class);

	private SimulationRunner simRunner;
	private String simConfigFile;

	public JsonDataGenerator(String simConfigString) {
		this.simConfigFile = simConfigString;
		try {
			log.debug("Creating Simulation Runner using Simulation Config [ " + simConfigString + " ]");
			SimulationConfig simConfig = getSimConfig();
			EventLoggerFactory.getInstance().configure(simConfig);
			List<EventLogger> loggers = new ArrayList<>();
			for (Map<String, Object> elProps : simConfig.getProducers()) {
				String elType = (String) elProps.get("type");
				EventLogger logger;
				try {
					logger = EventLoggerFactory.getInstance().getEventLogger(elType, elProps);
					loggers.add(logger);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (loggers.isEmpty()) {
				throw new IllegalArgumentException("You must configure at least one Producer in the Simulation Config");
			}
			this.simRunner = new SimulationRunner(simConfig, loggers);
		} catch (IOException ex) {
			log.error("Error getting Simulation Config [ " + simConfigString + " ]", ex);
		}
	}

	public void startRunning() {
		this.simRunner.startSimulation();
	}

	public void stopRunning() {
		this.simRunner.stopSimulation();
	}

	private SimulationConfig getSimConfig() throws IOException {
		return JSONConfigReader.readConfig(this.getClass().getClassLoader().getResourceAsStream(this.simConfigFile),
				SimulationConfig.class);
	}

	public boolean isRunning() {
		return this.simRunner.isRunning();
	}

	public static void main(String[] args) {
		String simConfig = "defaultSimConfig.json";
		if (args.length > 0) {
			simConfig = args[0];
			log.info("Overriding Simulation Config file from command line to use [ " + simConfig + " ]");
		}

		final JsonDataGenerator gen = new JsonDataGenerator(simConfig);

		final Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("Shutdown Hook Invoked.  Shutting Down Loggers");
				gen.stopRunning();
				try {
					mainThread.join();
				} catch (InterruptedException ex) {
					// oh well
				}
			}
		});

		gen.startRunning();
		while (gen.isRunning()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				// wakie wakie!
			}
		}

	}

	/**
	 * @return the simConfigFile
	 */
	public String getSimConfigFile() {
		return this.simConfigFile;
	}

	/**
	 * @param simConfigFile the simConfigFile to set
	 */
	public void setSimConfigFile(String simConfigFile) {
		this.simConfigFile = simConfigFile;
	}

}
