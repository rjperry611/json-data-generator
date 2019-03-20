/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.data.json.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.acesinc.data.json.generator.config.JSONConfigReader;
import net.acesinc.data.json.generator.config.SimulationConfig;
import net.acesinc.data.json.generator.config.WorkflowConfig;
import net.acesinc.data.json.generator.log.EventLogger;
import net.acesinc.data.json.generator.workflow.Workflow;

/**
 *
 * @author andrewserff
 */
public class SimulationRunner {

    private static final Logger log = LogManager.getLogger(SimulationRunner.class);
    private final SimulationConfig config;
    private final List<EventGenerator> eventGenerators;
    private final List<Thread> eventGenThreads;
    private boolean running;
    private final List<EventLogger> eventLoggers;

    public SimulationRunner(SimulationConfig config, List<EventLogger> loggers) {
        this.config = config;
        this.eventLoggers = loggers;
        this.eventGenerators = new ArrayList<EventGenerator>();
        this.eventGenThreads = new ArrayList<Thread>();

        setupSimulation();
    }

    private void setupSimulation() {
        this.running = false;
        for (WorkflowConfig workflowConfig : this.config.getWorkflows()) {
            try {
                Workflow w = JSONConfigReader.readConfig(
                        this.getClass().getClassLoader().getResourceAsStream(workflowConfig.getWorkflowFilename()), Workflow.class);
                final EventGenerator gen = new EventGenerator(w, workflowConfig, this.eventLoggers);
                log.info("Adding EventGenerator for [ " + workflowConfig.getWorkflowName() + ","
                        + workflowConfig.getWorkflowFilename() + " ]");
                this.eventGenerators.add(gen);
                this.eventGenThreads.add(new Thread(gen));
            } catch (IOException ex) {
                log.error("Error reading config: " + workflowConfig.getWorkflowName(), ex);
            }
        }
    }

    public void startSimulation() {
        log.info("Starting Simulation");

        if (this.eventGenThreads.size() > 0) {
            for (Thread t : this.eventGenThreads) {
                t.start();
            }
            this.running = true;
        }
    }

    public void stopSimulation() {
        log.info("Stopping Simulation");
        for (Thread t : this.eventGenThreads) {
            t.interrupt();
        }
        for (EventLogger l : this.eventLoggers) {
            l.shutdown();
        }
        this.running = false;
    }

    public boolean isRunning() {
        return this.running && anyGeneratorsRunning();
    }

    public boolean anyGeneratorsRunning() {
        for(EventGenerator eventGenorator : eventGenerators) {
            if(eventGenorator.isRunning()) {
                return true;
            }
        }
        return false;
    }

}
