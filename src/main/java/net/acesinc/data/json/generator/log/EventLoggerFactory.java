/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.data.json.generator.log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

import net.acesinc.data.json.generator.config.SimulationConfig;

/**
 *
 * @author andrewserff
 */
public class EventLoggerFactory {

    private static final Logger log = LogManager.getLogger(EventLoggerFactory.class);
    private static final String EVENT_LOGGERS_DEFAULT_PATH = "net.acesinc.data.json.generator.log";

    private boolean configured = false;
    private static EventLoggerFactory instance;
    private final Map<String, Class> eventLoggerNameMap;
    private final Map<String, EventLogger> eventLoggerCache;

    private static final ThreadLocal<EventLoggerFactory> localInstance = new ThreadLocal<EventLoggerFactory>() {
	@Override
	protected EventLoggerFactory initialValue() {
	    return new EventLoggerFactory();
	}
    };

    private EventLoggerFactory() {
	this.eventLoggerNameMap = new LinkedHashMap<>();
	this.eventLoggerCache = new LinkedHashMap<>();
	scanForEventLoggers(EVENT_LOGGERS_DEFAULT_PATH);
    }

    public static EventLoggerFactory getInstance() {
	return localInstance.get();
    }

    /**
     * Allows the type handler factory to be configured from the WorkflowConfig.
     * This will only configure itself once per thread. Any additional call to
     * config will be ignored.
     *
     * @param simConfig
     */
    public void configure(SimulationConfig simConfig) {
	if (!this.configured) {
	    for (String packageName : simConfig.getCustomEventLoggers()) {
		scanForEventLoggers(packageName);
	    }
	    this.configured = true;
	}
    }

    private void scanForEventLoggers(String packageName) {
	Reflections reflections = new Reflections(packageName);
	Set<Class<? extends EventLogger>> subTypes = reflections.getSubTypesOf(EventLogger.class);
	for (Class type : subTypes) {
	    // first, make sure we aren't trying to create an abstract class
	    if (Modifier.isAbstract(type.getModifiers())) {
		continue;
	    }
	    try {
		Object o = type.newInstance();
		Method nameMethod = o.getClass().getMethod("getName");
		nameMethod.setAccessible(true);

		String eventLoggerName = (String) nameMethod.invoke(o);
		this.eventLoggerNameMap.put(eventLoggerName, type);
		log.debug("Discovered EventLogger [ " + eventLoggerName + "," + type.getName() + " ]");
	    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException
		    | IllegalArgumentException | InvocationTargetException ex) {
		log.warn("Error instantiating EventLogger class [ " + type.getName()
			+ " ]. It will not be available during processing.", ex);
	    }
	}
    }

    public EventLogger getEventLogger(String name, Map<String, Object> props) throws Exception {
	EventLogger logger = this.eventLoggerCache.get(name);
	if (logger == null) {
	    Class handlerClass = this.eventLoggerNameMap.get(name);
	    if (handlerClass != null) {
		try {
		    logger = (EventLogger) handlerClass.newInstance();
		    logger.setLoggerProps(props);

		    this.eventLoggerCache.put(name, logger);
		} catch (Exception ex) {
		    log.warn("Error instantiating TypeHandler class [ " + handlerClass.getName() + " ]", ex);
		}
	    }
	} else {
	    logger.setLoggerProps(props);
	}
	return logger;
    }

    /*
     * public static void main(String[] args) { Map<String, Object> vals = new
     * LinkedHashMap<>(); EventLogger random = LoggerHandlerFactory.getInstance().
     * getEventLogger("random('one', 'two', 'three')", vals, ""); if (random ==
     * null) { log.error("error getting handler"); } else {
     * log.info("success! random value: " + random.getNextRandomValue()); } }
     */
}
