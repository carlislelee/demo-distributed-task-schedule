/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

import common.RuntimeEnv;
import config.Configuration;
import core.CentralProcessor;
import core.TaskScheduler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author Carlisle
 */
public class ReducerStartup {

    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ReducerStartup.class.getName());
    }

    public static void main(String args[]) {
        try {
            init();
            startup();
        } catch (Exception ex) {
            logger.error("starting reducer failed for " + ex.getMessage(), ex);
        }
        System.exit(0);
    }

    public static void init() throws Exception {
        String configurationFileName = "config.properties";
        logger.info("initializing stat reducer...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading configuration from " + configurationFileName + " failed.");
        }

        logger.info("initializng runtime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment failed");
        }
        logger.info("initialize runtime enviroment successfully");

        TaskScheduler.initialize();
        
        CentralProcessor.initialize();

        logger.info("intialize stat reducer successfully");
    }

    public static void startup() throws InterruptedException {
        CentralProcessor.startup();
    }

    public static void stop() {

    }
}
