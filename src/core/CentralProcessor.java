/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author Carlisle
 */
public class CentralProcessor {
    
    private static final int RESULT_QUEUE_SIZE = 1024;
    private static Map<String, DemandProcessor> demandProcessors = new HashMap<String, DemandProcessor>();
    
    private static boolean stop = true;
    private static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CentralProcessor.class.getName());
    }
    
    public CentralProcessor() {
        
    }
    
    public static void initialize() {
        logger.info("Initializing central processor...");
        
        logger.info("Initialize central processor successfully.");
    }
    
    public static void startup() throws InterruptedException {
        stop = false;
        while (!stop) {
            logger.info("【】" + demandProcessors.keySet());
            Thread.sleep(10000);
        }
        logger.info("Central Processor stoppped.");
    }
    
    public static boolean ifExistsDemand(String demandID) {
        return demandProcessors.containsKey(demandID);
    }
    
    public static void registerDemand(String demandID) {
        DemandProcessor dp = new DemandProcessor(demandID);
        demandProcessors.put(demandID, dp);
        Thread t = new Thread(dp);
        t.setName("Demand-" + demandID + "-Processor");
        t.start();
        logger.info("Start processor for demand-" + demandID + " sucessfully.");
    }
    
    public static void removeDemand(String demand) {
        DemandProcessor dp = demandProcessors.get(demand);
        dp.stop();
        demandProcessors.remove(demand);
        logger.info("Processor for demand-" + demand + " stopped.");
    }
    
    public static boolean isRunning() {
        return !stop;
    }
    
    private class ResultSender implements Runnable {
        
        @Override
        public void run() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
    
    public static void stop() {
        for (String d : demandProcessors.keySet()) {
            demandProcessors.get(d).stop();
        }
        stop = true;
    }
}
