/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

import common.RuntimeEnv;
import java.io.IOException;
import static java.lang.Thread.sleep;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author Carlisle
 */
public class DemandProcessor implements Runnable {

    private String demandID = null;
    private int seq = 0;
    private String destMQ = null;
    private String seqKey = null;

    private boolean stop = false;
    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CentralProcessor.class.getName());
    }

    public DemandProcessor(String pDemandID) {
        demandID = pDemandID;
        seqKey = "seq_reduce_" + demandID;
    }

    public String getDemandID() {
        return demandID;
    }

    public void process() {

    }

    public void stop() {
        stop = true;
    }

    @Override
    public void run() {
        while (!CentralProcessor.isRunning());
        while (!stop) {
            try {
                sleep(10000);
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(DemandProcessor.class.getName()).log(Level.SEVERE, null, ex);
            }
            logger.info("Demand processor for " + demandID + " is running...");
        }

//        while (true) {
//            //get seq from redis
//            seq = Integer.parseInt(rc.get(seqKey));
//            logger.info("get reduce seq : " + seq + " from redis.");
//            
//            //read small granularity packages from redis, until packing 30 ones or processing time reaches 5 min
//            ResultContainer container = new ResultContainer(demandID);
//            long startTime = System.currentTimeMillis();
//            for (int i = seq + 30; seq < i; seq++) {
//                if (System.currentTimeMillis() - startTime > 300000) {
//                    break;
//                }
//                container.put(seq);
//            }
//            
//            try {
//                Message msg = new Message(destMQ, container.pack());
//                startTime = System.currentTimeMillis();
//                SendResult sr = producer.send(msg);
//                logger.debug("Send demand-" + demandID + " message to " + sr.getMessageQueue().getBrokerName() + ":" + sr.getQueueOffset() + ", status:" + sr.getSendStatus() + ", mq latency is " + (System.currentTimeMillis() - startTime));
//                
//            } catch (MQClientException ex) {
//                java.util.logging.Logger.getLogger(DemandProcessor.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (RemotingException ex) {
//                java.util.logging.Logger.getLogger(DemandProcessor.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (MQBrokerException ex) {
//                java.util.logging.Logger.getLogger(DemandProcessor.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (InterruptedException ex) {
//                java.util.logging.Logger.getLogger(DemandProcessor.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (IOException ex) {
//                java.util.logging.Logger.getLogger(DemandProcessor.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            
//            //register seq to redis
//            //rc.set(seqKey, seq);
//            
//        }
        logger.info("Demand processor for " + demandID + " is cancled...");
    }
}
