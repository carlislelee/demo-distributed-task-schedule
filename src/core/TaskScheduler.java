/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

import common.RuntimeEnv;
import consistenthashing.DynamicAllocate;
import consistenthashing.NodeLocator;
import consistenthashing.RNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author Carlisle
 */
public class TaskScheduler {

    private static String localip = null;
    private static String role = null;
    private static List<String> demandList = null;
    private static ZkClient zc = null;
    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(TaskScheduler.class.getName());
    }

    public static void initialize() {

        localip = (String) RuntimeEnv.getParam(RuntimeEnv.LOCAL_IP);
        demandList = (ArrayList<String>) RuntimeEnv.getParam(RuntimeEnv.DEMAND_LIST);
        zc = RuntimeEnv.zc;

        //init task status
        if (!zc.exists("/ulss/stat/reducer/task")) {
            zc.createPersistent("/ulss/stat/reducer/task");
        }
        if (!zc.exists("/ulss/stat/reducer/task/" + localip)) {
            zc.createPersistent("/ulss/stat/reducer/task/" + localip);
        }
        for (String d : demandList) {
            if (!zc.exists("/ulss/stat/reducer/task/" + localip + "/" + d)) {
                zc.createPersistent("/ulss/stat/reducer/task/" + localip + "/" + d, "WAITING");
            } else {
                zc.writeData("/ulss/stat/reducer/task/" + localip + "/" + d, "WAITING");
            }
            zc.subscribeDataChanges("/ulss/stat/reducer/task/" + localip + "/" + d, new TaskMonitor());
        }
        //elect master
        electMaster();

        scheduleTask();

        logger.info("Initialize task scheduler successfully!");
    }

    public static void electMaster() {
        logger.info("Begin master election...");
        if (!zc.exists("/ulss/stat/reducer/m")) {
            zc.createPersistent("/ulss/stat/reducer/m");
        }
        zc.createEphemeralSequential("/ulss/stat/reducer/m/lock-", localip);
        List<String> nodeList = zc.getChildren("/ulss/stat/reducer/m");
        if (nodeList.isEmpty()) {
            logger.error("No reducer server is running, it may be caused by zookeeper client.");
        }
        int masterSeq = Integer.MAX_VALUE;
        String masterZKNode = "";
        for (String n : nodeList) {
            int seq = Integer.parseInt(n.split("-")[1]);
            if (seq < masterSeq) {
                masterSeq = seq;
                masterZKNode = n;
            }
        }
        if (localip.equals(zc.readData("/ulss/stat/reducer/m/" + masterZKNode))) {
            role = "master";
        } else {
            role = "slave";
        }
        logger.info("Master election complete: master is " + zc.readData("/ulss/stat/reducer/m/" + masterZKNode));
    }

    public static void scheduleTask() {
        try {
            if (role.equals("slave")) {
                logger.info("I'm slave, I have to wait for master to arrange task.");
                return;
            }
            logger.info("Aha, I'm master!I'm going to arrange task for you poor slaves!");
            List<String> ipList = zc.getChildren("/ulss/stat/reducer/cluster");
            if (ipList.isEmpty()) {
                return;
            }
            if (ipList.size() == 1 && ipList.contains(localip)) {
                //if this is the only node in cluster
                logger.info("No slave is available, I have to deal with all demands myself...");
                if (!CentralProcessor.isRunning()) {
                    resetTaskStatus(zc.getChildren("/ulss/stat/reducer/task"));
                }
                for (String demand : demandList) {
                    if (!CentralProcessor.ifExistsDemand(demand)) {
                        zc.writeData("/ulss/stat/reducer/task/" + localip + "/" + demand, "READY");
                    }
                }
            } else {
                //register nodes on consistenthashing circle
                List<RNode> nodeList = new ArrayList<RNode>();
                for (String ip : ipList) {
                    nodeList.add(new RNode(ip));
                }
                NodeLocator nodelocator = null;
                DynamicAllocate dynamicallocate = new DynamicAllocate();
                dynamicallocate.setNodes(nodeList);
                nodelocator = dynamicallocate.getMD5NodeLocator();

                //get previous demand - node relation
                Map<String, String> prevDemand2IP = new HashMap<String, String>();//<demandID, nodeIP>
                for (String ip : ipList) {
                    for (String d : demandList) {
                        if (zc.readData("/ulss/stat/reducer/task/" + ip + "/" + d).equals("RUNNING")) {
                            prevDemand2IP.put(d, ip);
                        }
                    }
                }
                if (prevDemand2IP.size() != demandList.size()) {
                    logger.error("Current running demands num does not equals total demand num.");
                    throw new Exception("Schedule Excetion. ");
                }

                logger.debug("*" + prevDemand2IP);

                Map<String, String> taskToMove = new HashMap<String, String>();
                for (String d : demandList) {
                    String currIP = nodelocator.getPrimary(System.currentTimeMillis() + "" + d.hashCode()).getName();
                    if (!prevDemand2IP.get(d).equals(currIP)) {
                        //inform node(not currIP) who is processing the demand to stop

                        taskToMove.put(d, currIP);
                    }
                }

                for (String demand : taskToMove.keySet()) {
                    if (prevDemand2IP.get(demand).equals(localip)) {
                        logger.info("Detect task 【TERMINATE】 signal: " + "/ulss/stat/reducer/task/" + localip + "/" + demand + " | TERMINATE");
                        CentralProcessor.removeDemand(demand);
                        zc.writeData("/ulss/stat/reducer/task/" + localip + "/" + demand, "WAITING");
                    } else {
                        zc.writeData("/ulss/stat/reducer/task/" + prevDemand2IP.get(demand) + "/" + demand, "TERMINATE");
                        System.out.println("【】【】【】\n" + "/ulss/stat/reducer/task/" + prevDemand2IP.get(demand) + "/" + demand + " to " + zc.readData("/ulss/stat/reducer/task/" + prevDemand2IP.get(demand) + "/" + demand));
                    }
                }

                logger.debug("*" + taskToMove);

                //check whether prev node has terminated migrating task
                List<String> ll = new ArrayList<String>(taskToMove.keySet());
                while (true) {
                    logger.info("Demands :" + ll + " will be moved to new nodes.");
                    for (String d : taskToMove.keySet()) {
                        //
                        if (zc.readData("/ulss/stat/reducer/task/" + prevDemand2IP.get(d) + "/" + d).equals("WAITING")) {
                            Thread.sleep(3000);
                            zc.writeData("/ulss/stat/reducer/task/" + taskToMove.get(d) + "/" + d, "READY");
                            ll.remove(d);
                        }
                    }
                    if (ll.isEmpty()) {
                        break;
                    } else {
                        logger.warn("Demands : " + ll + " are not done on previous nodes: " + prevDemand2IP + ", they will not be moved to new nodes:" + taskToMove + " until they're done");
                        Thread.sleep(1000);
                    }
                }
            }
            logger.info("Task scheduled complete!");

        } catch (Exception ex) {
            logger.error("Schedule taska failed for : " + ex);
        }
    }

    public static void resetTaskStatus(List<String> nodes) {
        for (String n : nodes) {
            for (String d : zc.getChildren("/ulss/stat/reducer/task/" + n)) {
                zc.writeData("/ulss/stat/reducer/task/" + n + "/" + d, "WAITING");
            }
        }
        logger.info("Reset status for " + nodes + " successfully.");
    }

    private static class TaskMonitor implements IZkDataListener {

        @Override
        public void handleDataChange(String path, Object o) throws Exception {
            if (o.equals("TERMINATE")) {
                logger.info("Detect task 【TERMINATE】 signal: " + path + " | " + o);
                String demand = path.split("\\/")[path.split("\\/").length - 1];
                CentralProcessor.removeDemand(demand);
                zc.writeData(path, "WAITING");
            } else if (o.equals("READY")) {
                logger.info("Detect task 【READY】 signal: " + path + " | " + o);
                String demand = path.split("\\/")[path.split("\\/").length - 1];
                CentralProcessor.registerDemand(demand);
                zc.writeData(path, "RUNNING");
            } else {
                logger.info("Detect task 【OTHER】 signal: " + path + " | " + o);
            }
        }

        @Override
        public void handleDataDeleted(String string) throws Exception {
            logger.warn(string + " data deleted.");
        }

    }

}
