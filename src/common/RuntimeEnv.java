/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package common;

import config.Configuration;
import core.TaskScheduler;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class RuntimeEnv {

    private static Configuration conf = null;

    public static final String ZK_CLUSTER = "zkCluster";

    public static final String LOCAL_IP = "localIP";

    public static final String DEMAND_LIST = "demandList";
    public static final String DEMAND_ID_MQ_MAP = "did2mq";
    public static final String DEMAND_ID_RULE_MAP = "did2rule";

    public static boolean loadClusterSizeChanged = false;
    private static List<String> reduceClusterList = new ArrayList<String>();

    public static ZkClient zc = null;

    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RuntimeEnv.class.getName());
    }

    public static boolean initialize(Configuration pConf) throws Exception {

        if (pConf == null) {
            logger.error("configuration object is null");
            return false;
        }
        conf = pConf;
        //zk
        getConfAndSaveAsString(ZK_CLUSTER, false);
        
        if (!initZooKeeper()) {
            return false;
        }

        List<String> dl = new ArrayList<String>();
        dl.add("1");
        dl.add("2");
        dl.add("3");
        dl.add("4");
        dl.add("5");
        
        addParam(DEMAND_LIST, dl);
        addParam(LOCAL_IP, InetAddress.getLocalHost().getHostAddress());

        return true;
    }

    public static void addParam(String pParamName, Object pValue) {
        synchronized (dynamicParams) {
            dynamicParams.put(pParamName, pValue);
        }
    }

    public static Object getParam(String pParamName) {
        return dynamicParams.get(pParamName);
    }

    public static String getParamAsString(String pParamName) {
        return (String) dynamicParams.get(pParamName);
    }

    public static Integer getParamAsInteger(String pParamName) {
        return (Integer) dynamicParams.get(pParamName);
    }

    public static Boolean getParamAsBoolean(String pParamName) {
        return (Boolean) dynamicParams.get(pParamName);
    }

    public static void getConfAndSaveAsInteger(String confName) throws Exception {
        String tmp = conf.getString(confName, "");
        if (tmp.isEmpty()) {
            throw new Exception("parameter " + confName + " does not exist or is not defined");
        }
        try {
            dynamicParams.put(confName, Integer.parseInt(tmp));
        } catch (Exception ex) {
            throw new Exception("parameter " + confName + " 's format is wrong for " + ex.getMessage(), ex);
        }
    }

    public static void getConfAndSaveAsString(String confName, boolean allowEmpty) throws Exception {
        String tmp = conf.getString(confName, "");
        if (tmp.isEmpty() && !allowEmpty) {
            throw new Exception("parameter " + confName + " does not exist or is not defined");
        }
        dynamicParams.put(confName, tmp);
    }

    public static void getConfAndSaveAsBoolean(String confName) throws Exception {
        String tmp = conf.getString(confName, "");
        if (tmp.isEmpty()) {
            throw new Exception("parameter " + confName + " does not exist or is not defined");
        }
        try {
            dynamicParams.put(confName, Boolean.parseBoolean(tmp));
        } catch (Exception ex) {
            throw new Exception("parameter " + confName + " 's format is wrong for " + ex.getMessage(), ex);
        }
    }

    public static boolean initZooKeeper() {
        //Initialize Zookeeper
        try {
            zc = new ZkClient(getParamAsString(ZK_CLUSTER));
        } catch (Exception ex) {
            logger.error("Zookeeper client connection failed for " + ex);
        }

        try {
            if (!zc.exists("/ulss")) {
                zc.createPersistent("/ulss");
            }
            if (!zc.exists("/ulss/stat")) {
                zc.createPersistent("/ulss/stat");
            }
            if (!zc.exists("/ulss/stat/reducer")) {
                zc.createPersistent("/ulss/stat/reducer");
            }
            if (!zc.exists("/ulss/stat/reducer/cluster")) {
                zc.createPersistent("/ulss/stat/reducer/cluster");
            }
            zc.createEphemeral("/ulss/stat/reducer/cluster/" + InetAddress.getLocalHost().getHostAddress(), "");
            reduceClusterList = zc.getChildren("/ulss/stat/reducer/cluster");
            zc.subscribeChildChanges("/ulss/stat/reducer/cluster", new ReduceClusterWatcher());
            logger.debug("subcribe child changes for reduce cluster successfully.");
        } catch (Exception ex) {
            logger.error("Cann't register localhost to zk for ", ex);
        }

        if (reduceClusterList.isEmpty()) {
            logger.error("no dd server is running.");
            return false;
        }
        Timer checker = new Timer("reduceCluster-checker");
        checker.schedule(new ReducerClusterChecker(), 0, 30000);
        return true;
    }

    public static void dumpEnvironment() {
        conf.dumpConfiguration();
    }

    public static List<String> getReducerList() {
        return reduceClusterList;
    }

    private static class ReduceClusterWatcher implements IZkChildListener {

        @Override
        public void handleChildChange(String string, List<String> list) throws Exception {
            logger.info("Reduce CLuster node list updated to " + list);
            if(list.isEmpty()||list==null){
                throw new Exception("Cluster list is fucked!");
            }
            List<String> downNodes = reduceClusterList;
            downNodes.removeAll(list);
            if(!downNodes.isEmpty()){
                TaskScheduler.resetTaskStatus(downNodes);
            }
            reduceClusterList = list;
            TaskScheduler.electMaster();
            TaskScheduler.scheduleTask();
        }

    }

    private static class ReducerClusterChecker extends TimerTask {

        @Override
        public void run() {

        }
    }
}
