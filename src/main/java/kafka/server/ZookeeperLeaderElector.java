package kafka.server;

import kafka.controller.ControllerContext;
import kafka.controller.KafkaController;
import kafka.utils.Utils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
public class ZookeeperLeaderElector implements LeaderElector{

    private static Logger logger = Logger.getLogger(ZookeeperLeaderElector.class);

    public  ControllerContext controllerContext;
    public String electionPath;
    public int brokerId;
    public KafkaController kafkaController;

    public ZookeeperLeaderElector(ControllerContext controllerContext, String electionPath, int brokerId, KafkaController kafkaController) {
        this.controllerContext = controllerContext;
        this.electionPath = electionPath;
        this.brokerId = brokerId;
        this.kafkaController = kafkaController;

        index = electionPath.lastIndexOf("/");
        if (index > 0)
            ZkUtils.makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index));
    }

    int leaderId = -1;

    // create the election path in ZK, if one does not exist
    int index ;

    LeaderChangeListener leaderChangeListener = new LeaderChangeListener();


    public void startup(){
        controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener);
        synchronized(controllerContext.controllerLock) {
            elect();
        }
    }

    public boolean amILeader(){
        return leaderId == brokerId;
    }

    public boolean elect(){
        long timestamp = System.currentTimeMillis();
        Map<String,String> map1 = new HashMap<>();
        map1.put("version","1");
        map1.put("brokerid",Long.toString(brokerId));
        List<String> l1 = Utils.mapToJsonFields(map1, false);
        Map<String,String> map2 = new HashMap<>();
        map2.put("timestamp",Long.toString(timestamp));
        List<String> l2 = Utils.mapToJsonFields(map2, true);
        l1.addAll(l2);
        String electString = Utils.mergeJsonFields(l1);
        try {
            ZkUtils.createElectorEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
                    controllerContext.zkSessionTimeout);
            logger.info(brokerId + " successfully elected as leader");
            leaderId = brokerId;
            kafkaController.onControllerFailover();
        } catch (ZkNodeExistsException e){
            // If someone else has written the path, then
            String data = ZkUtils.readDataMaybeNull(controllerContext.zkClient, electionPath).getKey();
            if(data != null && !data.isEmpty()){
                leaderId = KafkaController.parseControllerId(data);
            }else{
                logger.warn("A leader has been elected but just resigned, this will result in another round of election");
                leaderId = -1;
            }
            if (leaderId != -1)
                logger.debug("Broker %d was elected as leader instead of broker %d".format(leaderId + "", brokerId));

        }catch (Throwable e2){
            logger.error("Error while electing or becoming leader on broker %d".format(brokerId + ""), e2);
            leaderId = -1;
        }

        return amILeader();

    }

    public void  close(){
        leaderId = -1;
    }
    public void resign(){
        leaderId = -1;
        ZkUtils.deletePath(controllerContext.zkClient, electionPath);
    }

    class LeaderChangeListener implements IZkDataListener {
        public void handleDataChange(String dataPath, Object data) throws Exception{
            synchronized(controllerContext.controllerLock) {
                leaderId = KafkaController.parseControllerId(data.toString());
                logger.info("New leader is %d".format(leaderId + ""));
            }
        }

        public void handleDataDeleted(String dataPath) throws Exception{
            synchronized(controllerContext.controllerLock) {
                logger.debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
                        .format(brokerId + "", dataPath));
                elect();
            }
        }
    }
}
