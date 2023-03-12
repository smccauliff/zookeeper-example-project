package zkexample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LeadershipWatcher implements Watcher {

  private static final Logger LOG = LogManager.getLogger(LeadershipWatcher.class);

  private final String zkPrefix;
  private ZooKeeper zooKeeper;
  private String currentZNodeName;

  private LeadershipState isLeader = LeadershipState.UNKNOWN;

  private final LeadershipChangeListener leadershipChangeListener;

  public LeadershipWatcher(String zkPrefix, LeadershipChangeListener leadershipChangeListener) {
    this.zkPrefix = zkPrefix;
    this.leadershipChangeListener = leadershipChangeListener;
  }

  public void setZooKeeper(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  /**
   * This should be called by whatever ZK callback manages the connections so we can create a new znode when we become
   * disconnected from zookeeper.
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void enterElection() throws InterruptedException, KeeperException {
    leadershipChangeListener.leaderState(LeadershipState.UNKNOWN);

    // The host name is here for debugging purposes only
    String hostName = "unknown";
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }

    String zkNodeNameWithoutSequenceNumber = zkPrefix + "/" + UUID.randomUUID() + "_" + hostName + "_";
    currentZNodeName = zooKeeper.create(zkNodeNameWithoutSequenceNumber, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    LOG.info("Entered election with znode {}.", currentZNodeName);

    election();
  }

  private void election() throws InterruptedException, KeeperException {
    List<String> candidates = zooKeeper.getChildren(zkPrefix, false);
    Collections.sort(candidates , (a, b) -> {
      long diff = sequenceNumberPartOfZnodeName(a) - sequenceNumberPartOfZnodeName(b);
      if (diff < 0) {
        return -1;
      } else if (diff > 0) {
        return 1;
      } else {
        return 0;
      }
    });

    if (sequenceNumberPartOfZnodeName(candidates.get(0)) == sequenceNumberPartOfZnodeName(currentZNodeName)) {
      LOG.info("Leader {}", currentZNodeName);
      updateLeadershipOnChange(LeadershipState.LEADER);
    } else {
      LOG.info ("Follower of {}.", candidates.get(0));
      zooKeeper.exists(candidates.get(0), this);
      updateLeadershipOnChange(LeadershipState.FOLLOWER);
    }
  }


  static long sequenceNumberPartOfZnodeName(String znodeName) {
    int index = znodeName.lastIndexOf('_');
    return Long.parseLong(znodeName.substring(index));
  }

  private void updateLeadershipOnChange(LeadershipState newState) {
    if (newState != isLeader) {
      LOG.info("Changing leadership state from {} to {}.", isLeader, newState);
      isLeader= newState;
      leadershipChangeListener.leaderState(isLeader);
    }
  }
  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()) {
      case NodeDeleted:
      case ChildWatchRemoved:
      case NodeCreated:
        try {
          election();
        } catch (Exception e) {
          //TODO: assume external retry
          LOG.error("Entering election failed.", e);
          updateLeadershipOnChange(LeadershipState.UNKNOWN);
        }
      default:
        // Ignored
    }
  }


}
