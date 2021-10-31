package zkexample;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ZkDynamicConfigReader {
  private static final Logger LOG = LogManager.getLogger(ZkDynamicConfigReader.class);

  public static void main(String[] args) throws Exception {

    String hostPort = args[0];
    String rootPath = "/example-dynamic-config";
    ConfigListener listener = new ConfigListener() {
      @Override
      public void reconfigure(Config config) {
        LOG.info("New config is {}.", config);
      }
    };

    ConfigWatcher configWatcher = new ConfigWatcher(rootPath, listener);
    ZooKeeper zooKeeper = new ZooKeeper(hostPort, (int) TimeUnit.SECONDS.toMillis(40), configWatcher);
    configWatcher.setZooKeeper(zooKeeper);
    Thread.sleep(100_000);
  }

  /**
   * This is the thing that actually does all the ZK watching.
   */
    private static final class ConfigWatcher implements Watcher, AsyncCallback.StringCallback {
      private ZooKeeper zooKeeper;
      private final String rootPath;
      private final Gson gson = new Gson();
      private final String defaultConfigJson = gson.toJson(new Config());
      private final ConfigListener configListener;

      private final AsyncCallback.DataCallback getDataCallback = new DataCallback() {
        @Override
        public synchronized void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
          if (rc == KeeperException.Code.OK.intValue()) {
            try {
              String configStr = new String(data, StandardCharsets.UTF_8);
              Config latestConfig = gson.fromJson(configStr, Config.class);
              configListener.reconfigure(latestConfig);
            } catch (Exception e) {
              LOG.error("Failed to deserialize new config.", e);
            }
          } else {
            LOG.error("Getting new config returned an error code {}.", rc);
          }
        }
      };


      public ConfigWatcher(String rootPath, ConfigListener configListener) {
        this.rootPath = rootPath;
        this.configListener = configListener;
      }

      synchronized void setZooKeeper(ZooKeeper zooKeeper) {
        if (this.zooKeeper == null) {
          notifyAll();
        }
        this.zooKeeper = zooKeeper;
      }

      @Override
      public synchronized void process(WatchedEvent watchedEvent) {
        if (zooKeeper == null) {
          try {
            wait();
          } catch (InterruptedException interruptedException) {
            throw new RuntimeException(interruptedException);
          }
        }

        if (watchedEvent.getType() == Event.EventType.None) {
          switch (watchedEvent.getState()) {
            case SyncConnected:
              initiateRecursiveCreatePath(rootPath + "/config");
              LOG.info("Sent async create.");
              break;
            case Disconnected:
              LOG.warn("Disconnected from zookeeper.");
              break;
            case Expired:
              LOG.warn("Zookeeper config connection state change {}.", watchedEvent);
              //TODO: when the session has expired a new ZK client needs to be created.
              break;
            default:
              break;
          }
        } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
          setWatchForNewConfig();
        }
      }


      // create returned
      @Override
      public void processResult(int rc, String path, Object pathParts, String name) {
        List<String> pathPartsList = (List<String>) pathParts;
        List<String> remainingParts = pathPartsList.subList(1, pathPartsList.size());

        if (rc == KeeperException.Code.OK.intValue()) {
          LOG.info("Node {} created successfully.", path);
          if (remainingParts.isEmpty()) {
            setWatchForNewConfig();
          } else {
            byte[] data = null;
            if (remainingParts.size() == 1) {
              data = defaultConfigJson.getBytes(StandardCharsets.UTF_8);
            }
            zooKeeper.create(
                    path + "/" + remainingParts.get(0),
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    this, remainingParts);
          }
        } else if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
          if (pathPartsList.size() == 1) {
            //TODO:  this won't work for all error conditions probably need to retry the creation of this node
            setWatchForNewConfig();
          } else {
            byte[] data = null;
            if (remainingParts.size() == 1) {
              data = defaultConfigJson.getBytes(StandardCharsets.UTF_8);
            }
            zooKeeper.create(
                    path + "/" + remainingParts.get(0),
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    this, remainingParts);
          }
        } else {
          LOG.error("Failed to create or find the path {}", path);
        }
      }

      void setWatchForNewConfig() {
        LOG.info("Setting watch and getting current config.");
        zooKeeper.getData(rootPath + "/config", this, getDataCallback, null);
      }

      void initiateRecursiveCreatePath(String path) {
        String[] parts = path.split("/");
        List<String> partsList = Arrays.asList(parts);
        if (parts[0].equals("")) {
          partsList = partsList.subList(1, partsList.size());
        }
        try {
          zooKeeper.create(
                  "/" + partsList.get(0),
                  defaultConfigJson.getBytes(StandardCharsets.UTF_8),
                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT,
                  this, partsList);
        } catch (Exception e) {
          LOG.error("Initiate create path " + path + " failed.", e);
        }
      }
    }



}
