package zkexample;

import org.apache.zookeeper.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ZkDynamicConfigReader {
  public static void main(String[] args) throws Exception {

    String hostPort = args[0];
    Semaphore connectionOk = new Semaphore(1);
    ZookeeperReconnector zookeeperReconnector = new ZookeeperReconnector(hostPort, (int) TimeUnit.SECONDS.toMillis(40), connectionOk);
    new Thread(zookeeperReconnector).start();
  }

  private static final class ZookeeperReconnector implements Runnable {
    private final String hostPort;
    private final int sessionTimeout;
    private final Semaphore connectionOk;

    public ZookeeperReconnector(String hostPort, int sessionTimeout, Semaphore connectionOk) {
      this.hostPort = hostPort;
      this.sessionTimeout = sessionTimeout;
      this.connectionOk = connectionOk;
    }

    public void run() {
      ZooKeeper zooKeeper = null;
      do {
        try {
          connectionOk.acquire();
          if (zooKeeper != null) {
            try {
              zooKeeper.close();
            } catch (Exception ignore) {
              //TODO: log close exception
            }
          }
          zooKeeper = new ZooKeeper(hostPort, sessionTimeout, new ConnectionWatcher(connectionOk));
        } catch (InterruptedException interruptedException) {
          //TODO: log an exit thread.
        } catch (Exception e) {
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          } catch (InterruptedException ignored) {
          }
        }
      } while (true);
    }

    private static final class ConnectionWatcher implements Watcher {
      private final Semaphore connectionOk;

      public ConnectionWatcher(Semaphore connectionOk) {
        this.connectionOk = connectionOk;
      }

      @Override
      public void process(WatchedEvent e) {
        System.out.println(e);
        if (e.getType() == Event.EventType.None) {
          switch (e.getState()) {
            case SyncConnected:
              // Ok
              break;
            case Disconnected:
            case Expired:
              connectionOk.release();
            default:
              break;
          }
        }
      }
    }

    private static void create(ZooKeeper zooKeeper, String path, byte[] data)
            throws KeeperException,
            InterruptedException {

      zooKeeper.create(
              path,
              data,
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
    }
  }
}
