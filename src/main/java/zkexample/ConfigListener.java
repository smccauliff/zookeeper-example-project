package zkexample;

/**
 * This is the thing that gets the reconfiguration callbacks when something has changed. This does not have a
 * ZK dependency.
 */
public interface ConfigListener {

  void reconfigure(Config config);
}
