package zkexample;

import java.util.Objects;

public final class Config {
  public boolean configItemOne = true;
  public int configItemTwo = 30;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Config config = (Config) o;
    return configItemOne == config.configItemOne && configItemTwo == config.configItemTwo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(configItemOne, configItemTwo);
  }

  @Override
  public String toString() {
    return "Config{" +
            "configItemOne=" + configItemOne +
            ", configItemTwo=" + configItemTwo +
            '}';
  }
}
