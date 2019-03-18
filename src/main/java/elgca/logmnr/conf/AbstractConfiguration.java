package elgca.logmnr.conf;

import java.util.Map;
import java.util.Optional;

public abstract class AbstractConfiguration {
    private Map<?, ?> config;

    AbstractConfiguration(Map<?, ?> config) {
        this.config = config;
    }

    Optional<String> getOptional(String name) {
        return Optional.ofNullable((String) config.get(name));
    }

    String get(String name) {
        return (String) config.get(name);
    }

    String getString(String name) {
        return get(name);
    }

    Integer getInt(String name) {
        return getOptional(name)
                .map(Integer::valueOf)
                .orElse(null);
    }

    Long getLong(String name) {
        return getOptional(name)
                .map(Long::valueOf)
                .orElse(null);
    }
}
