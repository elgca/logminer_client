package elgca.logmnr.conf;

import java.util.Map;
import java.util.Optional;

public abstract class AbstractConfiguration {
    private Map<?, ?> config;

    public AbstractConfiguration(Map<?, ?> config) {
        this.config = config;
    }

    public Optional<String> getOptional(String name) {
        return Optional.ofNullable((String) config.get(name));
    }

    public String get(String name) {
        return (String) config.get(name);
    }

    public String getString(String name) {
        return get(name);
    }

    public Integer getInt(String name) {
        return getOptional(name)
                .map(Integer::valueOf)
                .orElse(null);
    }

    public Long getLong(String name) {
        return getOptional(name)
                .map(Long::valueOf)
                .orElse(null);
    }
}
