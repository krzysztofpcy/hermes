package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class ZookeeperPathChildrenCache {

    private final LoadingCache<String, List<String>> cache;

    ZookeeperPathChildrenCache(CuratorFramework zookeeper, int expireAfterMinutes) {
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(expireAfterMinutes, TimeUnit.MINUTES)
                .build(new CacheLoader<String, List<String>>() {
                    @Override
                    public List<String> load(String key) throws Exception {
                        return zookeeper.getChildren().forPath(key);
                    }
                });
    }

    List<String> getChildrenPaths(String path) throws Exception {
        try {
            return cache.get(path);
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        }
    }
}
