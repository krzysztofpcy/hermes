package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.glassfish.hk2.api.Factory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.di.CuratorType;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

import javax.inject.Inject;
import javax.inject.Named;

public class WorkloadConstraintsRepositoryFactory implements Factory<WorkloadConstraintsRepository> {

    private final CuratorFramework zookeeper;
    private final ObjectMapper mapper;
    private final ZookeeperPaths paths;
    private final ConfigFactory configFactory;

    @Inject
    public WorkloadConstraintsRepositoryFactory(@Named(CuratorType.HERMES) CuratorFramework zookeeper,
                                                ObjectMapper mapper,
                                                ZookeeperPaths paths,
                                                ConfigFactory configFactory) {
        this.zookeeper = zookeeper;
        this.mapper = mapper;
        this.paths = paths;
        this.configFactory = configFactory;
    }

    @Override
    public WorkloadConstraintsRepository provide() {
        int pathsCacheExpireAfter = configFactory.getIntProperty(Configs.CONSUMER_WORKLOAD_CONSTRAINTS_PATHS_CACHE_EXPIRE_AFTER);
        return new ZookeeperWorkloadConstraintsRepository(zookeeper, mapper, paths, pathsCacheExpireAfter);
    }

    @Override
    public void dispose(WorkloadConstraintsRepository instance) {
    }
}
