package com.example.outbox;

import com.aerospike.client.Host;
import com.example.outbox.domain.ApiRequestRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

import java.util.Arrays;
import java.util.Collection;

/**
 * Reactive Aerospike wiring.
 *
 * This class deliberately lives in the root package (com.example.outbox): Spring Data scans the
 * configuration class's package, and everything below it, for @Document entities.
 */
@Configuration
@EnableReactiveAerospikeRepositories(basePackageClasses = ApiRequestRepository.class)
public class AerospikeConfig extends AbstractReactiveAerospikeDataConfiguration {

    private final String hosts;
    private final String namespace;

    public AerospikeConfig(@Value("${aerospike.hosts}") String hosts,
                           @Value("${aerospike.namespace}") String namespace) {
        this.hosts = hosts;
        this.namespace = namespace;
    }

    @Override
    protected Collection<Host> getHosts() {
        // "host1:3000,host2:3000" - default port 3000 when omitted
        return Arrays.asList(Host.parseHosts(hosts, 3000));
    }

    @Override
    protected String nameSpace() {
        return namespace;
    }
}
