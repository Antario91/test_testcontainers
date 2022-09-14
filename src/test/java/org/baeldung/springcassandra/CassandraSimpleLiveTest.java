package org.baeldung.springcassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

// This live test needs a running Docker instance so that a Cassandra container can be created

//@Testcontainers
//@SpringBootTest
class CassandraSimpleLiveTest {

    private static final String KEYSPACE_NAME = "test";

//    @Container
    private static final CassandraContainer cassandra = (CassandraContainer) new CassandraContainer("cassandra:4.0.5")
            .withExposedPorts(9042)
            .withEnv("HEAP_NEWSIZE", "128M")
            .withEnv("MAX_HEAP_SIZE", "1024M")
            .withEnv("JVM_OPTS", "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0")
            .withEnv("CASSANDRA_SNITCH", "GossipingPropertyFileSnitch");

//    @BeforeAll
    static void setupCassandraConnectionProperties() {
        System.setProperty("spring.data.cassandra.keyspace-name", KEYSPACE_NAME);
        System.setProperty("spring.data.cassandra.contact-points", cassandra.getContainerIpAddress());
        System.setProperty("spring.data.cassandra.port", String.valueOf(cassandra.getMappedPort(9042)));

        createKeyspace(cassandra.getCluster());
    }

    static void createKeyspace(Cluster cluster) {
        Session session = cluster.connect();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication = \n" +
                "{'class':'SimpleStrategy','replication_factor':'1'};");
    }

//    @Test
    void givenCassandraContainer_whenSpringContextIsBootstrapped_thenContainerIsRunningWithNoExceptions() {
        System.out.println("before asserting");
        assertThat(cassandra.isRunning()).isTrue();
        System.out.println("after asserting");
    }

}
