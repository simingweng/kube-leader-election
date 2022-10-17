package com.cloudera;

import java.io.IOException;
import java.time.Duration;

import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.Config;

/**
 * Leader election using Kubernetes Lease resource
 *
 */
public class App {
    public static void main(String[] args) throws IOException {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);

        // this will create a lease resource named "nifi" under "default" namespace,
        // and use "my-node-identity" as participant identity
        LeaseLock lock = new LeaseLock("default", "nifi", "my-node-identity");
        LeaderElectionConfig config = new LeaderElectionConfig(
                lock,
                Duration.ofSeconds(10), // lease duration
                Duration.ofSeconds(8), // renew deadline
                Duration.ofSeconds(2)); // retry interval

        try (LeaderElector elector = new LeaderElector(config)) {
            elector.run(
                    () -> {
                        System.out.println("I am leader now");
                    },
                    () -> {
                        System.out.println("I am no longer the leader");
                    },
                    leaderId -> {
                        System.out.println("leader has been switch to " + leaderId);
                    });
        }
    }
}
