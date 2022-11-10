package com.azure.client.examples;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.Region;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.compute.models.ComputeUsage;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionPoolMetrics;
import reactor.netty.resources.ConnectionProvider;

import java.net.SocketAddress;
import java.time.Duration;

/**
 * Can use a single HttpClient across different azure clients, thus reusing same connection pool.
 * - Create NettyHttpClient with connection pool size 10.
 * - Create 100 azure clients with the same NettyHttpClient.
 * - Make 100 concurrent calls using azure clients.
 * - Observe that connection pool is exhausted.
 */
public class CanUseSingleConnectionPoolAcrossAzureClients {

    private static final int MAX_CONNECTION_POOL_SIZE = 10;

    public static boolean runSample() {
        Region region = Region.US_EAST;

        final int clientCount = 100;
        final AzureProfile profile = new AzureProfile(AzureEnvironment.AZURE);
        final TokenCredential credential = new DefaultAzureCredentialBuilder()
            .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
            .build();

        try {
            CustomMetricsRegistrar customMetricsRegistrar = new CustomMetricsRegistrar();

            //============================================================
            // Create NettyHttpClient with connection pool size 10.
            HttpClient httpClient = new NettyAsyncHttpClientBuilder()
                .connectionProvider(
                    ConnectionProvider.builder("custom")
                        // By default, HttpClient uses a “fixed” connection pool with 500 as the maximum number of active channels
                        // and 1000 as the maximum number of further channel acquisition attempts allowed to be kept in a pending state.
                        .maxConnections(MAX_CONNECTION_POOL_SIZE)
                        // When the maximum number of channels in the pool is reached, up to specified new attempts to
                        // acquire a channel are delayed (pending) until a channel is returned to the pool again, and further attempts are declined with an error.
                        .pendingAcquireMaxCount(clientCount)
                        .maxIdleTime(Duration.ofSeconds(20)) // Configures the maximum time for a connection to stay idle to 20 seconds.
                        .maxLifeTime(Duration.ofSeconds(60)) // Configures the maximum time for a connection to stay alive to 60 seconds.
                        .pendingAcquireTimeout(Duration.ofSeconds(10)) // Configures the maximum time for the pending acquire operation to 60 seconds.
                        .evictInBackground(Duration.ofSeconds(120)) // Every two minutes, the connection pool is regularly checked for connections that are applicable for removal.
                        .metrics(true, () -> customMetricsRegistrar) // Enable pool metrics.
                        .build())
                .connectTimeout(Duration.ofSeconds(1))
                .build();

            //============================================================
            // Create 100 azure clients with the same NettyHttpClient.
            Flux<ComputeUsage>[] usageFluxArray = new Flux[clientCount];
            for (int i = 0; i < clientCount; i++) {
                AzureResourceManager azureResourceManager = AzureResourceManager
                    .configure()
                    .withLogLevel(HttpLogDetailLevel.BASIC)
                    .withHttpClient(httpClient)
                    .authenticate(credential, profile)
                    // withSubscription(subscriptionId) can be used to specify different subscriptions, here use default for simplicity
                    .withDefaultSubscription();

                //============================================================
                // Construct 100 concurrent calls using azure clients for later use.
                usageFluxArray[i] = azureResourceManager.computeUsages()
                    .listByRegionAsync(region)
                    .subscribeOn(Schedulers.boundedElastic());
            }
            //============================================================
            // Make concurrent calls and wait for concurrent calls to finish.
            Flux.merge(usageFluxArray).blockLast();

            //============================================================
            // Observe that connection pool is exhausted.
            return customMetricsRegistrar.metrics.maxAllocatedSize() == MAX_CONNECTION_POOL_SIZE // max size
                && customMetricsRegistrar.metrics.allocatedSize() == MAX_CONNECTION_POOL_SIZE; // exhuasted all connections
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        if (!runSample()) {
            throw new IllegalStateException("Sample run failed.");
        }
    }

    private static class CustomMetricsRegistrar implements ConnectionProvider.MeterRegistrar {
        private ConnectionPoolMetrics metrics;
        @Override
        public void registerMetrics(String s, String s1, SocketAddress socketAddress, ConnectionPoolMetrics connectionPoolMetrics) {
            metrics = connectionPoolMetrics;
        }
    }
}