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
import io.netty.util.NettyRuntime;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.function.Function;

/**
 * Can use a single HttpClient across different azure clients, thus reusing same connection pool.
 * - Create NettyHttpClient with connection pool size 500 and thread pool size 1000;
 * - Create 100 azure clients with the same NettyHttpClient.
 * - Make 100 concurrent calls using azure clients.
 */
public class CanUseSingleConnectionPoolAndThreadPoolAcrossAzureClients {

    private static final int MAX_CONNECTION_POOL_SIZE = 500;
    private static final int PENDING_ACQUIRE_CONNECTION_COUNT = 1000;
    private static final int THREAD_POOL_SIZE = NettyRuntime.availableProcessors() * 2;
    private static final int CLIENT_COUNT = 100;

    public static boolean runSample() {
        Region region = Region.US_EAST;

        try {

            //============================================================
            // Create NettyHttpClient with connection pool size 500 and thread pool size 1000.
            HttpClient httpClient = new NettyAsyncHttpClientBuilder()
                // Connection pool configuration.
                // Official Reactor Netty documentation for defaults: https://projectreactor.io/docs/netty/release/reference/#_connection_pool_2
                .connectionProvider(
                    ConnectionProvider.builder("connection-pool")
                        // By default, HttpClient uses a “fixed” connection pool with 500 as the maximum number of active channels
                        // and 1000 as the maximum number of further channel acquisition attempts allowed to be kept in a pending state.
                        .maxConnections(MAX_CONNECTION_POOL_SIZE)
                        // When the maximum number of channels in the pool is reached, up to specified new attempts to
                        // acquire a channel are delayed (pending) until a channel is returned to the pool again, and further attempts are declined with an error.
                        .pendingAcquireMaxCount(PENDING_ACQUIRE_CONNECTION_COUNT)
                        .maxIdleTime(Duration.ofSeconds(20)) // Configures the maximum time for a connection to stay idle to 20 seconds.
                        .maxLifeTime(Duration.ofSeconds(60)) // Configures the maximum time for a connection to stay alive to 60 seconds.
                        .pendingAcquireTimeout(Duration.ofSeconds(60)) // Configures the maximum time for the pending acquire operation to 60 seconds.
                        .evictInBackground(Duration.ofSeconds(120)) // Every two minutes, the connection pool is regularly checked for connections that are applicable for removal.
                        .build())
                // Thread pool configuration.
                // Official Reactor Netty documentation for defaults: https://projectreactor.io/docs/netty/release/reference/#client-tcp-level-configurations-event-loop-group
                .eventLoopGroup(LoopResources
                        .create(
                                "client-thread-pool", // thread pool name
                                THREAD_POOL_SIZE,           // thread pool size
                                true)
                        // we use our custom event loop here, disable the native one
                        .onClient(false))
                .build();

            //============================================================
            // Create 100 azure clients with the same NettyHttpClient.
            final AzureProfile profile = new AzureProfile(AzureEnvironment.AZURE);
            final TokenCredential credential = new DefaultAzureCredentialBuilder()
                    .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
//                    .executorService(ForkJoinPool.commonPool()) // thread pool for executing token acquisition, usually we leave it as default
                    .httpClient(httpClient) //
                    .build();

            Flux<ComputeUsage>[] usageFluxArray = new Flux[CLIENT_COUNT];
            for (int i = 0; i < CLIENT_COUNT; i++) {
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
                    .publishOn(Schedulers.boundedElastic())
                .map(computeUsage -> {
                    System.out.println(Thread.currentThread().getName());
                    return computeUsage;
                });
            }
            //============================================================
            // Make concurrent calls and wait for concurrent calls to finish.
            Flux.merge(usageFluxArray).blockLast();

            return true;
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
}
