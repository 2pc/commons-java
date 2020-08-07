package  com.d11i.github;
import io.lettuce.core.*;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.apache.commons.lang3.StringUtils;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LettuceDistibutedlock {

    private static String UN_LOCK_LUA_SCRIPT = "if redis.call('get', KEYS[1] == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end)";

    public RedisClusterClient redisClusterClient;
    public StatefulRedisClusterConnection<String, String> connection;
    public RedisAdvancedClusterAsyncCommands<String,String> commands;

    private String redisUrl;

    public void init(String redisUrl) throws IllegalAccessException {
        this.redisUrl = redisUrl;

        if(StringUtils.isEmpty(redisUrl)){
            throw new IllegalAccessException("redis url is null");
        }
        String[] urls = redisUrl.split(",");
        List<RedisURI> list = new ArrayList<RedisURI>();
        for (String url: urls) {
            list.add(RedisURI.create("redis://"+ url +"/0"));
        }
        redisClusterClient = RedisClusterClient.create(list);
        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions
                .builder()
                .enableAdaptiveRefreshTrigger(ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT,
                        ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                .enablePeriodicRefresh(true)
                .enablePeriodicRefresh(Duration.ofSeconds(30)).build();
        redisClusterClient.setOptions(
                ClusterClientOptions.builder().topologyRefreshOptions(topologyRefreshOptions).build());

        connection = redisClusterClient.connect();

        connection.setReadFrom(ReadFrom.REPLICA_PREFERRED);
        commands = connection.async();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                connection.close();
                redisClusterClient.shutdown();
            }
        });

        };


    public CompletableFuture<String> tryLockAsync(String lockKey,String lockId){
        SetArgs setArgs = new SetArgs();
        setArgs.nx().px(2000);
        return (CompletableFuture<String>)commands.set(lockKey, lockId, setArgs);
    }

    public CompletableFuture<Long> tryUnLockAsync(String lockKey, String lockId){
        String[] keys = {lockKey};
        RedisFuture<Long> evalRet = commands.eval(UN_LOCK_LUA_SCRIPT, ScriptOutputType.INTEGER, keys ,lockId);

        return (CompletableFuture<Long>)evalRet;
    }



    }


