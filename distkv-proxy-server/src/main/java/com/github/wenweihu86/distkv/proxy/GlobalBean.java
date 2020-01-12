package com.github.wenweihu86.distkv.proxy;

import com.github.wenweihu86.distkv.api.MetaAPI;
import com.github.wenweihu86.distkv.api.StoreAPI;
import com.github.wenweihu86.rpc.client.EndPoint;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCClientOptions;
import com.github.wenweihu86.rpc.client.RPCProxy;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wenweihu86 on 2017/6/8.
 */
public class GlobalBean {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalBean.class);
    private static GlobalBean instance;

    private Toml toml;
    private List<ShardingClient> metaServerShadings;
    private Map<Integer, ShardingClient> storeServerShardingMap;

    public GlobalBean() {
        String fileName = "/proxy.toml";
        File file = new File(getClass().getResource(fileName).getFile());
        toml = new Toml().read(file);
        readServerShardingsConf();  // 将配置文件中的内容解析
        initRPCClient();    // 让 shardingClient 分别持有 MetaAPI 和 StoreAPI 对应的 rpc 客户端
    }

    public GlobalBean(String toml) {
        File file = new File(toml);
        this.toml = new Toml().read(file);
        readServerShardingsConf();  // 将 toml 配置文件中的信息解析到 metaServerShadings 和 storeServerShardingMap 中
        initRPCClient();
    }

    public static GlobalBean getInstance() {
        if (instance == null) {
            instance = new GlobalBean();
        }
        return instance;
    }

    public static GlobalBean getInstance(String toml) {
        if (instance == null) {
            instance = new GlobalBean(toml);
        }
        return instance;
    }

    public int getPort() {
        return (int) toml.getLong("port").longValue();
    }

    public List<ShardingClient> getMetaServerShadings() {
        return metaServerShadings;
    }

    public Map<Integer, ShardingClient> getStoreServerShardingMap() {
        return storeServerShardingMap;
    }

    // 让 shardingClient 分别持有 MetaAPI 和 StoreAPI 对应的 rpc 客户端
    private void initRPCClient() {
        Toml metaServerConf = toml.getTable("meta_server");
        RPCClientOptions metaOptions = readRPCClientOptions(metaServerConf);    // 读取参数信息
        for (ShardingClient shardingClient : metaServerShadings) {
            RPCClient rpcClient = new RPCClient(shardingClient.getServers(), metaOptions);
            shardingClient.setRpcClient(rpcClient);
            MetaAPI metaAPI = RPCProxy.getProxy(rpcClient, MetaAPI.class);
            shardingClient.setMetaAPI(metaAPI); // 这里是每个 shardingClient 都持有了 MetaAPI 对应的 rpc 客户端
        }

        Toml storeServerConf = toml.getTable("store_server");
        RPCClientOptions storeOptions = readRPCClientOptions(storeServerConf);  // 读取参数信息
        for (ShardingClient shardingClient : storeServerShardingMap.values()) {
            RPCClient rpcClient = new RPCClient(shardingClient.getServers(), storeOptions);
            shardingClient.setRpcClient(rpcClient);
            StoreAPI storeAPI = RPCProxy.getProxy(rpcClient, StoreAPI.class);
            shardingClient.setStoreAPI(storeAPI);   // 这里是每个 shardingClient 都持有了 StoreAPI 对应的 rpc 客户端
        }
    }

    // 读取参数信息
    private RPCClientOptions readRPCClientOptions(Toml serverConf) {
        RPCClientOptions options = new RPCClientOptions();
        options.setConnectTimeoutMillis(
                serverConf.getLong("connect_timeout_ms").intValue());
        options.setWriteTimeoutMillis(
                serverConf.getLong("write_timeout_ms").intValue());
        options.setReadTimeoutMillis(
                serverConf.getLong("read_timeout_ms").intValue());
        LOG.info("reading rpc client options conf, " +
                        "connect_timeout_ms={}, write_timeout_ms={}, read_timeout_ms={}",
                options.getConnectTimeoutMillis(),
                options.getWriteTimeoutMillis(),
                options.getReadTimeoutMillis());
        return options;
    }

    // 将 toml 配置文件中的信息解析到 metaServerShadings 和 storeServerShardingMap 中
    private void readServerShardingsConf() {
        metaServerShadings = new ArrayList<>();
        Toml metaServerConf = toml.getTable("meta_server");
        List<Toml> shardingConfList = metaServerConf.getTables("sharding");
        for (Toml shardingConf : shardingConfList) {
            metaServerShadings.add(readShardingConf(shardingConf)); // 解析 index  和 server，解析为 ShardingClient，持有了三个 Server 信息
        }

        storeServerShardingMap = new HashMap<>();
        Toml storeServerConf = toml.getTable("store_server");
        shardingConfList = storeServerConf.getTables("sharding");
        for (Toml shardingConf : shardingConfList) {
            ShardingClient shardingClient = readShardingConf(shardingConf); // 解析 index  和 server，解析为 ShardingClient，持有了三个 Server 信息
            storeServerShardingMap.put(shardingClient.getIndex(), shardingClient);
        }
    }

    // 解析 index  和 server，解析为 ShardingClient，持有了三个 Server 信息
    private ShardingClient readShardingConf(Toml shardingConf) {
        int index = shardingConf.getLong("index").intValue();
        List<Toml> serverConfList = shardingConf.getTables("server");
        List<EndPoint> servers = new ArrayList<>();
        for (Toml serverConf : serverConfList) {
            servers.add(readServerConf(serverConf));
        }
        ShardingClient sharding = new ShardingClient(index, servers);
        return sharding;
    }

    private EndPoint readServerConf(Toml serverConf) {
        String ip = serverConf.getString("ip");
        int port = serverConf.getLong("port").intValue();
        EndPoint endPoint = new EndPoint(ip, port);
        LOG.info("read conf server, ip={}, port={}", ip, port);
        return endPoint;
    }

}
