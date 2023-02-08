/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.remote.client;

import com.alibaba.nacos.api.ability.ClientAbilities;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.PayloadRegistry;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.RequestFuture;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.request.ConnectResetRequest;
import com.alibaba.nacos.api.remote.request.HealthCheckRequest;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.ClientDetectionResponse;
import com.alibaba.nacos.api.remote.response.ConnectResetResponse;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.api.exception.NacosException.SERVER_ERROR;

/**
 * abstract remote client to connect to server.
 *
 * @author liuzunfei
 * @version $Id: RpcClient.java, v 0.1 2020年07月13日 9:15 PM liuzunfei Exp $
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class RpcClient implements Closeable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger("com.alibaba.nacos.common.remote.client");

    //  new ServerListManager
    private ServerListFactory serverListFactory;

    //com.alibaba.nacos.common.remote.client.RpcClient.start里面去从queue中take并处理
    protected LinkedBlockingQueue<ConnectionEvent> eventLinkedBlockingQueue = new LinkedBlockingQueue<ConnectionEvent>();
    
    protected volatile AtomicReference<RpcClientStatus> rpcClientStatus = new AtomicReference<RpcClientStatus>(
            RpcClientStatus.WAIT_INIT);
    
    protected ScheduledExecutorService clientEventExecutor;

    // 使用阻塞队列来实现信号
    private final BlockingQueue<ReconnectContext> reconnectionSignal = new ArrayBlockingQueue<ReconnectContext>(1);
    
    protected volatile Connection currentConnection;
    
    protected Map<String, String> labels = new HashMap<String, String>();
    
    private String name;
    
    private String tenant;
    
    private static final int RETRY_TIMES = 3;

    // 3s?
    private static final long DEFAULT_TIMEOUT_MILLS = 3000L;
    
    protected ClientAbilities clientAbilities;
    
    /**
     * default keep alive time 5s.
     */
    private long keepAliveTime = 5000L;
    
    private long lastActiveTimeStamp = System.currentTimeMillis();
    
    /**
     * listener called where connection's status changed.
     */
    //ArrayList(NamingGrpcConnectionEventListener)
    protected List<ConnectionEventListener> connectionEventListeners = new ArrayList<ConnectionEventListener>();
    
    /**
     * 处理来自 server的请求
     * handlers to process server push request.
     */
    //new ArrayList(ConnectResetRequestHandler()             --> ConnectResetRequest
    //      , CustomRegisterServerRequestHandler(匿名内部类)   --> ClientDetectionRequest
    //      , NamingPushRequestHandler(serviceInfoHolder));  --> NotifySubscriberRequest
    protected List<ServerRequestHandler> serverRequestHandlers = new ArrayList<ServerRequestHandler>();
    
    static {
        PayloadRegistry.init();
    }
    
    public RpcClient(String name) {
        this.name = name;
    }
    
    public RpcClient(ServerListFactory serverListFactory) {
        this.serverListFactory = serverListFactory;
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
        LoggerUtils.printIfInfoEnabled(LOGGER, "RpcClient init in constructor, ServerListFactory ={}",
                serverListFactory.getClass().getName());
    }
    
    public RpcClient(String name, ServerListFactory serverListFactory) {
        this(name);
        this.serverListFactory = serverListFactory;
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
        LoggerUtils.printIfInfoEnabled(LOGGER, "RpcClient init in constructor, ServerListFactory ={}",
                serverListFactory.getClass().getName());
    }
    
    /**
     * init client abilities.
     *
     * @param clientAbilities clientAbilities.
     */
    public RpcClient clientAbilities(ClientAbilities clientAbilities) {
        this.clientAbilities = clientAbilities;
        return this;
    }
    
    /**
     *
     * 启动服务器列表工厂。只能启动一次。
     * init server list factory.only can init once.
     *
     * @param serverListFactory serverListFactory
     */
    //NamingGrpcClientProxy#start()
    //    serverListFactory: new ServerListManager
    public RpcClient serverListFactory(ServerListFactory serverListFactory) {
        // 判断状态(rpcClientStatus)是否在启动中
        if (!isWaitInitiated()) {
            return this;
        }
        this.serverListFactory = serverListFactory;
        // rpcClientStatus---> 状态替换
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
        
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]RpcClient init, ServerListFactory ={}", name,
                serverListFactory.getClass().getName());
        return this;
    }
    
    /**
     * init labels.
     *
     * @param labels labels
     */
    public RpcClient labels(Map<String, String> labels) {
        this.labels.putAll(labels);
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]RpcClient init label, labels={}", name, this.labels);
        return this;
    }
    
    /**
     * init keepalive time.
     *
     * @param keepAliveTime keepAliveTime
     * @param timeUnit      timeUnit
     */
    public RpcClient keepAlive(long keepAliveTime, TimeUnit timeUnit) {
        this.keepAliveTime = keepAliveTime * timeUnit.toMillis(keepAliveTime);
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]RpcClient init keepalive time, keepAliveTimeMillis={}", name,
                keepAliveTime);
        return this;
    }
    
    /**
     * Notify when client disconnected.
     */
    protected void notifyDisConnected() {
        if (connectionEventListeners.isEmpty()) {
            return;
        }
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Notify disconnected event to listeners", name);
        for (ConnectionEventListener connectionEventListener : connectionEventListeners) {
            try {
                connectionEventListener.onDisConnect();
            } catch (Throwable throwable) {
                LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Notify disconnect listener error,listener ={}", name,
                        connectionEventListener.getClass().getName());
            }
        }
    }
    
    /**
     * 通知客户端一个新连接
     * Notify when client new connected.
     */
    protected void notifyConnected() {
        if (connectionEventListeners.isEmpty()) {
            return;
        }
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Notify connected event to listeners.", name);
        for (ConnectionEventListener connectionEventListener : connectionEventListeners) {
            try {
                //NamingGrpcRedoService#onConnected
                connectionEventListener.onConnected();
            } catch (Throwable throwable) {
                LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Notify connect listener error,listener ={}", name,
                        connectionEventListener.getClass().getName());
            }
        }
    }
    
    /**
     * check is this client is initiated.
     *
     * @return is wait initiated or not.
     */
    public boolean isWaitInitiated() {
        return this.rpcClientStatus.get() == RpcClientStatus.WAIT_INIT;
    }
    
    /**
     * check is this client is running.
     *
     * @return is running or not.
     */
    public boolean isRunning() {
        return this.rpcClientStatus.get() == RpcClientStatus.RUNNING;
    }
    
    /**
     * check is this client is shutdown.
     *
     * @return is shutdown or not.
     */
    public boolean isShutdown() {
        return this.rpcClientStatus.get() == RpcClientStatus.SHUTDOWN;
    }
    
    /**
     * check if current connected server is in serverlist ,if not switch server.
     */
    public void onServerListChange() {
        if (currentConnection != null && currentConnection.serverInfo != null) {
            ServerInfo serverInfo = currentConnection.serverInfo;
            boolean found = false;
            for (String serverAddress : serverListFactory.getServerList()) {
                if (resolveServerInfo(serverAddress).getAddress().equalsIgnoreCase(serverInfo.getAddress())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LoggerUtils.printIfInfoEnabled(LOGGER,
                        "Current connected server {}  is not in latest server list,switch switchServerAsync",
                        serverInfo.getAddress());
                switchServerAsync();
            }
            
        }
    }
    
    /**
     * Start this client.
     */
    //NamingGrpcClientProxy#start()
    public final void  start() throws NacosException {
        // 替换状态为启动 INITIALIZED->STARTING
        boolean success = rpcClientStatus.compareAndSet(RpcClientStatus.INITIALIZED, RpcClientStatus.STARTING);
        if (!success) {
            return;
        }

//----------------
        clientEventExecutor = new ScheduledThreadPoolExecutor(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.remote.worker");
                t.setDaemon(true);
                return t;
            }
        });

        // 处理eventLinkedBlockingQueue中的event事件
        // connection event consumer.
        clientEventExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while (!clientEventExecutor.isTerminated() && !clientEventExecutor.isShutdown()) {
                    ConnectionEvent take = null;
                    try {
                        take = eventLinkedBlockingQueue.take();
                        // 连接
                        if (take.isConnected()) {
                            notifyConnected();
                        } else if (take.isDisConnected()) {
                            notifyDisConnected();
                        }
                    } catch (Throwable e) {
                        //Do nothing
                    }
                }
            }
        });
//----------------


        // 检查rpc通信 心跳也在这里面， 也就是维持rpc连接
        clientEventExecutor.submit(new Runnable() {
            @Override
            public void run() {
                // 死循环
                while (true) {
                    try {
                        // 判断this.rpcClientStatus的状态是否是关机
                        if (isShutdown()) {
                            break;
                        }
                        // poll(long timeout, TimeUnit unit)：获取并移除此队列的头部，在指定的等待时间前等待可用的元素（如果有必要）。
                        // 妙啊，2用
                        ReconnectContext reconnectContext = reconnectionSignal
                                .poll(keepAliveTime, TimeUnit.MILLISECONDS);
                        if (reconnectContext == null) {
                            //check alive time.
                            if (System.currentTimeMillis() - lastActiveTimeStamp >= keepAliveTime) {
                                // 连接检查是否正常,  这地方是健康检查，，健康检查5秒一次
                                boolean isHealthy = healthCheck();
                                // 失败就重连
                                if (!isHealthy) {
                                    if (currentConnection == null) {
                                        // 说明当前连接断开了，等待重连
                                        continue;
                                    }
                                    LoggerUtils.printIfInfoEnabled(LOGGER,
                                            "[{}]Server healthy check fail,currentConnection={}", name,
                                            currentConnection.getConnectionId());
                                    
                                    RpcClientStatus rpcClientStatus = RpcClient.this.rpcClientStatus.get();
                                    // 如果rpc状态为下线，这退出
                                    if (RpcClientStatus.SHUTDOWN.equals(rpcClientStatus)) {
                                        break;
                                    }
                                    // 替换状态为不健康的
                                    boolean success = RpcClient.this.rpcClientStatus
                                            .compareAndSet(rpcClientStatus, RpcClientStatus.UNHEALTHY);
                                    // 状态变更， 接下来珍惜i改呢重连操作
                                    if (success) {
                                        reconnectContext = new ReconnectContext(null, false);
                                    } else {
                                        continue;
                                    }
                                    
                                } else {
                                    lastActiveTimeStamp = System.currentTimeMillis();
                                    continue;
                                }
                            } else {
                                // 入宫没达到健康检查时间则跳过
                                continue;
                            }
                            
                        }
                        // 主要处理serverList 改变的情况
                        if (reconnectContext.serverInfo != null) {
                            //clear recommend server if server is not in server list.
                            // 当前连接是否存在
                            boolean serverExist = false;
                            for (String server : getServerListFactory().getServerList()) {
                                // 更新server信息
                                ServerInfo serverInfo = resolveServerInfo(server);
                                // 判断当前链接的sever是否在更新后的server列表中
                                if (serverInfo.getServerIp().equals(reconnectContext.serverInfo.getServerIp())) {
                                    serverExist = true;
                                    // 更新端口
                                    reconnectContext.serverInfo.serverPort = serverInfo.serverPort;
                                    break;
                                    // 正常就跳出当前循环
                                }
                            }
                            // 如果已连接的服务不在最新的服务列表中，那么从现在的服务列表中选一个重新连接
                            if (!serverExist) {
                                LoggerUtils.printIfInfoEnabled(LOGGER,
                                        "[{}] Recommend server is not in server list ,ignore recommend server {}", name,
                                        reconnectContext.serverInfo.getAddress());
                                
                                reconnectContext.serverInfo = null;
                                
                            }
                        }

                        // 重连接。。。其实内部是切换server
                        reconnect(reconnectContext.serverInfo, reconnectContext.onRequestFail);
                    } catch (Throwable throwable) {
                        //Do nothing
                    }
                }
            }
        });

// 连接服务端
        //connect to server ,try to connect to server sync once, async starting if fail.
        Connection connectToServer = null;
        rpcClientStatus.set(RpcClientStatus.STARTING);
        // 重试次数，默认3
        int startUpRetryTimes = RETRY_TIMES;
        while (startUpRetryTimes > 0 && connectToServer == null) {
            try {
                startUpRetryTimes--;
                ServerInfo serverInfo = nextRpcServer();
                
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Try to connect to server on start up, server: {}", name,
                        serverInfo);
                // todo 链接nacos server
                connectToServer = connectToServer(serverInfo);
            } catch (Throwable e) {
                LoggerUtils.printIfWarnEnabled(LOGGER,
                        "[{}]Fail to connect to server on start up, error message={}, start up retry times left: {}",
                        name, e.getMessage(), startUpRetryTimes);
            }
            
        }
        
        if (connectToServer != null) {
            LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Success to connect to server [{}] on start up,connectionId={}",
                    name, connectToServer.serverInfo.getAddress(), connectToServer.getConnectionId());
            this.currentConnection = connectToServer;
            rpcClientStatus.set(RpcClientStatus.RUNNING);
            // 发布链接通知
            eventLinkedBlockingQueue.offer(new ConnectionEvent(ConnectionEvent.CONNECTED));
        } else {
            // 连接失败的情况，就需要重新去连接了

            //底层是reconnectionSignal.offer(new ReconnectContext   --> offer一个到reconnectionSignal
            switchServerAsync();
        }

        // 把ConnectResetRequestHandler添加到serverRequestHandlers中
        registerServerRequestHandler(new ConnectResetRequestHandler());
        
        //register client detection request.
        registerServerRequestHandler(new CustomRegisterServerRequestHandler());
//        registerServerRequestHandler(new ServerRequestHandler() {
//            @Override
//            public Response requestReply(Request request) {
//                if (request instanceof ClientDetectionRequest) {
//                    return new ClientDetectionResponse();
//                }
//
//                return null;
//            }
//        });
        
    }


    /**
     * 自己去抽离出来的
     */
    class CustomRegisterServerRequestHandler implements ServerRequestHandler{
        @Override
        public Response requestReply(Request request) {
            if (request instanceof ClientDetectionRequest) {
                return new ClientDetectionResponse();
            }

            return null;
        }
    }


    class ConnectResetRequestHandler implements ServerRequestHandler {
        
        @Override
        public Response requestReply(Request request) {
            
            if (request instanceof ConnectResetRequest) {
                
                try {
                    synchronized (RpcClient.this) {
                        if (isRunning()) {
                            ConnectResetRequest connectResetRequest = (ConnectResetRequest) request;
                            if (StringUtils.isNotBlank(connectResetRequest.getServerIp())) {
                                ServerInfo serverInfo = resolveServerInfo(
                                        connectResetRequest.getServerIp() + Constants.COLON + connectResetRequest
                                                .getServerPort());
                                switchServerAsync(serverInfo, false);
                            } else {
                                switchServerAsync();
                            }
                        }
                    }
                } catch (Exception e) {
                    LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Switch server error ,{}", name, e);
                }
                return new ConnectResetResponse();
            }
            return null;
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        LOGGER.info("Shutdown rpc client ,set status to shutdown");
        rpcClientStatus.set(RpcClientStatus.SHUTDOWN);
        LOGGER.info("Shutdown  client event executor " + clientEventExecutor);
        clientEventExecutor.shutdownNow();
        LOGGER.info("Close current connection " + currentConnection.getConnectionId());
        closeConnection(currentConnection);
    }
    
    private boolean healthCheck() {
        HealthCheckRequest healthCheckRequest = new HealthCheckRequest();
        if (this.currentConnection == null) {
            return false;
        }
        try {
            Response response = this.currentConnection.request(healthCheckRequest, 3000L);
            //not only check server is ok ,also check connection is register.
            return response != null && response.isSuccess();
        } catch (NacosException e) {
            //ignore
        }
        return false;
    }
    
    public void switchServerAsyncOnRequestFail() {
        switchServerAsync(null, true);
    }
    
    public void switchServerAsync() {
        switchServerAsync(null, false);
    }
    
    protected void switchServerAsync(final ServerInfo recommendServerInfo, boolean onRequestFail) {
        reconnectionSignal.offer(new ReconnectContext(recommendServerInfo, onRequestFail));
    }
    
    /**
     * 切换Server
     * switch server .
     */
    protected void reconnect(final ServerInfo recommendServerInfo, boolean onRequestFail) {
        
        try {
            // recommendServerInfo 目前的逻辑基本是空的，没发现有不空的情况
            AtomicReference<ServerInfo> recommendServer = new AtomicReference<ServerInfo>(recommendServerInfo);
            // 做一次连接状态校验，如果这时候正常，就没必要去处理了
            if (onRequestFail && healthCheck()) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Server check success,currentServer is{} ", name,
                        currentConnection.serverInfo.getAddress());
                rpcClientStatus.set(RpcClientStatus.RUNNING);
                return;
            }
            
            LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] try to re connect to a new server ,server is {}", name,
                    recommendServerInfo == null ? " not appointed,will choose a random server."
                            : (recommendServerInfo.getAddress() + ", will try it once."));
            
            // loop until start client success.
            boolean switchSuccess = false;
            
            int reConnectTimes = 0;
            int retryTurns = 0;
            Exception lastException = null;
            while (!switchSuccess && !isShutdown()) {
                
                //1.get a new server
                ServerInfo serverInfo = null;
                try {
                    serverInfo = recommendServer.get() == null ? nextRpcServer() : recommendServer.get();
                    //2.create a new channel to new server
                    Connection connectionNew = connectToServer(serverInfo);
                    if (connectionNew != null) {
                        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] success to connect a server  [{}],connectionId={}",
                                name, serverInfo.getAddress(), connectionNew.getConnectionId());
                        //successfully create a new connect.
                        if (currentConnection != null) {
                            LoggerUtils.printIfInfoEnabled(LOGGER,
                                    "[{}] Abandon prev connection ,server is  {}, connectionId is {}", name,
                                    currentConnection.serverInfo.getAddress(), currentConnection.getConnectionId());
                            //set current connection to enable connection event.
                            currentConnection.setAbandon(true);
                            closeConnection(currentConnection);
                        }
                        currentConnection = connectionNew;
                        rpcClientStatus.set(RpcClientStatus.RUNNING);
                        switchSuccess = true;
                        boolean s = eventLinkedBlockingQueue.add(new ConnectionEvent(ConnectionEvent.CONNECTED));
                        return;
                    }
                    
                    //close connection if client is already shutdown.
                    if (isShutdown()) {
                        closeConnection(currentConnection);
                    }
                    
                    lastException = null;
                    
                } catch (Exception e) {
                    lastException = e;
                } finally {
                    recommendServer.set(null);
                }
                
                if (reConnectTimes > 0
                        && reConnectTimes % RpcClient.this.serverListFactory.getServerList().size() == 0) {
                    LoggerUtils.printIfInfoEnabled(LOGGER,
                            "[{}] fail to connect server,after trying {} times, last try server is {},error={}", name,
                            reConnectTimes, serverInfo, lastException == null ? "unknown" : lastException);
                    if (Integer.MAX_VALUE == retryTurns) {
                        retryTurns = 50;
                    } else {
                        retryTurns++;
                    }
                }
                
                reConnectTimes++;
                
                try {
                    //sleep x milliseconds to switch next server.
                    if (!isRunning()) {
                        // first round ,try servers at a delay 100ms;second round ,200ms; max delays 5s. to be reconsidered.
                        Thread.sleep(Math.min(retryTurns + 1, 50) * 100L);
                    }
                } catch (InterruptedException e) {
                    // Do  nothing.
                }
            }
            
            if (isShutdown()) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Client is shutdown ,stop reconnect to server", name);
            }
            
        } catch (Exception e) {
            LoggerUtils.printIfWarnEnabled(LOGGER, "[{}] Fail to  re connect to server ,error is {}", name, e);
        }
    }
    
    private void closeConnection(Connection connection) {
        if (connection != null) {
            connection.close();
            eventLinkedBlockingQueue.add(new ConnectionEvent(ConnectionEvent.DISCONNECTED));
        }
    }
    
    /**
     * get connection type of this client.
     *
     * @return ConnectionType.
     */
    public abstract ConnectionType getConnectionType();
    
    /**
     * increase offset of the nacos server port for the rpc server port.
     *  端口偏移，GRPC的server端口8848+1000
     * @return rpc port offset
     */
    public abstract int rpcPortOffset();
    
    /**
     * get current server.
     *
     * @return server info.
     */
    public ServerInfo getCurrentServer() {
        if (this.currentConnection != null) {
            return currentConnection.serverInfo;
        }
        return null;
    }
    
    /**
     * send request.
     *
     * @param request request.
     * @return response from server.
     */
    public Response request(Request request) throws NacosException {
        return request(request, DEFAULT_TIMEOUT_MILLS);
    }
    
    /**
     * send request.
     *
     * @param request request.
     * @return response from server.
     */
    // timeoutMills: 默认3s
    public Response request(Request request, long timeoutMills) throws NacosException {
        int retryTimes = 0;
        Response response = null;
        Exception exceptionThrow = null;
        long start = System.currentTimeMillis();
        // 重试
        while (retryTimes < RETRY_TIMES && System.currentTimeMillis() < timeoutMills + start) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_DISCONNECT,
                            "Client not connected,current status:" + rpcClientStatus.get());
                }
                // com.alibaba.nacos.common.remote.client.grpc.GrpcConnection#request
                response = this.currentConnection.request(request, timeoutMills);
                if (response == null) {
                    throw new NacosException(SERVER_ERROR, "Unknown Exception.");
                }
                if (response instanceof ErrorResponse) {
                    if (response.getErrorCode() == NacosException.UN_REGISTER) {
                        synchronized (this) {
                            waitReconnect = true;
                            if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
                                LoggerUtils.printIfErrorEnabled(LOGGER,
                                        "Connection is unregistered, switch server,connectionId={},request={}",
                                        currentConnection.getConnectionId(), request.getClass().getSimpleName());
                                switchServerAsync();
                            }
                        }
                        
                    }
                    throw new NacosException(response.getErrorCode(), response.getMessage());
                }
                // return response.
                lastActiveTimeStamp = System.currentTimeMillis();
                return response;
                
            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        //wait client to re connect.
                        Thread.sleep(Math.min(100, timeoutMills / 3));
                    } catch (Exception exception) {
                        //Do nothing.
                    }
                }
                
                LoggerUtils.printIfErrorEnabled(LOGGER, "Send request fail, request={}, retryTimes={},errorMessage={}",
                        request, retryTimes, e.getMessage());
                
                exceptionThrow = e;
                
            }
            retryTimes++;
            
        }
        
        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }
        
        if (exceptionThrow != null) {
            throw (exceptionThrow instanceof NacosException) ? (NacosException) exceptionThrow
                    : new NacosException(SERVER_ERROR, exceptionThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "Request fail, unknown Error");
        }
    }
    
    /**
     * send async request.
     *
     * @param request request.
     */
    public void asyncRequest(Request request, RequestCallBack callback) throws NacosException {
        int retryTimes = 0;
        
        Exception exceptionToThrow = null;
        long start = System.currentTimeMillis();
        while (retryTimes < RETRY_TIMES && System.currentTimeMillis() < start + callback.getTimeout()) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "Client not connected.");
                }
                this.currentConnection.asyncRequest(request, callback);
                return;
            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        //wait client to re connect.
                        Thread.sleep(Math.min(100, callback.getTimeout() / 3));
                    } catch (Exception exception) {
                        //Do nothing.
                    }
                }
                LoggerUtils
                        .printIfErrorEnabled(LOGGER, "[{}]Send request fail, request={}, retryTimes={},errorMessage={}",
                                name, request, retryTimes, e.getMessage());
                exceptionToThrow = e;
                
            }
            retryTimes++;
            
        }
        
        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }
        if (exceptionToThrow != null) {
            throw (exceptionToThrow instanceof NacosException) ? (NacosException) exceptionToThrow
                    : new NacosException(SERVER_ERROR, exceptionToThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "AsyncRequest fail, unknown error");
        }
    }
    
    /**
     * send async request.
     *
     * @param request request.
     * @return request future.
     */
    public RequestFuture requestFuture(Request request) throws NacosException {
        int retryTimes = 0;
        long start = System.currentTimeMillis();
        Exception exceptionToThrow = null;
        while (retryTimes < RETRY_TIMES && System.currentTimeMillis() < start + DEFAULT_TIMEOUT_MILLS) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "Client not connected.");
                }
                return this.currentConnection.requestFuture(request);
            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        //wait client to re connect.
                        Thread.sleep(100L);
                    } catch (Exception exception) {
                        //Do nothing.
                    }
                }
                LoggerUtils
                        .printIfErrorEnabled(LOGGER, "[{}]Send request fail, request={}, retryTimes={},errorMessage={}",
                                name, request, retryTimes, e.getMessage());
                exceptionToThrow = e;
                
            }
        }
        
        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }
        
        if (exceptionToThrow != null) {
            throw (exceptionToThrow instanceof NacosException) ? (NacosException) exceptionToThrow
                    : new NacosException(SERVER_ERROR, exceptionToThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "Request future fail, unknown error");
        }
        
    }
    
    /**
     * connect to server.
     *
     * @param serverInfo server address to connect.
     * @return return connection when successfully connect to server, or null if failed.
     * @throws Exception exception when fail to connect to server.
     */
    //RpcClient.start()  serverInfo: 要连接的服务端Server信息
    public abstract Connection connectToServer(ServerInfo serverInfo) throws Exception;
    
    /**
     * handle server request.
     *
     * @param request request.
     * @return response.
     */
    protected Response handleServerRequest(final Request request) {
        
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]receive server push request,request={},requestId={}", name,
                request.getClass().getSimpleName(), request.getRequestId());
        lastActiveTimeStamp = System.currentTimeMillis();
        //
        for (ServerRequestHandler serverRequestHandler : serverRequestHandlers) {
            try {
                // 处理消息
                // ConnectionSetupRequest
                //com.alibaba.nacos.common.remote.client.RpcClient.CustomRegisterServerRequestHandler.requestReply  --> ClientDetectionRequest
                //com.alibaba.nacos.client.naming.remote.gprc.NamingPushRequestHandler.requestReply  --> NotifySubscriberRequest
                Response response = serverRequestHandler.requestReply(request);
                
                if (response != null) {
                    LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]ack server push request,request={},requestId={}", name,
                            request.getClass().getSimpleName(), request.getRequestId());
                    return response;
                }
            } catch (Exception e) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]handleServerRequest:{}, errorMessage={}", name,
                        serverRequestHandler.getClass().getName(), e.getMessage());
            }
            
        }
        return null;
    }
    
    /**
     * Register connection handler. Will be notified when inner connection's state changed.
     *
     * @param connectionEventListener connectionEventListener
     */
    public synchronized void registerConnectionListener(ConnectionEventListener connectionEventListener) {
        
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Registry connection listener to current client:{}", name,
                connectionEventListener.getClass().getName());
        this.connectionEventListeners.add(connectionEventListener);
    }
    
    /**
     * 注册serverRequestHandler，处理程序将处理来自服务器端的请求。
     *
     * Register serverRequestHandler, the handler will handle the request from server side.
     *
     * @param serverRequestHandler serverRequestHandler
     */
    public synchronized void registerServerRequestHandler(ServerRequestHandler serverRequestHandler) {
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Register server push request handler:{}", name,
                serverRequestHandler.getClass().getName());
        
        this.serverRequestHandlers.add(serverRequestHandler);
    }
    
    /**
     * Getter method for property <tt>name</tt>.
     *
     * @return property value of name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Setter method for property <tt>name</tt>.
     *
     * @param name value to be assigned to property name
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Getter method for property <tt>serverListFactory</tt>.
     *
     * @return property value of serverListFactory
     */
    public ServerListFactory getServerListFactory() {
        return serverListFactory;
    }
    
    protected ServerInfo nextRpcServer() {
        // 获得一个server的地址(随机)
        String serverAddress = getServerListFactory().genNextServer();
        return resolveServerInfo(serverAddress);
    }
    
    protected ServerInfo currentRpcServer() {
        String serverAddress = getServerListFactory().getCurrentServer();
        return resolveServerInfo(serverAddress);
    }
    
    /**
     * 组装ServerInfo， 也就是解析url ，取出地址和端口
     *
     * resolve server info.
     *
     * @param serverAddress address.
     * @return
     */
    @SuppressWarnings("PMD.UndefineMagicConstantRule")
    private ServerInfo resolveServerInfo(String serverAddress) {
        // 全局端口号
        String property = System.getProperty("nacos.server.port", "8848");
        int serverPort = Integer.parseInt(property);
        ServerInfo serverInfo = null;
        // server url是否是http协议
        if (serverAddress.contains(Constants.HTTP_PREFIX)) {
            String[] split = serverAddress.split(Constants.COLON);
            String serverIp = split[1].replaceAll("//", "");
            if (split.length > 2 && StringUtils.isNotBlank(split[2])) {
                serverPort = Integer.parseInt(split[2]);
            }
            serverInfo = new ServerInfo(serverIp, serverPort);
        } else {
            String[] split = serverAddress.split(Constants.COLON);
            String serverIp = split[0];
            if (split.length > 1 && StringUtils.isNotBlank(split[1])) {
                serverPort = Integer.parseInt(split[1]);
            }
            serverInfo = new ServerInfo(serverIp, serverPort);
        }
        return serverInfo;
    }

    /**
     * 服务信息， ip和端口
     */
    public static class ServerInfo {
        
        protected String serverIp;
        
        protected int serverPort;
        
        public ServerInfo() {
        
        }
        
        public ServerInfo(String serverIp, int serverPort) {
            this.serverPort = serverPort;
            this.serverIp = serverIp;
        }
        
        /**
         * get address, ip:port.
         *
         * @return address.
         */
        public String getAddress() {
            return serverIp + Constants.COLON + serverPort;
        }
        
        /**
         * Setter method for property <tt>serverIp</tt>.
         *
         * @param serverIp value to be assigned to property serverIp
         */
        public void setServerIp(String serverIp) {
            this.serverIp = serverIp;
        }
        
        /**
         * Setter method for property <tt>serverPort</tt>.
         *
         * @param serverPort value to be assigned to property serverPort
         */
        public void setServerPort(int serverPort) {
            this.serverPort = serverPort;
        }
        
        /**
         * Getter method for property <tt>serverIp</tt>.
         *
         * @return property value of serverIp
         */
        public String getServerIp() {
            return serverIp;
        }
        
        /**
         * Getter method for property <tt>serverPort</tt>.
         *
         * @return property value of serverPort
         */
        public int getServerPort() {
            return serverPort;
        }
        
        @Override
        public String toString() {
            return "{serverIp='" + serverIp + '\'' + ", server main port=" + serverPort + '}';
        }
    }
    
    public class ConnectionEvent {
        
        public static final int CONNECTED = 1;
        
        public static final int DISCONNECTED = 0;
        
        int eventType;
        
        public ConnectionEvent(int eventType) {
            this.eventType = eventType;
        }
        
        public boolean isConnected() {
            return eventType == CONNECTED;
        }
        
        public boolean isDisConnected() {
            return eventType == DISCONNECTED;
        }
    }
    
    /**
     * Getter method for property <tt>labels</tt>.
     *
     * @return property value of labels
     */
    public Map<String, String> getLabels() {
        return labels;
    }

    // 连接上下文
    class ReconnectContext {
        
        public ReconnectContext(ServerInfo serverInfo, boolean onRequestFail) {
            this.onRequestFail = onRequestFail;
            this.serverInfo = serverInfo;
        }
        
        boolean onRequestFail;
        
        ServerInfo serverInfo;
    }
    
    public String getTenant() {
        return tenant;
    }
    
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
}
