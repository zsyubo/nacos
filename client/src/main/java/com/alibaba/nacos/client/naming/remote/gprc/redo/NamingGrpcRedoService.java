/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.naming.remote.gprc.redo;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.InstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.SubscriberRedoData;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 命名客户gprc重做服务。
 * 当连接重新连接到服务器时，重做注册和订阅。
 *
 * NamingGrpcRedoService实现接口ConnectionEventListener，负责处理连接变更，并周期性执行客户端的注册和订阅的重置操作（满足条件情况下），其实就是为了在客户端连接失效然后再重新建立连接之后，提供了一个服务重新注册和订阅的操作。
 *  https://zhuanlan.zhihu.com/p/508902797
 *
 * Naming client gprc redo service.
 *
 * <p>When connection reconnect to server, redo the register and subscribe.
 *
 * @author xiweng.yy
 */
public class NamingGrpcRedoService implements ConnectionEventListener {
    
    private static final String REDO_THREAD_NAME = "com.alibaba.nacos.client.naming.grpc.redo";
    
    private static final int REDO_THREAD = 1;
    
    /**
     * TODO get redo delay from config.
     */
    private static final long DEFAULT_REDO_DELAY = 3000L;
    
    private final ConcurrentMap<String, InstanceRedoData> registeredInstances = new ConcurrentHashMap<>();
    
    private final ConcurrentMap<String, SubscriberRedoData> subscribes = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService redoExecutor;
    
    private volatile boolean connected = false;
    
    public NamingGrpcRedoService(NamingGrpcClientProxy clientProxy) {
        // 创建一个执行器
        this.redoExecutor = new ScheduledThreadPoolExecutor(REDO_THREAD, new NameThreadFactory(REDO_THREAD_NAME));
        // 延时3秒 ，无限循环
        this.redoExecutor.scheduleWithFixedDelay(new RedoScheduledTask(clientProxy, this), DEFAULT_REDO_DELAY,
                DEFAULT_REDO_DELAY, TimeUnit.MILLISECONDS);
    }
    
    public boolean isConnected() {
        return connected;
    }
    
    @Override
    public void onConnected() {
        connected = true;
        LogUtils.NAMING_LOGGER.info("Grpc connection connect");
    }
    
    @Override
    public void onDisConnect() {
        connected = false;
        LogUtils.NAMING_LOGGER.warn("Grpc connection disconnect, mark to redo");
        synchronized (registeredInstances) {
            registeredInstances.values().forEach(instanceRedoData -> instanceRedoData.setRegistered(false));
        }
        synchronized (subscribes) {
            subscribes.values().forEach(subscriberRedoData -> subscriberRedoData.setRegistered(false));
        }
        LogUtils.NAMING_LOGGER.warn("mark to redo completed");
    }
    
    /**
     * Cache registered instance for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param instance    registered instance
     */
    public void cacheInstanceForRedo(String serviceName, String groupName, Instance instance) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        InstanceRedoData redoData = InstanceRedoData.build(serviceName, groupName, instance);
        synchronized (registeredInstances) {
            registeredInstances.put(key, redoData);
        }
    }
    
    /**
     * Instance register successfully, mark registered status as {@code true}.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void instanceRegistered(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.setRegistered(true);
            }
        }
    }
    
    /**
     * Instance deregister, mark unregistering status as {@code true}.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void instanceDeregister(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
            }
        }
    }
    
    /**
     * Remove registered instance for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void removeInstanceForRedo(String serviceName, String groupName) {
        synchronized (registeredInstances) {
            registeredInstances.remove(NamingUtils.getGroupedName(serviceName, groupName));
        }
    }
    
    /**
     * 找到所有需要重做的实例重做数据。
     *
     * Find all instance redo data which need do redo.
     *
     * @return set of {@code InstanceRedoData} need to do redo.
     */
    public Set<InstanceRedoData> findInstanceRedoData() {
        Set<InstanceRedoData> result = new HashSet<>();
        synchronized (registeredInstances) {
            for (InstanceRedoData each : registeredInstances.values()) {
                if (each.isNeedRedo()) {
                    result.add(each);
                }
            }
        }
        return result;
    }
    
    /**
     * Cache subscriber for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void cacheSubscriberForRedo(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        SubscriberRedoData redoData = SubscriberRedoData.build(serviceName, groupName, cluster);
        synchronized (subscribes) {
            subscribes.put(key, redoData);
        }
    }
    
    /**
     * Subscriber register successfully, mark registered status as {@code true}.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void subscriberRegistered(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        // 处理重试相关的  , 标注为已注册
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData) {
                redoData.setRegistered(true);
            }
        }
    }
    
    /**
     * Subscriber deregister, mark unregistering status as {@code true}.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void subscriberDeregister(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
            }
        }
    }
    
    /**
     * Remove subscriber for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void removeSubscriberForRedo(String serviceName, String groupName, String cluster) {
        synchronized (subscribes) {
            subscribes.remove(ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster));
        }
    }
    
    /**
     * Find all subscriber redo data which need do redo.
     *
     * @return set of {@code SubscriberRedoData} need to do redo.
     */
    public Set<SubscriberRedoData> findSubscriberRedoData() {
        Set<SubscriberRedoData> result = new HashSet<>();
        synchronized (subscribes) {
            for (SubscriberRedoData each : subscribes.values()) {
                if (each.isNeedRedo()) {
                    result.add(each);
                }
            }
        }
        return result;
    }
    
    /**
     * Shutdown redo service.
     */
    public void shutdown() {
        LogUtils.NAMING_LOGGER.info("Shutdown grpc redo service executor " + redoExecutor);
        registeredInstances.clear();
        subscribes.clear();
        redoExecutor.shutdownNow();
    }
    
}
