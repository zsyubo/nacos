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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 服务信息更新服务。
 * 这个是做定时更新的
 *
 * Service information update service.
 *
 * @author xiweng.yy
 */
public class ServiceInfoUpdateService implements Closeable {

    // 1s
    private static final long DEFAULT_DELAY = 1000L;
    
    private static final int DEFAULT_UPDATE_CACHE_TIME_MULTIPLE = 6;

    // 任务map
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final ScheduledExecutorService executor;
    
    private final NamingClientProxy namingClientProxy;
    
    private final InstancesChangeNotifier changeNotifier;
    
    public ServiceInfoUpdateService(Properties properties, ServiceInfoHolder serviceInfoHolder,
            NamingClientProxy namingClientProxy, InstancesChangeNotifier changeNotifier) {
        // 初始化执行器
        this.executor = new ScheduledThreadPoolExecutor(initPollingThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.naming.updater"));
        this.serviceInfoHolder = serviceInfoHolder;
        this.namingClientProxy = namingClientProxy;
        this.changeNotifier = changeNotifier;
    }
    
    private int initPollingThreadCount(Properties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }
        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
                UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }
    
    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (futureMap.get(serviceKey) != null) {
            return;
        }
        // 直接锁
        synchronized (futureMap) {
            if (futureMap.get(serviceKey) != null) {
                return;
            }
            
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, groupName, clusters));
            futureMap.put(serviceKey, future);
        }
    }
    
    private synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Stop to schedule update if contain task.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void stopUpdateIfContain(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (!futureMap.containsKey(serviceKey)) {
            return;
        }
        synchronized (futureMap) {
            if (!futureMap.containsKey(serviceKey)) {
                return;
            }
            futureMap.remove(serviceKey);
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    public class UpdateTask implements Runnable {
        
        long lastRefTime = Long.MAX_VALUE;
        
        private final String serviceName;
        
        private final String groupName;
        
        private final String clusters;
        
        private final String groupedServiceName;
        
        private final String serviceKey;
    
        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;
        
        public UpdateTask(String serviceName, String groupName, String clusters) {
            this.serviceName = serviceName;
            this.groupName = groupName;
            this.clusters = clusters;
            this.groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
            this.serviceKey = ServiceInfo.getKey(groupedServiceName, clusters);
        }

        /**
         * 定时去增量同步的
         */
        @Override
        public void run() {
            long delayTime = DEFAULT_DELAY;
            
            try {
                if (!changeNotifier.isSubscribed(groupName, serviceName, clusters) && !futureMap.containsKey(serviceKey)) {
                    NAMING_LOGGER
                            .info("update task is stopped, service:" + groupedServiceName + ", clusters:" + clusters);
                    return;
                }

                // 从Map中取出server信息
                ServiceInfo serviceObj = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
                // 空的话，去查
                if (serviceObj == null) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    serviceInfoHolder.processServiceInfo(serviceObj);
                    lastRefTime = serviceObj.getLastRefTime();
                    return;
                }

                //正常是相同的，也就是同步成功的时间
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    serviceInfoHolder.processServiceInfo(serviceObj);
                }
                lastRefTime = serviceObj.getLastRefTime();
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                // serviceObj.getCacheMillis() 也就是服务端可以控制client缓存时间？
                // TODO multiple time can be configured.    默认6秒
                delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                // 重置失败计数
                resetFailCount();
            } catch (Throwable e) {
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + groupedServiceName, e);
            } finally {
                // 去发起下一次更新,如果更新正常，那么下一次触发时间为60s
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
                // delayTime << failCount  如果失败了，就指数级时间重试
            }
        }
    
        private void incFailCount() {
            //2
            //4
            //8
            //16
            //32
            //64
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }
    
        private void resetFailCount() {
            failCount = 0;
        }
    }
}
