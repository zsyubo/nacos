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

package com.alibaba.nacos.client.naming;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.Balancer;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.remote.NamingClientProxyDelegate;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.InitUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.utils.ValidatorUtils;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Nacos Naming Service.
 *
 * @author nkorange
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosNamingService implements NamingService {
    
    private static final String DEFAULT_NAMING_LOG_FILE_PATH =  "naming.log";
    
    private static final String UP = "UP";
    
    private static final String DOWN = "DOWN";
    
    /**
     * Each Naming service should have different namespace.
     */
    private String namespace;
    // ps...貌似没地方用到啊
    private String logName;
    
    private ServiceInfoHolder serviceInfoHolder;
    
    private InstancesChangeNotifier changeNotifier;

    // NamingClientProxyDelegate
    private NamingClientProxy clientProxy;
    
    public NacosNamingService(String serverList) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);
        init(properties);
    }

    // properties: NacosProperties
    public NacosNamingService(Properties properties) throws NacosException {
//properties:
//        "secretKey" -> ""
//        "namespace" -> ""
//        "username" -> "nacos"
//        "namingLoadCacheAtStart" -> "false"
//        "serverAddr" -> "ubuntu.wsl:8848"
//        "com.alibaba.nacos.naming.log.filename" -> ""
//        "clusterName" -> "DEFAULT"
//        "password" -> "nacos"
//        "accessKey" -> ""
//        "endpoint" -> ""
        init(properties);
    }

    //NacosNamingService#NacosNamingService(java.util.Properties)
    //    properties: NacosProperties
    private void init(Properties properties) throws NacosException {
        // 检查配置文件是否有错误
        ValidatorUtils.checkInitParam(properties);
        // 初始化NameSpace
        this.namespace = InitUtils.initNamespaceForNaming(properties);
        // 序列化相关的，  所以这里将NoneSelector、ExpressionSelector这两个类进行注册或者销毁
        InitUtils.initSerialization();
        //初始化 web root context，也就是访问路径吧 ，有可能存在自定义服务名的情况
        InitUtils.initWebRootContext(properties);
        // 初始化Log
        initLogName(properties);
        // 观察者模式
        this.changeNotifier = new InstancesChangeNotifier();
        // 注册一个发布者？
        NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
        // 注册一个订阅者(消费者)
        NotifyCenter.registerSubscriber(changeNotifier);
        // 服务持有者，里面存放了从注册中心拉去的instance
        this.serviceInfoHolder = new ServiceInfoHolder(namespace, properties);
        // 代理，一些网络操作封装
        this.clientProxy = new NamingClientProxyDelegate(this.namespace, serviceInfoHolder, properties, changeNotifier);
    }
    
    private void initLogName(Properties properties) {
        //优先是系统参数，然后才是系统配置文件  -Dcom.alibaba.nacos.naming.log.filename=name.log
        logName = System.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME);
        if (StringUtils.isEmpty(logName)) {
            // 从配置文件读取logname
            if (properties != null && StringUtils
                    .isNotEmpty(properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME))) {
                logName = properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME);
            } else {
                logName = DEFAULT_NAMING_LOG_FILE_PATH;
            }
        }
    }
    
    @Override
    public void registerInstance(String serviceName, String ip, int port) throws NacosException {
        registerInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        registerInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void registerInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setWeight(1.0);
        instance.setClusterName(clusterName);
        registerInstance(serviceName, groupName, instance);
    }
    
    @Override
    public void registerInstance(String serviceName, Instance instance) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        NamingUtils.checkInstanceIsLegal(instance);
        clientProxy.registerService(serviceName, groupName, instance);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setClusterName(clusterName);
        deregisterInstance(serviceName, groupName, instance);
    }
    
    @Override
    public void deregisterInstance(String serviceName, Instance instance) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        clientProxy.deregisterService(serviceName, groupName, instance);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<String>());
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<String>());
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, boolean subscribe) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<String>(), subscribe);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, boolean subscribe)
            throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<String>(), subscribe);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters) throws NacosException {
        return getAllInstances(serviceName, clusters, true);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters)
            throws NacosException {
        return getAllInstances(serviceName, groupName, clusters, true);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException {
        return getAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters,
            boolean subscribe) throws NacosException {
        ServiceInfo serviceInfo;
        String clusterString = StringUtils.join(clusters, ",");
        // true   com.alibaba.cloud.nacos.discovery.NacosDiscoveryClient#getInstances
        if (subscribe) {
            serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            if (null == serviceInfo) {
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
        } else {
            serviceInfo = clientProxy.queryInstancesOfService(serviceName, groupName, clusterString, 0, false);
        }
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<Instance>();
        }
        return list;
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, new ArrayList<String>(), healthy);
    }

    // namingService().selectInstances(serviceId, group, true);
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy) throws NacosException {
//        serviceName: shop-user-service
//        groupName: DEFAULT_GROUP
//        healthy:true
        return selectInstances(serviceName, groupName, healthy, true);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, new ArrayList<String>(), healthy, subscribe);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy, boolean subscribe)
            throws NacosException {
//        serviceName: shop-user-service
//        groupName: DEFAULT_GROUP
        //new ArrayList<String>()
        //healthy: true
        //subscribe: true
        return selectInstances(serviceName, groupName, new ArrayList<String>(), healthy, subscribe);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy)
            throws NacosException {
        return selectInstances(serviceName, clusters, healthy, true);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy)
            throws NacosException {
        return selectInstances(serviceName, groupName, clusters, healthy, true);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, Constants.DEFAULT_GROUP, clusters, healthy, subscribe);
    }

    //serviceName: shop-user-service
    //groupName: DEFAULT_GROUP
    //new ArrayList<String>()
    //healthy: true
    //subscribe: true
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy,
            boolean subscribe) throws NacosException {
        // clusters 限定的集群列表

        ServiceInfo serviceInfo;
        String clusterString = StringUtils.join(clusters, ",");
        // 是否订阅, 默认为true
        if (subscribe) {
            // 从本地列表中获取
            serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            if (null == serviceInfo) {
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
        } else {
            serviceInfo = clientProxy.queryInstancesOfService(serviceName, groupName, clusterString, 0, false);
        }
        return selectInstances(serviceInfo, healthy);
    }
    
    private List<Instance> selectInstances(ServiceInfo serviceInfo, boolean healthy) {
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<Instance>();
        }
        
        Iterator<Instance> iterator = list.iterator();
        while (iterator.hasNext()) {
            Instance instance = iterator.next();
            // 不健康的就移除
            if (healthy != instance.isHealthy() || !instance.isEnabled() || instance.getWeight() <= 0) {
                iterator.remove();
            }
        }
        
        return list;
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<String>());
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, true);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, boolean subscribe) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<String>(), subscribe);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, boolean subscribe)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, new ArrayList<String>(), subscribe);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters) throws NacosException {
        return selectOneHealthyInstance(serviceName, clusters, true);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, clusters, true);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters,
            boolean subscribe) throws NacosException {
        String clusterString = StringUtils.join(clusters, ",");
        if (subscribe) {
            ServiceInfo serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            if (null == serviceInfo) {
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
            return Balancer.RandomByWeight.selectHost(serviceInfo);
        } else {
            ServiceInfo serviceInfo = clientProxy
                    .queryInstancesOfService(serviceName, groupName, clusterString, 0, false);
            return Balancer.RandomByWeight.selectHost(serviceInfo);
        }
    }
    
    @Override
    public void subscribe(String serviceName, EventListener listener) throws NacosException {
        subscribe(serviceName, new ArrayList<String>(), listener);
    }
    
    @Override
    public void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        subscribe(serviceName, groupName, new ArrayList<String>(), listener);
    }
    
    @Override
    public void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        subscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }
    
    @Override
    public void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException {
        if (null == listener) {
            return;
        }
        String clusterString = StringUtils.join(clusters, ",");
        changeNotifier.registerListener(groupName, serviceName, clusterString, listener);
        clientProxy.subscribe(serviceName, groupName, clusterString);
    }
    
    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, new ArrayList<String>(), listener);
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, groupName, new ArrayList<String>(), listener);
    }
    
    @Override
    public void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        unsubscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException {
        String clustersString = StringUtils.join(clusters, ",");
        changeNotifier.deregisterListener(groupName, serviceName, clustersString, listener);
        if (!changeNotifier.isSubscribed(groupName, serviceName, clustersString)) {
            clientProxy.unsubscribe(serviceName, groupName, clustersString);
        }
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP);
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, groupName, null);
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector)
            throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP, selector);
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException {
        return clientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }
    
    @Override
    public List<ServiceInfo> getSubscribeServices() {
        return changeNotifier.getSubscribeServices();
    }
    
    @Override
    public String getServerStatus() {
        return clientProxy.serverHealthy() ? UP : DOWN;
    }
    
    @Override
    public void shutDown() throws NacosException {
        serviceInfoHolder.shutdown();
        clientProxy.shutdown();
    }
}
