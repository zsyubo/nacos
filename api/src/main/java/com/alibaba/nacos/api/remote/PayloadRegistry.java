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

package com.alibaba.nacos.api.remote;

import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 有效载荷注册表，包括所有请求和响应。
 *
 * payload regitry,include all request and response.
 *
 * @author liuzunfei
 * @version $Id: PayloadRegistry.java, v 0.1 2020年09月01日 10:56 AM liuzunfei Exp $
 */

public class PayloadRegistry {
    
    private static final Map<String, Class> REGISTRY_REQUEST = new HashMap<String, Class>();
    
    static boolean initialized = false;
    
    public static void init() {
        // 扫描Request、Response
        scan();
    }
    
    private static synchronized void scan() {
        if (initialized) {
            return;
        }

        // com.alibaba.nacos.api.remote.request client通信的
        // com.alibaba.nacos.api.config.remote.request 配置中心
        // com.alibaba.nacos.naming.cluster.remote.request  集群同步的
        List<String> requestScanPackage = Arrays
                .asList("com.alibaba.nacos.api.naming.remote.request", "com.alibaba.nacos.api.config.remote.request",
                        "com.alibaba.nacos.api.remote.request", "com.alibaba.nacos.naming.cluster.remote.request");
        for (String pkg : requestScanPackage) {
            Reflections reflections = new Reflections(pkg);
            Set<Class<? extends Request>> subTypesRequest = reflections.getSubTypesOf(Request.class);
            for (Class clazz : subTypesRequest) {
                String config = System.getProperty("println.scanLog");
                if(config != null && config.equals("true") ) {
                    System.out.println("request: " + clazz.getSimpleName() + ":" + clazz.getName());
                }
                register(clazz.getSimpleName(), clazz);
            }
        }
        //request: ServiceListRequest:com.alibaba.nacos.api.naming.remote.request.ServiceListRequest
        //request: ServiceQueryRequest:com.alibaba.nacos.api.naming.remote.request.ServiceQueryRequest
        //request: InstanceRequest:com.alibaba.nacos.api.naming.remote.request.InstanceRequest
        //request: AbstractNamingRequest:com.alibaba.nacos.api.naming.remote.request.AbstractNamingRequest
        //request: SubscribeServiceRequest:com.alibaba.nacos.api.naming.remote.request.SubscribeServiceRequest
        //request: NotifySubscriberRequest:com.alibaba.nacos.api.naming.remote.request.NotifySubscriberRequest
        //request: ServerRequest:com.alibaba.nacos.api.remote.request.ServerRequest
        //request: ConfigQueryRequest:com.alibaba.nacos.api.config.remote.request.ConfigQueryRequest
        //request: ConfigRemoveRequest:com.alibaba.nacos.api.config.remote.request.ConfigRemoveRequest
        //request: ConfigBatchListenRequest:com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest
        //request: ConfigChangeNotifyRequest:com.alibaba.nacos.api.config.remote.request.ConfigChangeNotifyRequest
        //request: ConfigChangeClusterSyncRequest:com.alibaba.nacos.api.config.remote.request.cluster.ConfigChangeClusterSyncRequest
        //request: ConfigPublishRequest:com.alibaba.nacos.api.config.remote.request.ConfigPublishRequest
        //request: AbstractConfigRequest:com.alibaba.nacos.api.config.remote.request.AbstractConfigRequest
        //request: ServerRequest:com.alibaba.nacos.api.remote.request.ServerRequest
        //request: ClientConfigMetricRequest:com.alibaba.nacos.api.config.remote.request.ClientConfigMetricRequest
        //request: ServerReloadRequest:com.alibaba.nacos.api.remote.request.ServerReloadRequest
        //request: PushAckRequest:com.alibaba.nacos.api.remote.request.PushAckRequest
        //request: ServerLoaderInfoRequest:com.alibaba.nacos.api.remote.request.ServerLoaderInfoRequest
        //request: InternalRequest:com.alibaba.nacos.api.remote.request.InternalRequest
        //request: ServerCheckRequest:com.alibaba.nacos.api.remote.request.ServerCheckRequest
        //request: HealthCheckRequest:com.alibaba.nacos.api.remote.request.HealthCheckRequest
        //request: ConnectionSetupRequest:com.alibaba.nacos.api.remote.request.ConnectionSetupRequest
        //request: ClientDetectionRequest:com.alibaba.nacos.api.remote.request.ClientDetectionRequest
        //request: ServerRequest:com.alibaba.nacos.api.remote.request.ServerRequest
        //request: ConnectResetRequest:com.alibaba.nacos.api.remote.request.ConnectResetRequest
        //request: DistroDataRequest:com.alibaba.nacos.naming.cluster.remote.request.DistroDataRequest
        //request: AbstractClusterRequest:com.alibaba.nacos.naming.cluster.remote.request.AbstractClusterRequest
        
        List<String> responseScanPackage = Arrays
                .asList("com.alibaba.nacos.api.naming.remote.response",
                "com.alibaba.nacos.api.config.remote.response", "com.alibaba.nacos.api.remote.response",
                "com.alibaba.nacos.naming.cluster.remote.response");
        for (String pkg : responseScanPackage) {
            Reflections reflections = new Reflections(pkg);
            Set<Class<? extends Response>> subTypesOfResponse = reflections.getSubTypesOf(Response.class);
            for (Class clazz : subTypesOfResponse) {
                String config = System.getProperty("println.initLog");
                if(config!=null && config.equals("true") ){
                    System.out.println("response: "+clazz.getSimpleName()+":"+clazz.getName());
                }

                register(clazz.getSimpleName(), clazz);
            }
        }
        //response: NotifySubscriberResponse:com.alibaba.nacos.api.naming.remote.response.NotifySubscriberResponse
        //response: SubscribeServiceResponse:com.alibaba.nacos.api.naming.remote.response.SubscribeServiceResponse
        //response: ServiceListResponse:com.alibaba.nacos.api.naming.remote.response.ServiceListResponse
        //response: InstanceResponse:com.alibaba.nacos.api.naming.remote.response.InstanceResponse
        //response: QueryServiceResponse:com.alibaba.nacos.api.naming.remote.response.QueryServiceResponse
        //response: ConfigChangeNotifyResponse:com.alibaba.nacos.api.config.remote.response.ConfigChangeNotifyResponse
        //response: ConfigQueryResponse:com.alibaba.nacos.api.config.remote.response.ConfigQueryResponse
        //response: ClientConfigMetricResponse:com.alibaba.nacos.api.config.remote.response.ClientConfigMetricResponse
        //response: ConfigChangeBatchListenResponse:com.alibaba.nacos.api.config.remote.response.ConfigChangeBatchListenResponse
        //response: ConfigRemoveResponse:com.alibaba.nacos.api.config.remote.response.ConfigRemoveResponse
        //response: ConfigChangeClusterSyncResponse:com.alibaba.nacos.api.config.remote.response.cluster.ConfigChangeClusterSyncResponse
        //response: ConfigPublishResponse:com.alibaba.nacos.api.config.remote.response.ConfigPublishResponse
        //response: ServerCheckResponse:com.alibaba.nacos.api.remote.response.ServerCheckResponse
        //response: ConnectResetResponse:com.alibaba.nacos.api.remote.response.ConnectResetResponse
        //response: ServerLoaderInfoResponse:com.alibaba.nacos.api.remote.response.ServerLoaderInfoResponse
        //response: HealthCheckResponse:com.alibaba.nacos.api.remote.response.HealthCheckResponse
        //response: ErrorResponse:com.alibaba.nacos.api.remote.response.ErrorResponse
        //response: ServerReloadResponse:com.alibaba.nacos.api.remote.response.ServerReloadResponse
        //response: ClientDetectionResponse:com.alibaba.nacos.api.remote.response.ClientDetectionResponse
        //response: DistroDataResponse:com.alibaba.nacos.naming.cluster.remote.response.DistroDataResponse
        initialized = true;
    }
    
    static void register(String type, Class clazz) {
        if (Modifier.isAbstract(clazz.getModifiers())) {
            return;
        }
        if (Modifier.isInterface(clazz.getModifiers())) {
            return;
        }
        if (REGISTRY_REQUEST.containsKey(type)) {
            throw new RuntimeException(String.format("Fail to register, type:%s ,clazz:%s ", type, clazz.getName()));
        }
        REGISTRY_REQUEST.put(type, clazz);
    }
    
    public static Class getClassByType(String type) {
        return REGISTRY_REQUEST.get(type);
    }
}
