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

package com.alibaba.nacos.naming.remote.rpc.handler;

import cn.hutool.json.JSONUtil;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.remote.request.SubscribeServiceRequest;
import com.alibaba.nacos.api.naming.remote.response.SubscribeServiceResponse;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.common.ActionTypes;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.impl.EphemeralClientOperationServiceImpl;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.alibaba.nacos.naming.web.NamingResourceParser;
import org.springframework.stereotype.Component;

/**
 * 处理订阅请求，其实就是去拉去对应存储在server中的集群数据
 * Handler to handle subscribe service.
 *
 * @author liuzunfei
 * @author xiweng.yy
 */
@Component
public class SubscribeServiceRequestHandler extends RequestHandler<SubscribeServiceRequest, SubscribeServiceResponse> {
    
    private final ServiceStorage serviceStorage;
    
    private final NamingMetadataManager metadataManager;

    private final EphemeralClientOperationServiceImpl clientOperationService;
    
    public SubscribeServiceRequestHandler(ServiceStorage serviceStorage,
            NamingMetadataManager metadataManager,
            EphemeralClientOperationServiceImpl clientOperationService) {
        this.serviceStorage = serviceStorage;
        this.metadataManager = metadataManager;
        this.clientOperationService = clientOperationService;
    }
    
    @Override
    @Secured(action = ActionTypes.READ, parser = NamingResourceParser.class)
    public SubscribeServiceResponse handle(SubscribeServiceRequest request, RequestMeta meta) throws NacosException {
        String cId = meta.getConnectionId();
        System.out.println(cId+"::SubscribeServiceRequestHandler-->request:"+ JSONUtil.toJsonStr(request));
        System.out.println(cId+"::SubscribeServiceRequestHandler-->meta:"+ JSONUtil.toJsonStr(meta));

        String namespaceId = request.getNamespace();
        String serviceName = request.getServiceName();
        String groupName = request.getGroupName();
        String app = request.getHeader("app", "unknown");
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        Service service = Service.newService(namespaceId, groupName, serviceName, true);
        Subscriber subscriber = new Subscriber(meta.getClientIp(), meta.getClientVersion(), app,
                meta.getClientIp(), namespaceId, groupedServiceName, 0, request.getClusters());
        ServiceInfo serviceInfo = handleClusterData(serviceStorage.getData(service),
                metadataManager.getServiceMetadata(service).orElse(null),
                subscriber);
        if (request.isSubscribe()) {
            clientOperationService.subscribeService(service, subscriber, meta.getConnectionId());
        } else {
            clientOperationService.unsubscribeService(service, subscriber, meta.getConnectionId());
        }
        System.out.println(cId+"::SubscribeServiceRequestHandler-->response:"+ JSONUtil.toJsonStr(serviceInfo));
        return new SubscribeServiceResponse(ResponseCode.SUCCESS.getCode(), "success", serviceInfo);
    }
    
    /**
     * 用于适应推送集群功能。
     * For adapt push cluster feature. Will be remove after 2.1.x.
     *
     * @param data       original data
     * @param metadata   service metadata
     * @param subscriber subscriber information
     * @return cluster filtered data
     */
    @Deprecated
    private ServiceInfo handleClusterData(ServiceInfo data, ServiceMetadata metadata, Subscriber subscriber) {
        return StringUtils.isBlank(subscriber.getCluster()) ? data
                : ServiceUtil.selectInstancesWithHealthyProtection(data, metadata, subscriber.getCluster());
    }
}
