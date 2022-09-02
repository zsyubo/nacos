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
import com.alibaba.nacos.api.naming.remote.request.ServiceQueryRequest;
import com.alibaba.nacos.api.naming.remote.response.QueryServiceResponse;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.common.ActionTypes;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.alibaba.nacos.naming.web.NamingResourceParser;
import org.springframework.stereotype.Component;

/**
 * Nacos query instances request handler.
 *
 * @author xiweng.yy
 */
@Component
public class ServiceQueryRequestHandler extends RequestHandler<ServiceQueryRequest, QueryServiceResponse> {
    
    private final ServiceStorage serviceStorage;
    
    private final NamingMetadataManager metadataManager;
    
    public ServiceQueryRequestHandler(ServiceStorage serviceStorage,
                                      NamingMetadataManager metadataManager) {
        this.serviceStorage = serviceStorage;
        this.metadataManager = metadataManager;
    }
    
    @Override
    @Secured(action = ActionTypes.READ, parser = NamingResourceParser.class)
    public QueryServiceResponse handle(ServiceQueryRequest request, RequestMeta meta) throws NacosException {
        String cId = meta.getConnectionId();
        System.out.println(cId+"::ServiceQueryRequestHandler-->request:"+ JSONUtil.toJsonStr(request));
        System.out.println(cId+"::ServiceQueryRequestHandler-->meta:"+ JSONUtil.toJsonStr(meta));

        String namespaceId = request.getNamespace();
        String groupName = request.getGroupName();
        String serviceName = request.getServiceName();
        Service service = Service.newService(namespaceId, groupName, serviceName);
        String cluster = null == request.getCluster() ? "" : request.getCluster();
        boolean healthyOnly = request.isHealthyOnly();
        ServiceInfo result = serviceStorage.getData(service);
        // 一般为空
        ServiceMetadata serviceMetadata = metadataManager.getServiceMetadata(service).orElse(null);
        result = ServiceUtil.selectInstancesWithHealthyProtection(result, serviceMetadata, cluster, healthyOnly, true);

        QueryServiceResponse response = QueryServiceResponse.buildSuccessResponse(result);
        System.out.println(cId+"::ServiceQueryRequestHandler-->response:"+ JSONUtil.toJsonStr(response));
        return response;
    }
}
