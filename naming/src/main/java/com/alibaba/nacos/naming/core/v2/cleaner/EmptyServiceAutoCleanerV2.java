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

package com.alibaba.nacos.naming.core.v2.cleaner;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.index.ClientServiceIndexesManager;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * 清理空服务的
 * Empty service auto cleaner for v2.x.
 *
 * @author xiweng.yy
 */
@Component
public class EmptyServiceAutoCleanerV2 extends AbstractNamingCleaner {
    
    private static final String EMPTY_SERVICE = "emptyService";
    
    private final ClientServiceIndexesManager clientServiceIndexesManager;
    
    private final ServiceStorage serviceStorage;
    
    public EmptyServiceAutoCleanerV2(ClientServiceIndexesManager clientServiceIndexesManager,
            ServiceStorage serviceStorage) {
        this.clientServiceIndexesManager = clientServiceIndexesManager;
        this.serviceStorage = serviceStorage;
        // 这个是清理空服务
        // 你妈的。。直接在构造的时候初始化定时任务， 30S一次，延时60s
        GlobalExecutor.scheduleExpiredClientCleaner(this, TimeUnit.SECONDS.toMillis(30),
                GlobalConfig.getEmptyServiceCleanInterval(), TimeUnit.MILLISECONDS);
        
    }
    
    @Override
    public String getType() {
        return EMPTY_SERVICE;
    }
    
    @Override
    public void doClean() {
        ServiceManager serviceManager = ServiceManager.getInstance();
        // Parallel flow opening threshold
        int parallelSize = 100;
        
        for (String each : serviceManager.getAllNamespaces()) {
            Set<Service> services = serviceManager.getSingletons(each);
            Stream<Service> stream = services.size() > parallelSize ? services.parallelStream() : services.stream();
            stream.forEach(this::cleanEmptyService);
        }
    }
    
    private void cleanEmptyService(Service service) {
        // 获取注册的client instance
        Collection<String> registeredService = clientServiceIndexesManager.getAllClientsRegisteredService(service);
        //                                  60秒过期
        if (registeredService.isEmpty() && isTimeExpired(service)) {
            Loggers.SRV_LOG.warn("namespace : {}, [{}] services are automatically cleaned", service.getNamespace(),
                    service.getGroupedServiceName());
            clientServiceIndexesManager.removePublisherIndexesByEmptyService(service);
            // 从ServiceManager中删除这个实例
            ServiceManager.getInstance().removeSingleton(service);
            serviceStorage.removeData(service);
            NotifyCenter.publishEvent(new MetadataEvent.ServiceMetadataEvent(service, true));
        }
    }
    
    private boolean isTimeExpired(Service service) {
        long currentTimeMillis = System.currentTimeMillis();
        return currentTimeMillis - service.getLastUpdatedTime() >= GlobalConfig.getEmptyServiceExpiredTime();
    }
}
