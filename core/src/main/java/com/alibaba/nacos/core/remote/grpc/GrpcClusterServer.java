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

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.core.utils.GlobalExecutor;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Server间GRPC同步
 * Grpc implementation as  a rpc server.
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
@Service
public class GrpcClusterServer extends BaseGrpcServer {
    
    private static final int PORT_OFFSET = 1001;

    public GrpcClusterServer() {
//        System.out.println("GrpcClusterServer()");
    }

    @Override
    public int rpcPortOffset() {
        return PORT_OFFSET;
    }

    @Override
    public void startServer(String className) throws Exception {
        System.out.println("GrpcClusterServer()::startServer");
        super.startServer("GrpcClusterServer");
    }

    @Override
    public ThreadPoolExecutor getRpcExecutor() {
        if (!GlobalExecutor.clusterRpcExecutor.allowsCoreThreadTimeOut()) {
            GlobalExecutor.clusterRpcExecutor.allowCoreThreadTimeOut(true);
        }
        return GlobalExecutor.clusterRpcExecutor;
    }
}
