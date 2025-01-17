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

import cn.hutool.core.util.IdUtil;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.utils.ReflectUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.utils.Loggers;
import io.grpc.Attributes;
import io.grpc.CompressorRegistry;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.DecompressorRegistry;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.internal.ServerStream;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Grpc implementation as a rpc server.
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
public abstract class BaseGrpcServer extends BaseRpcServer {
    
    private Server server;
    
    private static final String REQUEST_BI_STREAM_SERVICE_NAME = "BiRequestStream";
    
    private static final String REQUEST_BI_STREAM_METHOD_NAME = "requestBiStream";
    
    private static final String REQUEST_SERVICE_NAME = "Request";
    
    private static final String REQUEST_METHOD_NAME = "request";
    
    private static final String GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY = "nacos.remote.server.grpc.maxinbound.message.size";
    
    private static final long DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE = 10 * 1024 * 1024;
    
    @Autowired
    private GrpcRequestAcceptor grpcCommonRequestAcceptor;
    
    @Autowired
    private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }
    
    @Override
    public void startServer(String className) throws Exception {
        System.out.println("className:"+className);
        // GRPC的服务注册器
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();

        // 拦截器
        //服务器拦截器来设置连接ID。 抽取成一个静态内部类了
        // server interceptor to set connection id.
        ServerInterceptor serverInterceptor = new CustomServerInterceptor();
        //grpc中添加处理服务
        //MutableHandlerRegistry
        //BaseGrpcServer.CustomServerInterceptor
        addServices(className,handlerRegistry, serverInterceptor);
        //构造grpc服务端
        server = ServerBuilder.forPort(getServicePort())
                // getRpcExecutor： 线程池
                .executor(getRpcExecutor())
                // 设置最大消息为10M
                .maxInboundMessageSize(getInboundMessageSize())
                .fallbackHandlerRegistry(handlerRegistry)
                .compressorRegistry(CompressorRegistry.getDefaultInstance())  // 压缩配置
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())  // 解压配置
                .addTransportFilter(new ServerTransportFilter() {
                    //用于创建和删除服务器传输的筛选机制
                    // 也就是连接和关闭连接时的过滤器
                    @Override
                    public Attributes transportReady(Attributes transportAttrs) {
                        // 第一次建立连接时会进来
                        System.out.println("1.连接建立了");
                        //远程地址
                        InetSocketAddress remoteAddress = (InetSocketAddress) transportAttrs
                                .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                        // 本地地址
                        InetSocketAddress localAddress = (InetSocketAddress) transportAttrs
                                .get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
                        int remotePort = remoteAddress.getPort();
                        int localPort = localAddress.getPort();
                        String remoteIp = remoteAddress.getAddress().getHostAddress();
                        // 这地方当有新的请求进来时，去自动生成对应的key
                        Attributes attrWrapper = transportAttrs.toBuilder()
                                // 初始哈conn_id
                                .set(TRANS_KEY_CONN_ID, System.currentTimeMillis() + "_" + remoteIp + "_" + remotePort)
                                //
                                .set(TRANS_KEY_REMOTE_IP, remoteIp).set(TRANS_KEY_REMOTE_PORT, remotePort)
                                .set(TRANS_KEY_LOCAL_PORT, localPort).build();
                        String connectionId = attrWrapper.get(TRANS_KEY_CONN_ID);
                        Loggers.REMOTE_DIGEST.info("Connection transportReady,connectionId = {} ", connectionId);
                        return attrWrapper;
                        
                    }
                    
                    @Override
                    public void transportTerminated(Attributes transportAttrs) {
                        String connectionId = null;
                        try {
                            // 获取conn_id
                            connectionId = transportAttrs.get(TRANS_KEY_CONN_ID);
                        } catch (Exception e) {
                            // Ignore
                        }
                        if (StringUtils.isNotBlank(connectionId)) {
                            Loggers.REMOTE_DIGEST
                                    .info("Connection transportTerminated,connectionId = {} ", connectionId);
                            // todo 从connectionManager取消conn_id
                            connectionManager.unregister(connectionId);
                        }
                    }
                }).build();
        
        server.start();
    }

    /**
     * 自定义的
     */
    public  final class CustomServerInterceptor implements ServerInterceptor{
        @Override
        public <T, S> ServerCall.Listener<T> interceptCall(ServerCall<T, S> call, Metadata headers,
                ServerCallHandler<T, S> next) {
            // 每次调用都会进来
//            System.out.println("2.来了来了");
            Context ctx = Context.current()
                    .withValue(CONTEXT_KEY_CONN_ID, call.getAttributes().get(TRANS_KEY_CONN_ID))
                    .withValue(CONTEXT_KEY_CONN_REMOTE_IP, call.getAttributes().get(TRANS_KEY_REMOTE_IP))
                    .withValue(CONTEXT_KEY_CONN_REMOTE_PORT, call.getAttributes().get(TRANS_KEY_REMOTE_PORT))
                    .withValue(CONTEXT_KEY_CONN_LOCAL_PORT, call.getAttributes().get(TRANS_KEY_LOCAL_PORT));
            ctx = ctx.withValue(TRAD_ID, IdUtil.fastSimpleUUID());
            if (REQUEST_BI_STREAM_SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())) {
                Channel internalChannel = getInternalChannel(call);
                ctx = ctx.withValue(CONTEXT_KEY_CHANNEL, internalChannel);
            }
            return Contexts.interceptCall(ctx, call, headers, next);
        }
    }

    
    private int getInboundMessageSize() {
        String messageSize = System
                .getProperty(GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY, String.valueOf(DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE));
        return Integer.parseInt(messageSize);
    }
    
    private Channel getInternalChannel(ServerCall serverCall) {
        ServerStream serverStream = (ServerStream) ReflectUtils.getFieldValue(serverCall, "stream");
        return (Channel) ReflectUtils.getFieldValue(serverStream, "channel");
    }

    //这里会向io.grpc.Server对象中注册服务，当有客户端连接或接收到请求时，会触发对应的服务
    //handlerRegistry: MutableHandlerRegistry
    //serverInterceptor: BaseGrpcServer.CustomServerInterceptor
    private void addServices(String className,MutableHandlerRegistry handlerRegistry, ServerInterceptor... serverInterceptor) {
        
        // unary common call register.
        final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(REQUEST_SERVICE_NAME, REQUEST_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        //服务回调（客户端注册时，这里方法会接收到）  路由处理
        final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls
                .asyncUnaryCall((request, responseObserver) -> {
                    if(!className.equals("GrpcSdkServer")){
                        System.out.println("---------------------------->request:"+className);
                    }
                    //服务回调（客户端注册时，这里方法会接收到）
                    grpcCommonRequestAcceptor.request(request, responseObserver);
                });
        //构造服务

        final ServerServiceDefinition serviceDefOfUnaryPayload = ServerServiceDefinition.builder(REQUEST_SERVICE_NAME)
                .addMethod(unaryPayloadMethod, payloadHandler).build();
        //服务注册到grpc中
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfUnaryPayload, serverInterceptor));
        
        // bi stream register.
        final ServerCallHandler<Payload, Payload> biStreamHandler = ServerCalls.asyncBidiStreamingCall(
                (responseObserver) -> {
                    if(!className.equals("GrpcSdkServer")){
                        System.out.println("---------------------------->requestBiStream:"+className);
                    }
                    return grpcBiStreamRequestAcceptor.requestBiStream(responseObserver);
                });
        //构造服务
        final MethodDescriptor<Payload, Payload> biStreamMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING).setFullMethodName(MethodDescriptor
                        .generateFullMethodName(REQUEST_BI_STREAM_SERVICE_NAME, REQUEST_BI_STREAM_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        
        final ServerServiceDefinition serviceDefOfBiStream = ServerServiceDefinition
                .builder(REQUEST_BI_STREAM_SERVICE_NAME).addMethod(biStreamMethod, biStreamHandler).build();
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfBiStream, serverInterceptor));
        
    }
    
    @Override
    public void shutdownServer() {
        if (server != null) {
            server.shutdownNow();
        }
    }
    
    /**
     * get rpc executor.
     *
     * @return executor.
     */
    public abstract ThreadPoolExecutor getRpcExecutor();
    
    static final Attributes.Key<String> TRANS_KEY_CONN_ID = Attributes.Key.create("conn_id");

    //在BaseGrpcServer#startServer中设置的
    static final Attributes.Key<String> TRANS_KEY_REMOTE_IP = Attributes.Key.create("remote_ip");

    //在BaseGrpcServer#startServer中设置的
    static final Attributes.Key<Integer> TRANS_KEY_REMOTE_PORT = Attributes.Key.create("remote_port");

    //在BaseGrpcServer#startServer中设置的
    static final Attributes.Key<Integer> TRANS_KEY_LOCAL_PORT = Attributes.Key.create("local_port");

    //在BaseGrpcServer#startServer中设置的
    static final Context.Key<String> CONTEXT_KEY_CONN_ID = Context.key("conn_id");
    
    static final Context.Key<String> CONTEXT_KEY_CONN_REMOTE_IP = Context.key("remote_ip");
    
    static final Context.Key<Integer> CONTEXT_KEY_CONN_REMOTE_PORT = Context.key("remote_port");
    
    static final Context.Key<Integer> CONTEXT_KEY_CONN_LOCAL_PORT = Context.key("local_port");
    
    static final Context.Key<Channel> CONTEXT_KEY_CHANNEL = Context.key("ctx_channel");

    static final Context.Key<String> TRAD_ID = Context.key("trad_id");
}
