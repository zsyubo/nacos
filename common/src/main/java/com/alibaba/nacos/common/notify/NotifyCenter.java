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

package com.alibaba.nacos.common.notify;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import com.alibaba.nacos.common.utils.ClassUtils;
import com.alibaba.nacos.common.utils.MapUtil;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.nacos.api.exception.NacosException.SERVER_ERROR;

/**
 * 时间通知的：分为普通事件和慢事件
 *
 * 单例模式
 *
 * Unified Event Notify Center.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class NotifyCenter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    
    public static int ringBufferSize;
    
    public static int shareBufferSize;
    
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    // 匿名内部类(DefaultPublisher), 在static静态块汇总
    private static final EventPublisherFactory DEFAULT_PUBLISHER_FACTORY;
    
    private static final NotifyCenter INSTANCE = new NotifyCenter();

    // 发布慢事件 SlowEvent， 在static静态块中初始化, 其实就是一个消费者：
    //              com.alibaba.nacos.common.notify.DefaultPublisher.run---->DefaultPublisher#openEventHandler
    //               [NamingGrpcClientProxy]
    private DefaultSharePublisher sharePublisher;

    // 普通时间处理
    // 如果没有自定义，那么就是DefaultPublisher
    private static Class<? extends EventPublisher> clazz;
    
    /**
     * Publisher management container.
     */
    // 每一个DefaultPublisher都起了一个线程
    //  key:InstancesChangeEvent  value:DefaultPublisher
    //
    private final Map<String, EventPublisher> publisherMap = new ConcurrentHashMap<>(16);
    
    static {
        // Internal ArrayBlockingQueue buffer size. For applications with high write throughput,
        // this value needs to be increased appropriately. default value is 16384
        // 内部ArrayBlockingQueue的缓冲区大小。对于具有高写入量的应用，这个值需要适当增加。默认值是16384
        String ringBufferSizeProperty = "nacos.core.notify.ring-buffer-size";
        ringBufferSize = Integer.getInteger(ringBufferSizeProperty, 16384);
        
        // The size of the public publisher's message staging queue buffer
        // 公共发布者的消息暂存队列缓冲区的大小
        String shareBufferSizeProperty = "nacos.core.notify.share-buffer-size";
        shareBufferSize = Integer.getInteger(shareBufferSizeProperty, 1024);

        // 这地方是提供给第三方去实现的，在默认情况下，这地方是空的
        final Collection<EventPublisher> publishers = NacosServiceLoader.load(EventPublisher.class);
        Iterator<EventPublisher> iterator = publishers.iterator();
        // 多个取第一个
        if (iterator.hasNext()) {
            clazz = iterator.next().getClass();
        } else {
            clazz = DefaultPublisher.class;
        }
        
        DEFAULT_PUBLISHER_FACTORY = (cls, buffer) -> {
            try {
                //clazz: DefaultPublisher
                //cls: com.alibaba.nacos.client.naming.event.InstancesChangeEvent
                //buffer: 16384
                EventPublisher publisher = clazz.newInstance();
                publisher.init(cls, buffer);
                return publisher;
            } catch (Throwable ex) {
                LOGGER.error("Service class newInstance has error : ", ex);
                throw new NacosRuntimeException(SERVER_ERROR, ex);
            }
        };
        
        try {
            
            // Create and init DefaultSharePublisher instance.
            INSTANCE.sharePublisher = new DefaultSharePublisher();
            INSTANCE.sharePublisher.init(SlowEvent.class, shareBufferSize);
            
        } catch (Throwable ex) {
            LOGGER.error("Service class newInstance has error : ", ex);
        }
        
        ThreadUtils.addShutdownHook(NotifyCenter::shutdown);
    }
    
    @JustForTest
    public static Map<String, EventPublisher> getPublisherMap() {
        return INSTANCE.publisherMap;
    }
    
    @JustForTest
    public static EventPublisher getPublisher(Class<? extends Event> topic) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, topic)) {
            return INSTANCE.sharePublisher;
        }
        return INSTANCE.publisherMap.get(topic.getCanonicalName());
    }
    
    @JustForTest
    public static EventPublisher getSharePublisher() {
        return INSTANCE.sharePublisher;
    }
    
    /**
     * Shutdown the several publisher instance which notify center has.
     */
    public static void shutdown() {
        if (!CLOSED.compareAndSet(false, true)) {
            return;
        }
        LOGGER.warn("[NotifyCenter] Start destroying Publisher");
        
        for (Map.Entry<String, EventPublisher> entry : INSTANCE.publisherMap.entrySet()) {
            try {
                EventPublisher eventPublisher = entry.getValue();
                eventPublisher.shutdown();
            } catch (Throwable e) {
                LOGGER.error("[EventPublisher] shutdown has error : ", e);
            }
        }
        
        try {
            INSTANCE.sharePublisher.shutdown();
        } catch (Throwable e) {
            LOGGER.error("[SharePublisher] shutdown has error : ", e);
        }
        
        LOGGER.warn("[NotifyCenter] Destruction of the end");
    }
    
    /**
     * Register a Subscriber. If the Publisher concerned by the Subscriber does not exist, then PublihserMap will
     * preempt a placeholder Publisher with default EventPublisherFactory first.
     *
     * @param consumer subscriber
     */
    //NacosNamingService.init
    //    consumer: InstancesChangeNotifier
    //NamingGrpcClientProxy#start()
    //    consumer: NamingGrpcClientProxy
    public static void registerSubscriber(final Subscriber consumer) {
        registerSubscriber(consumer, DEFAULT_PUBLISHER_FACTORY);
    }
    
    /**
     * Register a Subscriber. If the Publisher concerned by the Subscriber does not exist, then PublihserMap will
     * preempt a placeholder Publisher with specified EventPublisherFactory first.
     *
     * @param consumer subscriber
     * @param factory  publisher factory.
     */
    //NotifyCenter.registerSubscriber(com.alibaba.nacos.common.notify.listener.Subscriber)
    //    consumer:
    //          InstancesChangeNotifier
    //          NamingGrpcClientProxy
    //    factory:NotifyCenter.DEFAULT_PUBLISHER_FACTORY
    public static void registerSubscriber(final Subscriber consumer, final EventPublisherFactory factory) {
        // If you want to listen to multiple events, you do it separately,
        // based on subclass's subscribeTypes method return list, it can register to publisher.
//订阅多个事件
        //如果你想监听多个事件，你就分开做，基于子类的subscribeTypes方法的返回列表，它可以注册到发布者。
        if (consumer instanceof SmartSubscriber) {
            // 其实就是订阅多个事件
            for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
                // For case, producer: defaultSharePublisher -> consumer: smartSubscriber.
                if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                    INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
                } else {
                    // For case, producer: defaultPublisher -> consumer: subscriber.
                    addSubscriber(consumer, subscribeType, factory);
                }
            }
            return;
        }

//订阅单个事件
        // InstancesChangeNotifier---->InstancesChangeEvent.class
        final Class<? extends Event> subscribeType = consumer.subscribeType();
        // 如果是慢事件，特殊处理，注册到INSTANCE.sharePublisher中
        if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
            INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
            return;
        }
        
        addSubscriber(consumer, subscribeType, factory);
    }

    /**
     * Add a subscriber to publisher.
     *
     * @param consumer      subscriber instance.
     * @param subscribeType subscribeType.
     * @param factory       publisher factory.  NotifyCenter.DEFAULT_PUBLISHER_FACTORY
     */
    private static void addSubscriber(final Subscriber consumer, Class<? extends Event> subscribeType,
            EventPublisherFactory factory) {
        //registerSubscriber(com.alibaba.nacos.common.notify.listener.Subscriber, com.alibaba.nacos.common.notify.EventPublisherFactory)
        //  consumer:  InstancesChangeNotifier
        //  subscribeType: InstancesChangeEvent.class
        //  factory: NotifyCenter.DEFAULT_PUBLISHER_FACTORY
        if(consumer != null && consumer.subscribeType() != null ){
            String config = System.getProperty("println.addSubscriber");
            if(StrUtil.isNotBlank(config) && StrUtil.equals(config, "true") ){
                System.out.println(consumer.getClass().getSimpleName()+"-->"+consumer.subscribeType().getSimpleName());
            }
        }
        String config = System.getProperty("println.subscribeType");
        if(StrUtil.isNotBlank(config) && StrUtil.equals(config, "true") ){
            System.out.println("subscribeType:"+subscribeType.getSimpleName());
        }
        final String topic = ClassUtils.getCanonicalName(subscribeType);
        synchronized (NotifyCenter.class) {
            // MapUtils.computeIfAbsent is a unsafe method.
            // 一般到这里，INSTANCE.publisherMap中已存在InstancesChangeEvent，所以这个操作没用
            MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, subscribeType, ringBufferSize);
        }
        EventPublisher publisher = INSTANCE.publisherMap.get(topic);
        if (publisher instanceof ShardedEventPublisher) {
            ((ShardedEventPublisher) publisher).addSubscriber(consumer, subscribeType);
        } else {
            publisher.addSubscriber(consumer);
        }
    }
    
    /**
     * Deregister subscriber.
     *
     * @param consumer subscriber instance.
     */
    public static void deregisterSubscriber(final Subscriber consumer) {
        if (consumer instanceof SmartSubscriber) {
            for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
                if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                    INSTANCE.sharePublisher.removeSubscriber(consumer, subscribeType);
                } else {
                    removeSubscriber(consumer, subscribeType);
                }
            }
            return;
        }
        
        final Class<? extends Event> subscribeType = consumer.subscribeType();
        if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
            INSTANCE.sharePublisher.removeSubscriber(consumer, subscribeType);
            return;
        }
        
        if (removeSubscriber(consumer, subscribeType)) {
            return;
        }
        throw new NoSuchElementException("The subscriber has no event publisher");
    }
    
    /**
     * Remove subscriber.
     *
     * @param consumer      subscriber instance.
     * @param subscribeType subscribeType.
     * @return whether remove subscriber successfully or not.
     */
    private static boolean removeSubscriber(final Subscriber consumer, Class<? extends Event> subscribeType) {
        
        final String topic = ClassUtils.getCanonicalName(subscribeType);
        EventPublisher eventPublisher = INSTANCE.publisherMap.get(topic);
        if (null == eventPublisher) {
            return false;
        }
        if (eventPublisher instanceof ShardedEventPublisher) {
            ((ShardedEventPublisher) eventPublisher).removeSubscriber(consumer, subscribeType);
        } else {
            eventPublisher.removeSubscriber(consumer);
        }
        return true;
    }
    
    /**
     * Request publisher publish event Publishers load lazily, calling publisher. Start () only when the event is
     * actually published.
     *
     * @param event class Instances of the event.
     */
    public static boolean publishEvent(final Event event) {
        try {
            return publishEvent(event.getClass(), event);
        } catch (Throwable ex) {
            LOGGER.error("There was an exception to the message publishing : ", ex);
            return false;
        }
    }
    
    /**
     * Request publisher publish event Publishers load lazily, calling publisher.
     *
     * @param eventType class Instances type of the event type.
     * @param event     event instance.
     */
    private static boolean publishEvent(final Class<? extends Event> eventType, final Event event) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
            return INSTANCE.sharePublisher.publish(event);
        }
        
        final String topic = ClassUtils.getCanonicalName(eventType);
        //server: ["com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent","com.alibaba.nacos.naming.core.v2.event.client.ClientEvent.ClientChangedEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent.ClientRegisterServiceEvent","com.alibaba.nacos.naming.consistency.ValueChangeEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent.ClientDeregisterServiceEvent","com.alibaba.nacos.naming.core.v2.event.client.ClientEvent.ClientVerifyFailedEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent.ClientSubscribeServiceEvent","com.alibaba.nacos.core.cluster.MembersChangeEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent.ServiceChangedEvent","com.alibaba.nacos.core.remote.control.TpsControlRuleChangeEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent.ClientUnsubscribeServiceEvent","com.alibaba.nacos.core.remote.event.ConnectionLimitRuleChangeEvent"
        // ,"com.alibaba.nacos.config.server.model.event.LocalDataChangeEvent","com.alibaba.nacos.naming.core.v2.event.client.ClientEvent.ClientDisconnectEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent.ServiceSubscribedEvent","com.alibaba.nacos.naming.core.v2.upgrade.UpgradeStates.UpgradeStateChangedEvent"
        // ,"com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent.InstanceMetadataEvent","com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent.ServiceMetadataEvent"
        // ,"com.alibaba.nacos.common.event.ServerConfigChangeEvent"]

        String config = System.getProperty("println.eventLog");
        if(StrUtil.isNotBlank(config) && StrUtil.equals(config, "true") ){
            System.out.println("publishEvent:"+ JSONUtil.toJsonStr(INSTANCE.publisherMap.keySet()));
        }
        EventPublisher publisher = INSTANCE.publisherMap.get(topic);
        if (publisher != null) {
            return publisher.publish(event);
        }
        LOGGER.warn("There are no [{}] publishers for this event, please register", topic);
        return false;
    }
    
    /**
     * Register to share-publisher.
     *
     * @param eventType class Instances type of the event type.
     * @return share publisher instance.
     */
    public static EventPublisher registerToSharePublisher(final Class<? extends SlowEvent> eventType) {
        return INSTANCE.sharePublisher;
    }
    
    /**
     * Register publisher with default factory.
     *
     * @param eventType    class Instances type of the event type.
     * @param queueMaxSize the publisher's queue max size.
     */
    //NacosNamingService#init
    //    eventType:InstancesChangeEvent
    //    queueMaxSize:16384
    public static EventPublisher registerToPublisher(final Class<? extends Event> eventType, final int queueMaxSize) {
        //    eventType:InstancesChangeEvent
        //    queueMaxSize:16384
        return registerToPublisher(eventType, DEFAULT_PUBLISHER_FACTORY, queueMaxSize);
    }
    
    /**
     * Register publisher with specified factory.
     *
     * @param eventType    class Instances type of the event type.
     * @param factory      publisher factory.
     * @param queueMaxSize the publisher's queue max size.
     */
    //NotifyCenter.registerToPublisher()
    //    eventType: InstancesChangeEvent
    //    factory: NotifyCenter.DEFAULT_PUBLISHER_FACTORY
    //    queueMaxSize:16384
    public static EventPublisher registerToPublisher(final Class<? extends Event> eventType,
            final EventPublisherFactory factory, final int queueMaxSize) {
        // 判断event是不是慢事件，如果是SlowEvent，那么sharePublisher在静态块汇总就已经初始化了
        if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
            return INSTANCE.sharePublisher;
        }
        //获取完整的ClassName: com.alibaba.nacos.client.naming.event.InstancesChangeEvent
        final String topic = ClassUtils.getCanonicalName(eventType);
        String config = System.getProperty("println.registerToPublisher");
        if(StrUtil.isNotBlank(config) && StrUtil.equals(config, "true") ) {
            System.out.println("registerToPublisher::" + eventType.getSimpleName());
        }
        synchronized (NotifyCenter.class) {
            // MapUtils.computeIfAbsent is a unsafe method.
            // Map<K, V> target, key, mappingFunction, C param1,  T param2
            // target是一个Map, 通过Key获取对应value，如果没有，那么就把 param1和param2 拿去调用factory#apply方法获得一个value，设置到map。
            MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, eventType, queueMaxSize);
        }
        return INSTANCE.publisherMap.get(topic);
    }
    
    /**
     * Register publisher.
     *
     * @param eventType class Instances type of the event type.
     * @param publisher the specified event publisher
     */
    public static void registerToPublisher(final Class<? extends Event> eventType, final EventPublisher publisher) {
        if (null == publisher) {
            return;
        }
        final String topic = ClassUtils.getCanonicalName(eventType);
        synchronized (NotifyCenter.class) {
            INSTANCE.publisherMap.putIfAbsent(topic, publisher);
        }
    }
    
    /**
     * Deregister publisher.
     *
     * @param eventType class Instances type of the event type.
     */
    public static void deregisterPublisher(final Class<? extends Event> eventType) {
        final String topic = ClassUtils.getCanonicalName(eventType);
        EventPublisher publisher = INSTANCE.publisherMap.remove(topic);
        try {
            publisher.shutdown();
        } catch (Throwable ex) {
            LOGGER.error("There was an exception when publisher shutdown : ", ex);
        }
    }
    
}
