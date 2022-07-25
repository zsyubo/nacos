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

package com.alibaba.nacos.client.naming.backups;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.cache.ConcurrentDiskUtil;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 故障转移反应器。
 * Failover reactor.
 *
 * @author nkorange
 */
public class FailoverReactor implements Closeable {

    //FailoverReactor中初始化会延迟10秒后做一次备份，如果发现缓存的/failover目录下有文件，就会写入本地。
    private static final String FAILOVER_DIR = "/failover";
    
    private static final String IS_FAILOVER_MODE = "1";
    
    private static final String NO_FAILOVER_MODE = "0";
    
    private static final String FAILOVER_MODE_PARAM = "failover-mode";
    
    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<String, ServiceInfo>();
    
    private final Map<String, String> switchParams = new ConcurrentHashMap<String, String>();
    
    private static final long DAY_PERIOD_MINUTES = 24 * 60;
    
    private final String failoverDir;

    // 相互持有，没撒特殊含义，只是为了调用代码时方便一点。
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final ScheduledExecutorService executorService;

    //ServiceInfoHolder#ServiceInfoHolder
    //    serviceInfoHolder: ServiceInfoHolder
    //    cacheDir: 缓存存放目录
    public FailoverReactor(ServiceInfoHolder serviceInfoHolder, String cacheDir) {
        this.serviceInfoHolder = serviceInfoHolder;
        // 故障转移时的文件夹
        this.failoverDir = cacheDir + FAILOVER_DIR;
        // 定时任务
        // init executorService
        this.executorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.naming.failover");
                return thread;
            }
        });
        this.init();
    }
    
    /**
     * Init.
     */
    //FailoverReactor#FailoverReactor()
    public void init() {
        // 5秒执行一次
        executorService.scheduleWithFixedDelay(new SwitchRefresher(), 0L, 5000L, TimeUnit.MILLISECONDS);
        //首次延迟30分钟，一天执行一次，备份到磁盘
        executorService.scheduleWithFixedDelay(new DiskFileWriter(), 30, DAY_PERIOD_MINUTES, TimeUnit.MINUTES);

        // 延时10秒执行一次  去检查磁盘缓存是否还存在(一次性)
        // backup file on startup if failover directory is empty.
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    File cacheDir = new File(failoverDir);
                    //如果缓存目录不存在
                    if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                        throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                    }
                    
                    File[] files = cacheDir.listFiles();
                    // 如果缓存文件不存在，则去手动执行缓存
                    if (files == null || files.length <= 0) {
                        new DiskFileWriter().run();
                    }
                } catch (Throwable e) {
                    NAMING_LOGGER.error("[NA] failed to backup file on startup.", e);
                }
                
            }
        }, 10000L, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Add day.
     *
     * @param date start time
     * @param num  add day number
     * @return new date
     */
    public Date addDay(Date date, int num) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    //调用者：FailoverReactor#init
    class SwitchRefresher implements Runnable {
        // 最后更新时间
        long lastModifiedMillis = 0L;
        
        @Override
        public void run() {
            try {
                // 故障切换文件
                File switchFile = new File(failoverDir + UtilAndComs.FAILOVER_SWITCH);
                if (!switchFile.exists()) {
                    switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    NAMING_LOGGER.debug("failover switch is not found, " + switchFile.getName());
                    return;
                }
                // 文件的最后修改时间
                long modified = switchFile.lastModified();

                // 如果文件的修改时间>内存中的时间，这种只有初始化的时候才有可能吧
                if (lastModifiedMillis < modified) {
                    lastModifiedMillis = modified;
                    // 获取文件中内容
                    String failover = ConcurrentDiskUtil.getFileContent(failoverDir + UtilAndComs.FAILOVER_SWITCH,
                            Charset.defaultCharset().toString());
                    // 内容不为空
                    if (!StringUtils.isEmpty(failover)) {
                        // 根据换行符进行切割
                        String[] lines = failover.split(DiskCache.getLineSeparator());
                        
                        for (String line : lines) {
                            String line1 = line.trim();
                            if (IS_FAILOVER_MODE.equals(line1)) {
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.TRUE.toString());
                                NAMING_LOGGER.info("failover-mode is on");
                                new FailoverFileReader().run();
                            } else if (NO_FAILOVER_MODE.equals(line1)) {
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                                NAMING_LOGGER.info("failover-mode is off");
                            }
                        }
                    } else {
                        switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    }
                }
                
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to read failover switch.", e);
            }
        }
    }

    /**
     * 备份回复
     */
    class FailoverFileReader implements Runnable {
        
        @Override
        public void run() {
            Map<String, ServiceInfo> domMap = new HashMap<String, ServiceInfo>(16);
            
            BufferedReader reader = null;
            try {
                // 缓存文件
                File cacheDir = new File(failoverDir);
                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }
                
                File[] files = cacheDir.listFiles();
                if (files == null) {
                    return;
                }
                
                for (File file : files) {
                    if (!file.isFile()) {
                        continue;
                    }
                    
                    if (file.getName().equals(UtilAndComs.FAILOVER_SWITCH)) {
                        continue;
                    }
                    
                    ServiceInfo dom = new ServiceInfo(file.getName());
                    
                    try {
                        String dataString = ConcurrentDiskUtil
                                .getFileContent(file, Charset.defaultCharset().toString());
                        reader = new BufferedReader(new StringReader(dataString));
                        
                        String json;
                        if ((json = reader.readLine()) != null) {
                            try {
                                dom = JacksonUtils.toObj(json, ServiceInfo.class);
                            } catch (Exception e) {
                                NAMING_LOGGER.error("[NA] error while parsing cached dom : " + json, e);
                            }
                        }
                        
                    } catch (Exception e) {
                        NAMING_LOGGER.error("[NA] failed to read cache for dom: " + file.getName(), e);
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                        } catch (Exception e) {
                            //ignore
                        }
                    }
                    if (!CollectionUtils.isEmpty(dom.getHosts())) {
                        domMap.put(dom.getKey(), dom);
                    }
                }
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] failed to read cache file", e);
            }
            
            if (domMap.size() > 0) {
                serviceMap = domMap;
            }
        }
    }
    
    class DiskFileWriter extends TimerTask {
        
        @Override
        public void run() {
            Map<String, ServiceInfo> map = serviceInfoHolder.getServiceInfoMap();
            for (Map.Entry<String, ServiceInfo> entry : map.entrySet()) {
                ServiceInfo serviceInfo = entry.getValue();
                if (StringUtils.equals(serviceInfo.getKey(), UtilAndComs.ALL_IPS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_LIST_KEY) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_CONFIGS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.VIP_CLIENT_FILE) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ALL_HOSTS)) {
                    continue;
                }
                
                DiskCache.write(serviceInfo, failoverDir);
            }
        }
    }

    /**
     * 故障转移
     * @return
     */
    public boolean isFailoverSwitch() {
        return Boolean.parseBoolean(switchParams.get(FAILOVER_MODE_PARAM));
    }
    
    public ServiceInfo getService(String key) {
        ServiceInfo serviceInfo = serviceMap.get(key);
        
        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
            serviceInfo.setName(key);
        }
        
        return serviceInfo;
    }
}
