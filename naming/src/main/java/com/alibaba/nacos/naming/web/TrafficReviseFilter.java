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
package com.alibaba.nacos.naming.web;

import com.alibaba.nacos.common.util.HttpMethod;
import com.alibaba.nacos.naming.cluster.ServerMode;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.ServerStatusManager;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Filter incoming traffic to refuse or revise unexpected requests
 *
 * @author nkorange
 * @since 1.0.0
 */
public class TrafficReviseFilter implements Filter {

    @Autowired
    private ServerStatusManager serverStatusManager;

    @Autowired
    private SwitchDomain switchDomain;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        // request limit if exist:
        String urlString = req.getRequestURI() + "?" + req.getQueryString();
        Map<String, Integer> limitedUrlMap = switchDomain.getLimitedUrlMap();

        if (limitedUrlMap != null && limitedUrlMap.size() > 0) {
            for (Map.Entry<String, Integer> entry : limitedUrlMap.entrySet()) {
                String limitedUrl = entry.getKey();
                if (StringUtils.startsWith(urlString, limitedUrl)) {
                    resp.setStatus(entry.getValue());
                    return;
                }
            }
        }

        // in AP mode, service and cluster cannot be edited:
        try {
            String path = new URI(req.getRequestURI()).getPath();
            if (ServerMode.AP.name().equals(switchDomain.getServerMode()) && !HttpMethod.GET.equals(req.getMethod())) {
                if (path.contains(UtilsAndCommons.NACOS_NAMING_CONTEXT + UtilsAndCommons.NACOS_NAMING_SERVICE_CONTEXT)
                    || path.contains(UtilsAndCommons.NACOS_NAMING_CONTEXT + UtilsAndCommons.NACOS_NAMING_CLUSTER_CONTEXT)) {
                    resp.getWriter().write("server in AP mode, request: " + req.getMethod() + " " + path + " not permitted");
                    resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    return;
                }
            }

        } catch (URISyntaxException e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Server parse url failed," + UtilsAndCommons.getAllExceptionMsg(e));
            return;
        }

        // if server is UP:
        if (serverStatusManager.getServerStatus() == ServerStatus.UP) {
            filterChain.doFilter(req, resp);
            return;
        }

        // requests from peer server should be let pass:
        String agent = req.getHeader("Client-Version");
        if (StringUtils.isBlank(agent)) {
            agent = req.getHeader("User-Agent");
        }

        if (StringUtils.startsWith(agent, UtilsAndCommons.NACOS_SERVER_HEADER)) {
            filterChain.doFilter(req, resp);
            return;
        }

        // write operation should be let pass in WRITE_ONLY status:
        if (serverStatusManager.getServerStatus() == ServerStatus.WRITE_ONLY && !HttpMethod.GET.equals(req.getMethod())) {
            filterChain.doFilter(req, resp);
            return;
        }

        // read operation should be let pass in READ_ONLY status:
        if (serverStatusManager.getServerStatus() == ServerStatus.READ_ONLY && HttpMethod.GET.equals(req.getMethod())) {
            filterChain.doFilter(req, resp);
            return;
        }

        resp.getWriter().write("server is " + serverStatusManager.getServerStatus().name() + " now, please try again later!");
        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }
}
