/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Aug 13, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.databricks.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.configuration.security.ProxyAuthorizationPolicy;
import org.apache.cxf.interceptor.LoggingInInterceptor;
import org.apache.cxf.interceptor.LoggingOutInterceptor;
import org.apache.cxf.jaxrs.client.ClientConfiguration;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
import org.apache.cxf.jaxrs.client.ResponseExceptionMapper;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transport.common.gzip.GZIPOutInterceptor;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.transports.http.configuration.ProxyServerType;
import org.eclipse.core.net.proxy.IProxyData;
import org.eclipse.core.net.proxy.IProxyService;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.Version;
import org.osgi.util.tracker.ServiceTracker;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

/**
 * Client to access Databricks REST API using CXF, JAX-RS and Jackson.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksRESTClient {
    private static final NodeLogger LOG = NodeLogger.getLogger(DatabricksRESTClient.class);

    /** Current API version. */
    private static final String API_VERSION = "2.0";

    /** Chunk threshold in bytes. */
    private static final int CHUNK_THRESHOLD = 10 * 1024 * 1024; // 10MB

    /** Length in bytes of each chunk. */
    private static final int CHUNK_LENGTH = 1 * 1024 * 1024; // 1MB

    private static final Version CLIENT_VERSION = FrameworkUtil.getBundle(DatabricksRESTClient.class).getVersion();
    private static final String USER_AGENT = "KNIME/" + CLIENT_VERSION;

    private static final ServiceTracker<IProxyService, IProxyService> PROXY_TRACKER;
    static {
        PROXY_TRACKER = new ServiceTracker<>(FrameworkUtil.getBundle(DatabricksRESTClient.class).getBundleContext(),
            IProxyService.class, null);
        PROXY_TRACKER.open();
    }

    private DatabricksRESTClient() {}

    /**
     * Map response HTTP status codes to {@link IOException}s with error message from JSON response if possible.
     */
    static class DatabricksResponseExceptionMapper implements ResponseExceptionMapper<IOException> {
        @Override
        public IOException fromResponse(final Response response) {
            String message = "";

            // try to parse JSON response with error message
            if (response.getMediaType().getSubtype().toLowerCase().contains("json")) {
                try {
                    message = response.readEntity(GenericErrorResponse.class).message;
                } catch (Exception e) {
                    message = e.getMessage();
                }
            }

            if (response.getStatus() == 403 && !StringUtils.isBlank(message)) {
                return new AccessDeniedException(message);
            } else if (response.getStatus() == 403) {
                return new AccessDeniedException("Invalid or missing authentication data");
            } else if (response.getStatus() == 404 && !StringUtils.isBlank(message)) {
                return new FileNotFoundException(message);
            } else if (response.getStatus() == 404) {
                return new FileNotFoundException("Resource not found");
            } else if (!StringUtils.isBlank(message)) {
                return new IOException("Server error: " + message);
            } else {
                return new IOException("Server error: " + response.getStatus());
            }
        }
    }

    /**
     * Creates a service proxy for given Databricks REST API interface without any authentication data. Use
     * {@link DatabricksRESTClient#create(String, Class, String, int, int)} or
     * {@link DatabricksRESTClient#create(String, Class, String, String, int, int)} instead.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param receiveTimeoutMillis Receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return Client implementation for given proxy interface
     */
    private static <T> T create(final String deploymentUrl, final Class<T> proxy,
            final int receiveTimeoutMillis, final int connectionTimeoutMillis) {

        final String baseUrl = deploymentUrl + "/api/" + API_VERSION;

        // Chunk transfer policy and timeout
        final HTTPClientPolicy clientPolicy = new HTTPClientPolicy();
        clientPolicy.setAllowChunking(true);
        clientPolicy.setChunkingThreshold(CHUNK_THRESHOLD);
        clientPolicy.setChunkLength(CHUNK_LENGTH);
        clientPolicy.setReceiveTimeout(receiveTimeoutMillis);
        clientPolicy.setConnectionTimeout(connectionTimeoutMillis);

        // HTTP Proxy configuration
        final ProxyAuthorizationPolicy proxyAuthPolicy = configureProxyIfNecessary(baseUrl, clientPolicy);

        // Create the API Proxy
        final List<Object> provider = Arrays.asList(new JacksonJsonProvider(), new DatabricksResponseExceptionMapper());
        final T proxyImpl = JAXRSClientFactory.create(baseUrl, proxy, provider);
        WebClient.client(proxyImpl)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .header("User-Agent", USER_AGENT);
        final ClientConfiguration config = WebClient.getConfig(proxyImpl);
        config.getInInterceptors().add(new GZIPInInterceptor());
        config.getOutInterceptors().add(new GZIPOutInterceptor());
        config.getHttpConduit().setClient(clientPolicy);
        if (proxyAuthPolicy != null) {
            config.getHttpConduit().setProxyAuthorization(proxyAuthPolicy);
        }
        if (LOG.isDebugEnabled()) {
            config.getInInterceptors().add(new LoggingInInterceptor());
            config.getOutInterceptors().add(new LoggingOutInterceptor());
        }

        return proxyImpl;
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using a bearer authentication token.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param token Authentication token
     * @param receiveTimeoutMillis Receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return Client implementation for given proxy interface
     */
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String token,
        final int receiveTimeoutMillis, final int connectionTimeoutMillis) {

        final T proxyImpl = create(deploymentUrl, proxy, receiveTimeoutMillis, connectionTimeoutMillis);
        WebClient.client(proxyImpl).header("Authorization", "Bearer " + token);
        return proxyImpl;
    }

    /**
     * Creates a service proxy for given Databricks REST API interface using basic authentication with user and
     * password.
     *
     * @param deploymentUrl https://...cloud.databricks.com
     * @param proxy Interface to create proxy for
     * @param user Username for authentication
     * @param password Password for authentication
     * @param receiveTimeoutMillis Receive timeout in milliseconds
     * @param connectionTimeoutMillis connection timeout in milliseconds
     * @return Client implementation for given proxy interface
     * @throws UnsupportedEncodingException if given user and password can't be encoded as UTF-8
     */
    public static <T> T create(final String deploymentUrl, final Class<T> proxy, final String user, final String password,
        final int receiveTimeoutMillis, final int connectionTimeoutMillis) throws UnsupportedEncodingException {

        final T proxyImpl = create(deploymentUrl, proxy, receiveTimeoutMillis, connectionTimeoutMillis);
        WebClient.client(proxyImpl).header("Authorization", "Basic " + Base64Utility.encode((user + ":" + password).getBytes("UTF-8")));
        return proxyImpl;
    }

    /**
     * Configures HTTP proxy on given client policy and returns proxy authorization policy if required on
     * <code>null</code>.
     *
     * @param url Base URL to configure proxy for
     * @param clientPolicy Policy to apply proxy configuration to
     * @return {@link ProxyAuthorizationPolicy} or <code>null</code> if no proxy is used or nor proxy authentication is
     *         required
     */
    private static ProxyAuthorizationPolicy configureProxyIfNecessary(final String url,
            final HTTPClientPolicy clientPolicy) {

        final URI uri = URI.create(url);
        ProxyAuthorizationPolicy proxyAuthPolicy = null;

        IProxyService proxyService = PROXY_TRACKER.getService();
        if (proxyService == null) {
            LOG.error("No Proxy service registered in Eclipse framework. Not using any proxies for databricks connection.");
            return null;
        }

        for (IProxyData proxy : proxyService.select(uri)) {

            final ProxyServerType proxyType;
            final int defaultPort;

            switch (proxy.getType()) {
                case IProxyData.HTTP_PROXY_TYPE:
                    proxyType = ProxyServerType.HTTP;
                    defaultPort = 80;
                    break;
                case IProxyData.HTTPS_PROXY_TYPE:
                    proxyType = ProxyServerType.HTTP;
                    defaultPort = 443;
                    break;
                case IProxyData.SOCKS_PROXY_TYPE:
                    proxyType = ProxyServerType.SOCKS;
                    defaultPort = 1080;
                    break;
                default:
                    throw new UnsupportedOperationException(String.format(
                        "Unsupported proxy type: %s. Please remove the proxy setting from File > Preferences > General > Network Connections.",
                        proxy.getType()));
            }

            if (proxy.getHost() != null) {
                clientPolicy.setProxyServerType(proxyType);
                clientPolicy.setProxyServer(proxy.getHost());

                clientPolicy.setProxyServerPort(defaultPort);
                if (proxy.getPort() != -1) {
                    clientPolicy.setProxyServerPort(proxy.getPort());
                }

                if (proxy.isRequiresAuthentication() && proxy.getUserId() != null && proxy.getPassword() != null) {
                    proxyAuthPolicy = new ProxyAuthorizationPolicy();
                    proxyAuthPolicy.setUserName(proxy.getUserId());
                    proxyAuthPolicy.setPassword(proxy.getPassword());
                }

                LOG.debug(String.format(
                    "Using proxy for REST connection to Databricks: %s:%d (type: %s, proxyAuthentication: %b)",
                    clientPolicy.getProxyServer(), clientPolicy.getProxyServerPort(), proxy.getType(),
                    proxyAuthPolicy != null));

                break;
            }
        }

        return proxyAuthPolicy;
    }

    /**
     * Release the internal state and configuration associated with this service proxy.
     *
     * @param proxy Client proxy implementation
     */
    public static <T> void close(final T proxy) {
        WebClient.client(proxy).close();
    }
}
