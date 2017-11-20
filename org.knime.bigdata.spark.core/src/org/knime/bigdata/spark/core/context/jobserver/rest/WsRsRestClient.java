package org.knime.bigdata.spark.core.context.jobserver.rest;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HostnameVerifier;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.configuration.security.ProxyAuthorizationPolicy;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transport.http.auth.HttpAuthSupplier;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.transports.http.configuration.ProxyServerType;
import org.eclipse.core.net.proxy.IProxyData;
import org.eclipse.core.net.proxy.IProxyService;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;
import org.osgi.framework.FrameworkUtil;
import org.osgi.util.tracker.ServiceTracker;

/**
 * creates and handles REST requests
 *
 * @author Bjoern Lohrmann, KNIME.com Gmbh
 * @author dwk
 *
 */
class WsRsRestClient implements IRestClient {

    private final static NodeLogger LOG = NodeLogger.getLogger(WsRsRestClient.class);

    /** Chunk threshold in bytes. */
    private static final int CHUNK_THRESHOLD = 10 * 1024 * 1024; // 10MB

    /** Length in bytes of each chunk. */
    private static final int CHUNK_LENGTH = 1 * 1024 * 1024; // 1MB

    private static HostnameVerifier getHostnameVerifier() {
        return new HostnameVerifier() {
            @Override
            public boolean verify(final String hostname, final javax.net.ssl.SSLSession sslSession) {
                return true;
            }
        };
    }

    private final static ServiceTracker<IProxyService, IProxyService> PROXY_TRACKER;
    static {
        PROXY_TRACKER = new ServiceTracker<>(FrameworkUtil.getBundle(WsRsRestClient.class).getBundleContext(),
            IProxyService.class, null);
        PROXY_TRACKER.open();
    }


    public final Client m_client;

    public final WebTarget m_baseTarget;

    private final HTTPClientPolicy m_clientPolicy;

    private final HttpAuthSupplier m_clientAuthSupplier;

    private ProxyAuthorizationPolicy m_proxyAuthPolicy;

    public WsRsRestClient(final SparkContextConfig contextConfig) throws KeyManagementException,
        NoSuchAlgorithmException, URISyntaxException, UnsupportedEncodingException, UnsupportedOperationException {
        // The JAX-RS interface is in a different plug-in than the CXF implementation. Therefore the interface classes
        // won't find the implementation via the default ContextFinder classloader. We set the current classes's
        // classloader as context classloader and then it will find the service definition from this plug-in.
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            m_client = ClientBuilder.newBuilder().sslContext(SSLProvider.setupSSLContext())
                .hostnameVerifier(getHostnameVerifier()).build();
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }

        if (contextConfig.useAuthentication() && (contextConfig.getUser() != null)
            && (contextConfig.getPassword() != null)) {
            m_client.property("Authorization", "Basic " + Base64Utility
                .encode((contextConfig.getUser() + ":" + contextConfig.getPassword()).getBytes("UTF-8")));
            m_clientAuthSupplier = new WsRsRestClientAuthSupplier(contextConfig.getUser(), contextConfig.getPassword());
        } else {
            m_clientAuthSupplier = null;
        }

        m_baseTarget = m_client.target(new URI(contextConfig.getJobServerUrl()));

        // Chunk transfer policy
        m_clientPolicy = new HTTPClientPolicy();
        m_clientPolicy.setAllowChunking(true);
        m_clientPolicy.setChunkingThreshold(CHUNK_THRESHOLD);
        m_clientPolicy.setChunkLength(CHUNK_LENGTH);

        configureProxyIfNecessary(contextConfig);
    }

    private void configureProxyIfNecessary(final SparkContextConfig contextConfig) {

        final URI jobserverURI = URI.create(contextConfig.getJobServerUrl());

        IProxyService proxyService = PROXY_TRACKER.getService();
        if (proxyService == null) {
            LOG.error("No Proxy service registered in Eclipse framework. Not using any proxies for Spark jobserver connection.");
            return;
        }


        for (IProxyData proxy : proxyService.select(jobserverURI)) {

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
                m_clientPolicy.setProxyServerType(proxyType);
                m_clientPolicy.setProxyServer(proxy.getHost());

                m_clientPolicy.setProxyServerPort(defaultPort);
                if (proxy.getPort() != -1) {
                    m_clientPolicy.setProxyServerPort(proxy.getPort());
                }

                if (proxy.isRequiresAuthentication() && proxy.getUserId() != null && proxy.getPassword() != null) {
                    m_proxyAuthPolicy = new ProxyAuthorizationPolicy();
                    m_proxyAuthPolicy.setUserName(proxy.getUserId());
                    m_proxyAuthPolicy.setPassword(proxy.getPassword());
                }

                LOG.debug(String.format(
                    "Using proxy for REST connection to Spark Jobserver: %s:%d (type: %s, proxyAuthentication: %b)",
                    m_clientPolicy.getProxyServer(), m_clientPolicy.getProxyServerPort(), proxy.getType(),
                    m_proxyAuthPolicy != null));

                break;
            }
        }
    }

    /**
     * create the invocation builder for this REST client
     *
     * @param aPath invocation path
     * @param aParams optional parameters
     * @return builder
     */
    private Invocation.Builder getInvocationBuilder(final String aPath, final String[] aParams) {

        WebTarget target = m_baseTarget.path(aPath);
        if (aParams != null) {
            for (int p = 0; p < aParams.length; p = p + 2) {
                target = target.queryParam(aParams[p], aParams[p + 1]);
            }
        }

        Invocation.Builder builder = target.request();
        HTTPConduit conduit = org.apache.cxf.jaxrs.client.WebClient.getConfig(builder).getHttpConduit();
        conduit.setClient(m_clientPolicy);

        if (m_proxyAuthPolicy != null) {
            conduit.setProxyAuthorization(m_proxyAuthPolicy);
        }

        if (m_clientAuthSupplier != null) {
            conduit.setAuthSupplier(m_clientAuthSupplier);
        }

        return builder;
    }

    @Override
    public <T> Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity) {
        try {
            // suppresses authentication popups
            try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
                final Invocation.Builder builder = getInvocationBuilder(aPath, aArgs);
                return builder.post(aEntity);
            }
        } catch (IOException e) {
            throw new ProcessingException(e);
        }
    }

    @Override
    public Response delete(final String aPath) {
        try {
            // suppresses authentication popups
            try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
                final Invocation.Builder builder = getInvocationBuilder(aPath, null);
                return builder.buildDelete().invoke();
            }
        } catch (IOException e) {
            throw new ProcessingException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Response get(final String aPath) {
        try {
            // suppresses authentication popups
            try (Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
                final Invocation.Builder builder = getInvocationBuilder(aPath, null);
                return builder.get();
            }
        } catch (IOException e) {
            throw new ProcessingException(e);
        }
    }
}
