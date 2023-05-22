package org.knime.bigdata.spark.core.sparkjobserver.rest;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

import javax.net.ssl.HostnameVerifier;

import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.configuration.security.ProxyAuthorizationPolicy;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transport.http.auth.HttpAuthSupplier;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.knime.bigdata.commons.rest.AbstractRESTClient;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;

/**
 * creates and handles REST requests
 *
 * @author Bjoern Lohrmann, KNIME.com Gmbh
 * @author dwk
 *
 */
class WsRsRestClient extends AbstractRESTClient implements IRestClient {

    private static HostnameVerifier getHostnameVerifier() {
        return new HostnameVerifier() {
            @Override
            public boolean verify(final String hostname, final javax.net.ssl.SSLSession sslSession) {
                return true;
            }
        };
    }

    public final Client m_client;

    public final WebTarget m_baseTarget;

    private final HTTPClientPolicy m_clientPolicy;

    private final HttpAuthSupplier m_clientAuthSupplier;

    private ProxyAuthorizationPolicy m_proxyAuthPolicy;

    public WsRsRestClient(final JobServerSparkContextConfig contextConfig) throws KeyManagementException,
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
        final long timeout = contextConfig.getReceiveTimeout().toMillis();
        m_clientPolicy = createClientPolicy(Duration.ofMillis(timeout), Duration.ofMillis(timeout));
        m_proxyAuthPolicy = configureProxyIfNecessary(contextConfig.getJobServerUrl(), m_clientPolicy);
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
