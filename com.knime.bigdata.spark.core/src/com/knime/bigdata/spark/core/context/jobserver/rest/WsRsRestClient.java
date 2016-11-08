package com.knime.bigdata.spark.core.context.jobserver.rest;

import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HostnameVerifier;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
class WsRsRestClient implements IRestClient {

    /** Chunk threshold in bytes. */
    private static final int CHUNK_THRESHOLD = 10*1024*1024; // 10MB

    /** Length in bytes of each chunk. */
    private static final int CHUNK_LENGTH = 1*1024*1024; // 1MB

    private static HostnameVerifier getHostnameVerifier() {
        return new HostnameVerifier() {
            @Override
            public boolean verify(final String hostname, final javax.net.ssl.SSLSession sslSession) {
                return true;
            }
        };
    }

    static {
        //disable loop-backs that ask for user login
        // TODO - verify that this does not conflict with other KNIME components...
        java.net.Authenticator.setDefault(new Authenticator() {});
    }


    public final Client m_client;

    public final WebTarget m_baseTarget;

    public WsRsRestClient(final SparkContextConfig contextConfig)
        throws KeyManagementException, NoSuchAlgorithmException, URISyntaxException, UnsupportedEncodingException {
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
        }

        m_baseTarget = m_client.target(new URI(contextConfig.getJobServerUrl()));
    }

    /**
     * create the invocation builder for this REST client
     *
     * @param aPath invocation path
     * @param aParams optional parameters
     * @return builder
     * @throws URISyntaxException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws KNIMESparkException
     */
    private Invocation.Builder getInvocationBuilder(final String aPath, final String[] aParams) {

        WebTarget target = m_baseTarget.path(aPath);
        if (aParams != null) {
            for (int p = 0; p < aParams.length; p = p + 2) {
                target = target.queryParam(aParams[p], aParams[p + 1]);
            }
        }
        Invocation.Builder builder = target.request();
        return builder;
    }

    @Override
    public <T> Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity) {
        Invocation.Builder builder = getInvocationBuilder(aPath, aArgs);
        configureChunkTransfer(builder);
        return builder.post(aEntity);
    }

    @Override
    public Response delete(final String aPath) {
        Invocation.Builder builder = getInvocationBuilder(aPath, null);
        return builder.buildDelete().invoke();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Response get(final String aPath) {
        final Invocation.Builder builder = getInvocationBuilder(aPath, null);
        return builder.get();
    }

    /**
     * Configures chunk transfer threshold and length.
     * See {@link #CHUNK_THRESHOLD} and {@link #CHUNK_LENGTH}.
     */
    private void configureChunkTransfer(final Invocation.Builder builder) {
        HTTPConduit http = org.apache.cxf.jaxrs.client.WebClient.getConfig(builder).getHttpConduit();
        HTTPClientPolicy httpClientPolicy = new HTTPClientPolicy();
        httpClientPolicy.setAllowChunking(true);
        httpClientPolicy.setChunkingThreshold(CHUNK_THRESHOLD);
        httpClientPolicy.setChunkLength(CHUNK_LENGTH);
        http.setClient(httpClientPolicy);
    }
}
