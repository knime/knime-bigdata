package com.knime.bigdata.spark.core.context.jobserver.rest;

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

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
class WsRsRestClient implements IRestClient {

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


    public final Client client;

    public final WebTarget baseTarget;

    public WsRsRestClient(final SparkContextConfig contextConfig) throws KeyManagementException, NoSuchAlgorithmException, URISyntaxException {
        client = ClientBuilder.newBuilder().sslContext(SSLProvider.setupSSLContext())
        .hostnameVerifier(getHostnameVerifier()).build();
        client.register(MultiPartFeature.class);
        if (contextConfig.useAuthentication() && contextConfig.getUser() != null && contextConfig.getPassword() != null) {
            client.register(HttpAuthenticationFeature.basic(contextConfig.getUser(),
                    String.valueOf(contextConfig.getPassword())));
        }

        baseTarget = client.target(new URI(contextConfig.getJobManagerUrl()));
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

        WebTarget target = baseTarget.path(aPath);
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
}
