package org.knime.bigdata.spark.core.sparkjobserver.rest;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.core.node.NodeLogger;

/**
 * creates and handles REST requests
 *
 * @author dwk
 *
 */
public class RestClient {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(RestClient.class.getName());

    private final IRestClient client;

    /**
     * create a config-specific REST client instance
     *
     * @param contextConfig
     * @throws URISyntaxException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws UnsupportedEncodingException
     */
    public RestClient(final JobServerSparkContextConfig contextConfig)
        throws KeyManagementException, NoSuchAlgorithmException, URISyntaxException, UnsupportedEncodingException {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Create RestClient");
            LOGGER.debug("Config: " + contextConfig);
        }
        client = new WsRsRestClient(contextConfig);
    }

    /**
     * Send a HTTP GET request.
     *
     * @param aPath
     * @return server response
     */
    public synchronized Response get(final String aPath) {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest GET");
            LOGGER.debug("Path: " + aPath);
        }

        return client.get(aPath);
    }

    /**
     * Send a HTTP POST request
     *
     * @param aPath
     * @param aArgs
     * @param aEntity
     * @return server response
     */
    public synchronized <T> Response post(final String aPath, final String[] aArgs, final Entity<T> aEntity) {

        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest POST");
            LOGGER.debug("Path: " + aPath);
            LOGGER.debug("Args: " + Arrays.toString(aArgs));
            LOGGER.debug("Entity: " + aEntity);
        }
        return client.post(aPath, aArgs, aEntity);
    }

    /**
     * Send a HTTP DELETE request
     *
     * @param aPath
     * @return server response
     */
    public synchronized Response delete(final String aPath) {
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Rest delete");
            LOGGER.debug("Path: " + aPath);
        }
        return client.delete(aPath);
    }
}
