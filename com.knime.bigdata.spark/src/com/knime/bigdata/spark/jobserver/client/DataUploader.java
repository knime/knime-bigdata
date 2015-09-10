package com.knime.bigdata.spark.jobserver.client;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.json.JsonArray;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 * handles the client side of the job-server in all requests related to uploading data files (including jars)
 *
 * @author dwk
 *
 */
public class DataUploader {

    /**
     * path prefix for data
     */
    public static final String DATA_PATH = "/data/";

    private final static NodeLogger LOGGER = NodeLogger.getLogger(DataUploader.class.getName());

    /**
     * upload data file to the server
     *
     * @param aContextContainer context configuration container
     * @param aPath path to source data file
     * @param aFileNamePrefix proposed file name prefix, the JobServer will return the full name,
     * likely including a time stamp to ensure uniqueness
     * @return path of data file on server
     *
     * @throws GenericKnimeSparkException
     */
    public static String uploadDataFile(final KNIMESparkContext aContextContainer, final String aPath, final String aFileNamePrefix)
            throws GenericKnimeSparkException {
        Response response = uploadFile(aContextContainer, aPath, DATA_PATH+aFileNamePrefix);
        return RestClient.getJSONFieldFromResponse(response, "result", "filename");
    }

    /**
     * Upload a file to the server.
     *
     * @param aContextContainer context configuration container
     * @param aDataPath path to source data file
     * @param aRoute route for job server (this is either /data/<filename> or /jars/appname
     * @return command response
     *
     * @throws GenericKnimeSparkException
     */
    public static Response uploadFile(final KNIMESparkContext aContextContainer, final String aDataPath, final String aRoute)
        throws GenericKnimeSparkException {
        final File file = new File(aDataPath);
        if (!file.exists()) {
            final String msg =
                "ERROR: file '" + file.getAbsolutePath()
                    + "' does not exist. Make sure to set the proper (relative) path in the application.conf file.";
            LOGGER.error(msg);
            throw new GenericKnimeSparkException(msg);
        }

        Response response = RestClient.post(aContextContainer, aRoute, null,
            Entity.entity(file, MediaType.APPLICATION_OCTET_STREAM));
        RestClient.checkStatus(response, "Failed to upload file to server. Path: " + aDataPath, Status.OK);
        return response;
    }

    /**
     * delete some (data) file on the server
     *
     * @param aContextContainer context configuration container
     * @param aFileName full path of the file as returned by the JobServer when the file was uploaded
     * @throws GenericKnimeSparkException
     */
    public static void deleteFile(final KNIMESparkContext aContextContainer, final String aFileName)
            throws GenericKnimeSparkException {
        try {
            Response response = RestClient.delete(aContextContainer, DATA_PATH + URLEncoder.encode(aFileName, "UTF-8"));
            RestClient.checkStatus(response, "Failed to delete file " + aFileName + "!", Status.OK);
        } catch (UnsupportedEncodingException e) {
            throw new GenericKnimeSparkException(e);
        }
    }



    /**
     * query the job-server for the list of currently managed files
     *
     * @param aContextContainer context configuration container
     * @return list of file names
     * @throws GenericKnimeSparkException
     */
    public static String[] listFiles(final KNIMESparkContext aContextContainer)
            throws GenericKnimeSparkException {
        final JsonArray files = RestClient.toJSONArray(aContextContainer, "/data");
        final String[] res = new String[files.size()];
        for (int i = 0; i < files.size(); i++) {
            res[i] = files.getString(i);
        }
        return res;
    }


}
