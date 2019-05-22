package org.knime.bigdata.spark.core.livy.jobapi;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils.StagingAreaAccess;

@SparkClass
public class StagingAreaTester {

    public static final String TESTFILE_STRING = "testfileString";

    public static void validateTestfileContent(final StagingAreaAccess stagingAreaAccess, final String testfileName)
        throws IOException {
        try (final DataInputStream dataIn = new DataInputStream(stagingAreaAccess.newDownloadStream(testfileName))) {
            if (!dataIn.readUTF().equals(TESTFILE_STRING)) {
                throw new IOException("Testfile content was not as expected");
            }
        }
    }

    public static String writeTestfileContent(final StagingAreaAccess stagingAreaAccess) throws IOException {
        final Entry<String, OutputStream> uploadStream = stagingAreaAccess.newUploadStream();

        try (final DataOutputStream dataOut = new DataOutputStream(uploadStream.getValue())) {
            dataOut.writeUTF(TESTFILE_STRING);
        }

        return uploadStream.getKey();
    }
}
