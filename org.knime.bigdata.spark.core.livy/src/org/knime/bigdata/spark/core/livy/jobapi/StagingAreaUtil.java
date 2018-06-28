package org.knime.bigdata.spark.core.livy.jobapi;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import org.knime.bigdata.spark.core.job.SparkClass;

@SparkClass
public class StagingAreaUtil {

    public static final String TESTFILE_STRING = "testfileString";

    public static void validateTestfileContent(final InputStream in) throws IOException {
        final DataInputStream dataIn = new DataInputStream(in);
        if (!dataIn.readUTF().equals(TESTFILE_STRING)) {
            throw new IOException("Testfile content was not as expected");
        }
    }

    public static void writeTestfileContent(final OutputStream out) throws IOException {
        final DataOutputStream dataOut = new DataOutputStream(out);
        dataOut.writeUTF(TESTFILE_STRING);
    }

    // counterpart is in class 
    public static Map<String, Object> toSerializedMap(final Map<String, Object> toSerialize) throws IOException {
        final AtomicReference<String> stagingFile = new AtomicReference<>();

        try {
            final Map<String, Object> serializedMap =
                LivyJobSerializationUtils.serializeObjectsToStream(toSerialize, () -> {
                    try {
                        Entry<String, OutputStream> outEntry = StagingArea.createUploadStream();
                        stagingFile.set(outEntry.getKey());
                        return outEntry.getValue();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

            serializedMap.put(LivyJobSerializationUtils.KEY_SERIALIZED_FIELDS_STAGING_FILE, stagingFile.get());
            return serializedMap;

        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            } else {
                throw e;
            }
        }
    }
}
