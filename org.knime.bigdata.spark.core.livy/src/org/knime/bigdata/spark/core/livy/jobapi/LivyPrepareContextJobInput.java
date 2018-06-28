package org.knime.bigdata.spark.core.livy.jobapi;

import java.util.List;

import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;

/**
 * Job input to use when preparing/validating a Livy Spark context.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LivyPrepareContextJobInput extends PrepareContextJobInput {

    public static final String LIVY_PREPARE_CONTEXT_JOB_ID = "LivyPrepareContextJob";

    private static final String KEY_STAGING_AREA = "stagingArea";

    private static final String KEY_STAGING_AREA_IS_PATH = "stagingAreaIsPath";

    private static final String KEY_TESTFILE_NAME = "testfileName";

    /**
     * Empty constructor for (de)serialization.
     */
    public LivyPrepareContextJobInput() {
        super();
    }

    /**
     * Creates a new instance.
     *
     * @param jobJarHash A hash over the contents of the job jar.
     * @param sparkVersion The Spark version to be expected on the cluster side.
     * @param pluginVersion The current version of the KNIME Extension for Apache Spark.
     * @param typeConverters The {@link IntermediateToSparkConverter}s to use on the cluster side.
     * @param stagingArea The path or full URI of the staging area to use.
     * @param stagingAreaIsPath When true, then the value in "stagingArea" is a path, otherwise it is a full URI.
     * @param testfileName The name of a testfile in the staging area, which shall be attempted to be read on the Spark
     *            driver.
     */
    public LivyPrepareContextJobInput(final String jobJarHash, final String sparkVersion, final String pluginVersion,
        final List<IntermediateToSparkConverter<?>> typeConverters, final String stagingArea,
        final boolean stagingAreaIsPath, final String testfileName) {

        super(jobJarHash, sparkVersion, pluginVersion, typeConverters);
        set(KEY_STAGING_AREA, stagingArea);
        set(KEY_STAGING_AREA_IS_PATH, stagingAreaIsPath);
        set(KEY_TESTFILE_NAME, testfileName);
    }

    /**
     * @return The path or full URI of the staging area to use.
     */
    public String getStagingArea() {
        return get(KEY_STAGING_AREA);
    }

    /**
     * @return true, if the value in "stagingArea" is a path, false if it is a full URI.
     */
    public boolean stagingAreaIsPath() {
        return get(KEY_STAGING_AREA_IS_PATH);
    }

    /**
     * @return the name of a testfile in the staging area, which shall be attempted to be read on the Spark driver.
     */
    public String getTestfileName() {
        return get(KEY_TESTFILE_NAME);
    }
}
