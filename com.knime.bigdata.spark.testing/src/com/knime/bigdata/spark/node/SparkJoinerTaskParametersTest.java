package com.knime.bigdata.spark.node;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;

import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.joiner.SparkJoinerTask;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SparkJoinerTaskParametersTest {

    @Test
    public void ensureThatAllRequiredParametersAreSet() throws Throwable {
        SparkJoinerTask testObj =
            new SparkJoinerTask(null, "tab1", "tab2", JoinMode.InnerJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");

        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate());
    }
}