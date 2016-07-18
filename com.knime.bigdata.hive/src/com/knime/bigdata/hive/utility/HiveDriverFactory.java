/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 20.05.2016 by koetter
 */
package com.knime.bigdata.hive.utility;

import org.eclipse.core.runtime.Platform;
import org.knime.core.node.port.database.connection.DefaultDBDriverFactory;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class HiveDriverFactory extends DefaultDBDriverFactory {
    /**Hive driver class name.*/
    static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";

    /**Constructor.*/
    HiveDriverFactory() {
        super(HiveDriverFactory.DRIVER, getBundleFiles(Platform.getBundle("com.knime.bigdata.cdh5"),
            "lib/apacheds-i18n-2.0.0-M15.jar",
            "lib/apacheds-kerberos-codec-2.0.0-M15.jar",
            "lib/api-asn1-api-1.0.0-M20.jar",
            "lib/api-util-1.0.0-M20.jar",
            "lib/avro.jar",
            "lib/commons-beanutils-1.7.0.jar",
            "lib/commons-beanutils-core-1.8.0.jar",
            "lib/commons-cli-1.2.jar",
            "lib/commons-codec-1.4.jar",
            "lib/commons-collections-3.2.1.jar",
            "lib/commons-compress-1.4.1.jar",
            "lib/commons-configuration-1.6.jar",
            "lib/commons-digester-1.8.jar",
            "lib/hadoop-annotations-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-auth-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-common-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-hdfs-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-mapreduce-client-app-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-mapreduce-client-common-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-mapreduce-client-core-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-mapreduce-client-jobclient-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-mapreduce-client-shuffle-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-yarn-api-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-yarn-client-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-yarn-common-2.3.0-cdh5.1.0.jar",
            "lib/hadoop-yarn-server-common-2.3.0-cdh5.1.0.jar",
            "lib/jackson-core-asl-1.8.8.jar",
            "lib/jackson-mapper-asl-1.8.8.jar",
            "lib/jaxb-api-2.2.2.jar",
            "lib/jersey-core-1.9.jar",
            "lib/jsr305-1.3.9.jar",
            "lib/paranamer-2.3.jar",
            "lib/protobuf-java-2.5.0.jar",
            "lib/slf4j-log4j12.jar",
            "lib/snappy-java-1.0.4.1.jar",
            "lib/stax-api-1.0-2.jar",
            "lib/xmlenc-0.52.jar",
            "lib/xz-1.0.jar",
            "lib/zookeeper.jar",
            "lib/hive-jdbc-0.12.0-cdh5.1.0.jar",
            "lib/hive-common-0.12.0-cdh5.1.0.jar",
            "lib/hive-metastore-0.12.0-cdh5.1.0.jar",
            "lib/hive-service-0.12.0-cdh5.1.0.jar",
            "lib/hive-shims-0.23-0.12.0-cdh5.1.0.jar",
            "lib/hive-shims-common-0.12.0-cdh5.1.0.jar",
            "lib/hive-shims-common-secure-0.12.0-cdh5.1.0.jar",
            "lib/libthrift-0.9.0.cloudera.2.jar",
            "lib/libfb303-0.9.0.jar"
        ));
    }
}
