CREATE TABLE iris (
        `sepal_length` DOUBLE,
        `sepal_width` DOUBLE,
        `petal_length` DOUBLE,
        `petal_width` DOUBLE,
        `class` STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ESCAPED BY '\\' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/tmp/iris.all.csv' INTO TABLE iris;