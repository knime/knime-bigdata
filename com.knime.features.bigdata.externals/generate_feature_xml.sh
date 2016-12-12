#!/bin/bash


if [ "$#" -gt "0" ] ; then
        echo "This script prints the contents of a feature.xml to stdout, which contains all bundles"
        echo "generated by maven2osgi/pom.xml."
        exit 0
fi

FEATURE_XML_HEADER='<?xml version="1.0" encoding="UTF-8"?>
<feature
      id="com.knime.features.bigdata.externals"
      label="Feature with all external plug-ins for the KNIME Big Data Extension"
      version="1.7.0.qualifier"
      provider-name="KNIME GmbH, Konstanz, Germany">
      
      <!-- THIS FILE IS GENERATED BY generate_feature_xml.sh. MANUAL EDITS MAY THUS BE OVERWRITTEN --> 
      '

FEATURE_XML_FOOTER="</feature>"

PLUGIN_TEMPLATE='   <plugin
      id=\"${plugin_name}\"
      download-size=\"0\"
      install-size=\"0\"
      version=\"${plugin_version}\"
      unpack=\"false\"/>
'

test -d ./maven2osgi || { echo >&2 "Could not find maven2osig subfolder. Aborting."; exit 1; }

# pushd maven2osgi
# rm -R ./target
# mvn p2:site || { echo >&2 "mvn p2:site failed. Aborting."; popd ; exit 1; }
# popd

echo "${FEATURE_XML_HEADER}"
for fullfile in  ./maven2osgi/target/repository/plugins/*.jar ; do 
  jarfile=$(basename "${fullfile}")
  plugin_name=$(echo "$jarfile" | sed 's/^\(.*\)_[^_]*\.jar$/\1/' )
  plugin_version=$( echo "$jarfile" | sed 's/^.*_\([^_]*\)\.jar$/\1/' )
  eval "echo \"${PLUGIN_TEMPLATE}\""
done
echo "${FEATURE_XML_FOOTER}"



