#
# SSL keystore generator script
#
SERVER_NAME=$HOSTNAME
CLIENT_NAME=$(hostname)
STOREPASS=testtest
VALIDITY=3650
SERVER_KEYSTORE=$BASE_DIR/server.keystore.jks
CLIENT_KEYSTORE=$BASE_DIR/client.keystore.jks
KEY_OPT="-keysize 2048 -keyalg RSA"

log "Using client CN: $CLIENT_NAME"

[ -f $SERVER_KEYSTORE ] && rm -f $SERVER_KEYSTORE
[ -f $CLIENT_KEYSTORE ] && rm -f $CLIENT_KEYSTORE

keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -keypass $STOREPASS -alias ca -dname CN=CA -genkeypair $KEY_OPT -validity $VALIDITY
keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -keypass $STOREPASS -alias $SERVER_NAME -dname CN=$SERVER_NAME -genkeypair $KEY_OPT
keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias $SERVER_NAME -certreq | \
            keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias ca -gencert -validity $VALIDITY | \
            keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias $SERVER_NAME -importcert
 
keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias ca -exportcert | \
            keytool -keystore $CLIENT_KEYSTORE -storepass $STOREPASS -alias ca -importcert -noprompt
 
keytool -keystore $CLIENT_KEYSTORE -storepass $STOREPASS -keypass $STOREPASS -alias $CLIENT_NAME -dname CN=$CLIENT_NAME -genkeypair $KEY_OPT
keytool -keystore $CLIENT_KEYSTORE -storepass $STOREPASS -alias $CLIENT_NAME -certreq | \
            keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias ca -gencert  -validity $VALIDITY | \
            keytool -keystore $CLIENT_KEYSTORE -storepass $STOREPASS -alias $CLIENT_NAME -importcert -noprompt
 
# keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias ca -exportcert -rfc -file CA.crt
# keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -alias $SERVER_NAME -exportcert -rfc -file server.crt
# keytool -keystore $CLIENT_KEYSTORE -storepass $STOREPASS -alias $CLIENT_NAME -exportcert -rfc -file client.crt
 
# keytool -keystore $SERVER_KEYSTORE -storepass $STOREPASS -list
# keytool -keystore $CLIENT_KEYSTORE -storepass $STOREPASS -list
