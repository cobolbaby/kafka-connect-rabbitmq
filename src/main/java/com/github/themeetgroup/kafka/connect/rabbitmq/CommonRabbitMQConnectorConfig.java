/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.themeetgroup.kafka.connect.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public abstract class CommonRabbitMQConnectorConfig extends AbstractConfig {

  public static final String USERNAME_CONFIG = "rabbitmq.username";
  public static final String PASSWORD_CONFIG = "rabbitmq.password";
  public static final String VIRTUAL_HOST_CONFIG = "rabbitmq.virtual.host";
  public static final String REQUESTED_CHANNEL_MAX_CONFIG = "rabbitmq.requested.channel.max";
  public static final String REQUESTED_FRAME_MAX_CONFIG = "rabbitmq.requested.frame.max";
  public static final String CONNECTION_TIMEOUT_CONFIG = "rabbitmq.connection.timeout.ms";
  public static final String HANDSHAKE_TIMEOUT_CONFIG = "rabbitmq.handshake.timeout.ms";
  public static final String SHUTDOWN_TIMEOUT_CONFIG = "rabbitmq.shutdown.timeout.ms";
  public static final String REQUESTED_HEARTBEAT_CONFIG = "rabbitmq.requested.heartbeat.seconds";
  public static final String AUTOMATIC_RECOVERY_ENABLED_CONFIG = "rabbitmq.automatic.recovery.enabled";
  public static final String TOPOLOGY_RECOVERY_ENABLED_CONFIG = "rabbitmq.topology.recovery.enabled";
  public static final String NETWORK_RECOVERY_INTERVAL_CONFIG = "rabbitmq.network.recovery.interval.ms";
  public static final String HOST_CONFIG = "rabbitmq.host";
  public static final String PORT_CONFIG = "rabbitmq.port";
  public static final String USE_SSL = "rabbitmq.ssl";
  public static final String KEYSTORE_LOCATION = "rabbitmq.ssl.keystore.location";
  public static final String KEYSTORE_PASSWORD = "rabbitmq.ssl.keystore.password";
  public static final String KEYSTORE_PASSPHRASE = "rabbitmq.ssl.keystore.passphrase";
  public static final String KEYSTORE_TYPE = "rabbitmq.ssl.keystore.type";
  public static final String TRUSTSTORE_LOCATION = "rabbitmq.ssl.truststore.location";
  public static final String TRUSTSTORE_PASSWORD = "rabbitmq.ssl.truststore.password";
  public static final String TRUSTSTORE_TYPE = "rabbitmq.ssl.truststore.type";
  public static final String SSL_PROTOCOL = "rabbitmq.ssl.protocol";

  static final String HOST_DOC = "The RabbitMQ host to connect to. See `ConnectionFactory.setHost(java.lang.String) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setHost-java.lang.String->`_";
  static final String USERNAME_DOC = "The username to authenticate to RabbitMQ with. See `ConnectionFactory.setUsername(java.lang.String) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setUsername-java.lang.String->`_";
  static final String PASSWORD_DOC = "The password to authenticate to RabbitMQ with. See `ConnectionFactory.setPassword(java.lang.String) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setPassword-java.lang.String->`_";
  static final String VIRTUAL_HOST_DOC = "The virtual host to use when connecting to the broker. See `ConnectionFactory.setVirtualHost(java.lang.String) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setVirtualHost-java.lang.String->`_";
  static final String REQUESTED_CHANNEL_MAX_DOC = "Initially requested maximum channel number. Zero for unlimited. See `ConnectionFactory.setRequestedChannelMax(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setRequestedChannelMax-int->`_";
  static final String REQUESTED_FRAME_MAX_DOC = "Initially requested maximum frame size, in octets. Zero for unlimited. See `ConnectionFactory.setRequestedFrameMax(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setRequestedFrameMax-int->`_";
  static final String CONNECTION_TIMEOUT_DOC = "Connection TCP establishment timeout in milliseconds. zero for infinite. See `ConnectionFactory.setConnectionTimeout(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setConnectionTimeout-int->`_";
  static final String HANDSHAKE_TIMEOUT_DOC = "The AMQP0-9-1 protocol handshake timeout, in milliseconds. See `ConnectionFactory.setHandshakeTimeout(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setHandshakeTimeout-int->`_";
  static final String SHUTDOWN_TIMEOUT_DOC = "Set the shutdown timeout. This is the amount of time that Consumer " +
      "implementations have to continue working through deliveries (and other Consumer callbacks) after the connection " +
      "has closed but before the ConsumerWorkService is torn down. If consumers exceed this timeout then any remaining " +
      "queued deliveries (and other Consumer callbacks, *including* the Consumer's handleShutdownSignal() invocation) " +
      "will be lost. " +
      "See `ConnectionFactory.setShutdownTimeout(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setShutdownTimeout-int->`_";
  static final String REQUESTED_HEARTBEAT_DOC = "Set the requested heartbeat timeout. Heartbeat frames will be sent " +
      "at about 1/2 the timeout interval. If server heartbeat timeout is configured to a non-zero value, this method " +
      "can only be used to lower the value; otherwise any value provided by the client will be used. " +
      "See `ConnectionFactory.setRequestedHeartbeat(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setRequestedHeartbeat-int->`_";
  static final String AUTOMATIC_RECOVERY_ENABLED_DOC = "Enables or disables automatic connection recovery. See `ConnectionFactory.setAutomaticRecoveryEnabled(boolean) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setAutomaticRecoveryEnabled-boolean->`_";
  static final String TOPOLOGY_RECOVERY_ENABLED_DOC = "Enables or disables topology recovery. See `ConnectionFactory.setTopologyRecoveryEnabled(boolean) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setTopologyRecoveryEnabled-boolean->`_";
  static final String NETWORK_RECOVERY_INTERVAL_DOC = "See `ConnectionFactory.setNetworkRecoveryInterval(long) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setNetworkRecoveryInterval-long->`_";
  static final String PORT_DOC = "The RabbitMQ port to connect to. See `ConnectionFactory.setPort(int) <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/ConnectionFactory.html#setPort-int->`_";
  static final String USE_SSL_DOC = "Enable SSL/TLS";
  static final String KEYSTORE_LOCATION_DOC = "Keystore location";
  static final String KEYSTORE_PASSWORD_DOC = "Keystore password";
  static final String KEYSTORE_PASSPHRASE_DOC = "Keystore passphrase";
  static final String KEYSTORE_TYPE_DOC = "Keystore type (only JKS supported)";
  static final String TRUSTSTORE_LOCATION_DOC = "Truststore location";
  static final String TRUSTSTORE_PASSWORD_DOC = "Truststore password";
  static final String TRUSTSTORE_TYPE_DOC = "Truststore type (only JKS supported)";
  static final String SSL_PROTOCOL_DOC = "SSL/TLS protocol to use";

  public final String username;
  public final String password;
  public final String virtualHost;
  public final int requestedChannelMax;
  public final int requestedFrameMax;
  public final int connectionTimeout;
  public final int handshakeTimeout;
  public final int shutdownTimeout;
  public final int requestedHeartbeat;
  public final boolean automaticRecoveryEnabled;
  public final boolean topologyRecoveryEnabled;
  public final long networkRecoveryInterval;
  public final String host;
  public final int port;
  public final boolean useSsl;
  public final String keystoreLocation;
  public final String keystorePassword;
  public final String keystorePassphrase;
  public final String keystoreType;
  public final String truststoreLocation;
  public final String truststorePassword;
  public final String truststoreType;
  public final String sslProtocol;

  public final ConnectionFactory connectionFactory;

  public CommonRabbitMQConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.username = this.getString(USERNAME_CONFIG);
    this.password = this.getString(PASSWORD_CONFIG);
    this.virtualHost = this.getString(VIRTUAL_HOST_CONFIG);
    this.requestedChannelMax = this.getInt(REQUESTED_CHANNEL_MAX_CONFIG);
    this.requestedFrameMax = this.getInt(REQUESTED_FRAME_MAX_CONFIG);
    this.connectionTimeout = this.getInt(CONNECTION_TIMEOUT_CONFIG);
    this.handshakeTimeout = this.getInt(HANDSHAKE_TIMEOUT_CONFIG);
    this.shutdownTimeout = this.getInt(SHUTDOWN_TIMEOUT_CONFIG);
    this.requestedHeartbeat = this.getInt(REQUESTED_HEARTBEAT_CONFIG);
    this.automaticRecoveryEnabled = this.getBoolean(AUTOMATIC_RECOVERY_ENABLED_CONFIG);
    this.topologyRecoveryEnabled = this.getBoolean(TOPOLOGY_RECOVERY_ENABLED_CONFIG);
    this.networkRecoveryInterval = this.getInt(NETWORK_RECOVERY_INTERVAL_CONFIG);
    this.host = this.getString(HOST_CONFIG);
    this.port = this.getInt(PORT_CONFIG);
    this.useSsl = this.getBoolean(USE_SSL);
    this.keystoreLocation = this.getString(KEYSTORE_LOCATION);
    this.keystorePassword = this.getString(KEYSTORE_PASSWORD);
    this.keystorePassphrase = this.getString(KEYSTORE_PASSPHRASE);
    this.keystoreType = this.getString(KEYSTORE_TYPE);
    this.truststoreLocation = this.getString(TRUSTSTORE_LOCATION);
    this.truststorePassword = this.getString(TRUSTSTORE_PASSWORD);
    this.truststoreType = this.getString(TRUSTSTORE_TYPE);
    this.sslProtocol = this.getString(SSL_PROTOCOL);

    this.connectionFactory = connectionFactory();
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(HOST_CONFIG, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_HOST, ConfigDef.Importance.HIGH, HOST_DOC)
        .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_USER, ConfigDef.Importance.HIGH, USERNAME_DOC)
        .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_PASS, ConfigDef.Importance.HIGH, PASSWORD_DOC)
        .define(VIRTUAL_HOST_CONFIG, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_VHOST, ConfigDef.Importance.HIGH, VIRTUAL_HOST_DOC)
        .define(REQUESTED_CHANNEL_MAX_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_CHANNEL_MAX, ConfigDef.Importance.LOW, REQUESTED_CHANNEL_MAX_DOC)
        .define(REQUESTED_FRAME_MAX_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_FRAME_MAX, ConfigDef.Importance.LOW, REQUESTED_FRAME_MAX_DOC)
        .define(CONNECTION_TIMEOUT_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT, ConfigDef.Importance.LOW, CONNECTION_TIMEOUT_DOC)
        .define(HANDSHAKE_TIMEOUT_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT, ConfigDef.Importance.LOW, HANDSHAKE_TIMEOUT_DOC)
        .define(SHUTDOWN_TIMEOUT_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT, ConfigDef.Importance.LOW, SHUTDOWN_TIMEOUT_DOC)
        .define(REQUESTED_HEARTBEAT_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_HEARTBEAT, ConfigDef.Importance.LOW, REQUESTED_HEARTBEAT_DOC)
        .define(AUTOMATIC_RECOVERY_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, AUTOMATIC_RECOVERY_ENABLED_DOC)
        .define(TOPOLOGY_RECOVERY_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, TOPOLOGY_RECOVERY_ENABLED_DOC)
        .define(NETWORK_RECOVERY_INTERVAL_CONFIG, ConfigDef.Type.INT, 10000, ConfigDef.Importance.LOW, NETWORK_RECOVERY_INTERVAL_DOC)
        .define(PORT_CONFIG, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_AMQP_PORT, ConfigDef.Importance.MEDIUM, PORT_DOC)
        .define(USE_SSL, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, USE_SSL_DOC)
        .define(KEYSTORE_LOCATION, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, KEYSTORE_LOCATION_DOC)
        .define(KEYSTORE_PASSWORD, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, KEYSTORE_PASSWORD_DOC)
        .define(KEYSTORE_PASSPHRASE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, KEYSTORE_PASSPHRASE_DOC)
        .define(KEYSTORE_TYPE, ConfigDef.Type.STRING, "JKS", ConfigDef.Importance.LOW, KEYSTORE_TYPE_DOC)
        .define(TRUSTSTORE_LOCATION, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, TRUSTSTORE_LOCATION_DOC)
        .define(TRUSTSTORE_PASSWORD, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, TRUSTSTORE_PASSWORD_DOC)
        .define(TRUSTSTORE_TYPE, ConfigDef.Type.STRING, "JKS", ConfigDef.Importance.LOW, TRUSTSTORE_TYPE_DOC)
        .define(SSL_PROTOCOL, ConfigDef.Type.STRING, "TLSv1.3", ConfigDef.Importance.LOW, SSL_PROTOCOL_DOC);
  }

  public final ConnectionFactory connectionFactory() {
    ConnectionFactory connectionFactory = new ConnectionFactory();

    connectionFactory.setHost(this.host);
    connectionFactory.setUsername(this.username);
    connectionFactory.setPassword(this.password);
    connectionFactory.setVirtualHost(this.virtualHost);
    connectionFactory.setRequestedChannelMax(this.requestedChannelMax);
    connectionFactory.setRequestedFrameMax(this.requestedFrameMax);
    connectionFactory.setConnectionTimeout(this.connectionTimeout);
    connectionFactory.setHandshakeTimeout(this.handshakeTimeout);
    connectionFactory.setShutdownTimeout(this.shutdownTimeout);
    connectionFactory.setRequestedHeartbeat(this.requestedHeartbeat);
    connectionFactory.setAutomaticRecoveryEnabled(this.automaticRecoveryEnabled);
    connectionFactory.setTopologyRecoveryEnabled(this.topologyRecoveryEnabled);
    connectionFactory.setNetworkRecoveryInterval(this.networkRecoveryInterval);

    connectionFactory.setPort(this.port);
    if (this.useSsl)
      try {
        connectionFactory.useSslProtocol(getSLLContext());
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        e.printStackTrace();
      } catch (UnrecoverableKeyException e) {
        e.printStackTrace();
      } catch (CertificateException e) {
        e.printStackTrace();
      } catch (KeyStoreException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    return connectionFactory;
  }

  public final SSLContext getSLLContext() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {
    char[] keyPassphrase = this.keystorePassword.toCharArray();
    char[] passphrase = this.keystorePassphrase.toCharArray();
    KeyStore ks = KeyStore.getInstance(this.keystoreType);
    ks.load(new BufferedInputStream(new FileInputStream(this.keystoreLocation)), keyPassphrase);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
    kmf.init(ks, passphrase);

    char[] trustPassphrase = this.truststorePassword.toCharArray();
    KeyStore tks = KeyStore.getInstance(this.truststoreType);
    tks.load(new BufferedInputStream(new FileInputStream(this.truststoreLocation)), trustPassphrase);
    TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
    tmf.init(tks);

    SSLContext sslContext = SSLContext.getInstance(this.sslProtocol);
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    return sslContext;
  }
}
