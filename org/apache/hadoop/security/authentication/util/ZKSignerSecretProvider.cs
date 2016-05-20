using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>
	/// A SignerSecretProvider that synchronizes a rolling random secret between
	/// multiple servers using ZooKeeper.
	/// </summary>
	/// <remarks>
	/// A SignerSecretProvider that synchronizes a rolling random secret between
	/// multiple servers using ZooKeeper.
	/// <p>
	/// It works by storing the secrets and next rollover time in a ZooKeeper znode.
	/// All ZKSignerSecretProviders looking at that znode will use those
	/// secrets and next rollover time to ensure they are synchronized.  There is no
	/// "leader" -- any of the ZKSignerSecretProviders can choose the next secret;
	/// which one is indeterminate.  Kerberos-based ACLs can also be enforced to
	/// prevent a malicious third-party from getting or setting the secrets.  It uses
	/// its own CuratorFramework client for talking to ZooKeeper.  If you want to use
	/// your own Curator client, you can pass it to ZKSignerSecretProvider; see
	/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationFilter"
	/// 	/>
	/// for more details.
	/// <p>
	/// The supported configuration properties are:
	/// <ul>
	/// <li>signer.secret.provider.zookeeper.connection.string: indicates the
	/// ZooKeeper connection string to connect with.</li>
	/// <li>signer.secret.provider.zookeeper.path: indicates the ZooKeeper path
	/// to use for storing and retrieving the secrets.  All ZKSignerSecretProviders
	/// that need to coordinate should point to the same path.</li>
	/// <li>signer.secret.provider.zookeeper.auth.type: indicates the auth type to
	/// use.  Supported values are "none" and "sasl".  The default value is "none"
	/// </li>
	/// <li>signer.secret.provider.zookeeper.kerberos.keytab: set this to the path
	/// with the Kerberos keytab file.  This is only required if using Kerberos.</li>
	/// <li>signer.secret.provider.zookeeper.kerberos.principal: set this to the
	/// Kerberos principal to use.  This only required if using Kerberos.</li>
	/// <li>signer.secret.provider.zookeeper.disconnect.on.close: when set to "true",
	/// ZKSignerSecretProvider will close the ZooKeeper connection on shutdown.  The
	/// default is "true". Only set this to "false" if a custom Curator client is
	/// being provided and the disconnection is being handled elsewhere.</li>
	/// </ul>
	/// The following attribute in the ServletContext can also be set if desired:
	/// <ul>
	/// <li>signer.secret.provider.zookeeper.curator.client: A CuratorFramework
	/// client object can be passed here. If given, the "zookeeper" implementation
	/// will use this Curator client instead of creating its own, which is useful if
	/// you already have a Curator client or want more control over its
	/// configuration.</li>
	/// </ul>
	/// </remarks>
	public class ZKSignerSecretProvider : org.apache.hadoop.security.authentication.util.RolloverSignerSecretProvider
	{
		private const string CONFIG_PREFIX = "signer.secret.provider.zookeeper.";

		/// <summary>Constant for the property that specifies the ZooKeeper connection string.
		/// 	</summary>
		public const string ZOOKEEPER_CONNECTION_STRING = CONFIG_PREFIX + "connection.string";

		/// <summary>Constant for the property that specifies the ZooKeeper path.</summary>
		public const string ZOOKEEPER_PATH = CONFIG_PREFIX + "path";

		/// <summary>Constant for the property that specifies the auth type to use.</summary>
		/// <remarks>
		/// Constant for the property that specifies the auth type to use.  Supported
		/// values are "none" and "sasl".  The default value is "none".
		/// </remarks>
		public const string ZOOKEEPER_AUTH_TYPE = CONFIG_PREFIX + "auth.type";

		/// <summary>Constant for the property that specifies the Kerberos keytab file.</summary>
		public const string ZOOKEEPER_KERBEROS_KEYTAB = CONFIG_PREFIX + "kerberos.keytab";

		/// <summary>Constant for the property that specifies the Kerberos principal.</summary>
		public const string ZOOKEEPER_KERBEROS_PRINCIPAL = CONFIG_PREFIX + "kerberos.principal";

		/// <summary>
		/// Constant for the property that specifies whether or not the Curator client
		/// should disconnect from ZooKeeper on shutdown.
		/// </summary>
		/// <remarks>
		/// Constant for the property that specifies whether or not the Curator client
		/// should disconnect from ZooKeeper on shutdown.  The default is "true".  Only
		/// set this to "false" if a custom Curator client is being provided and the
		/// disconnection is being handled elsewhere.
		/// </remarks>
		public const string DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN = CONFIG_PREFIX + "disconnect.on.shutdown";

		/// <summary>
		/// Constant for the ServletContext attribute that can be used for providing a
		/// custom CuratorFramework client.
		/// </summary>
		/// <remarks>
		/// Constant for the ServletContext attribute that can be used for providing a
		/// custom CuratorFramework client. If set ZKSignerSecretProvider will use this
		/// Curator client instead of creating a new one. The providing class is
		/// responsible for creating and configuring the Curator client (including
		/// security and ACLs) in this case.
		/// </remarks>
		public const string ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE = CONFIG_PREFIX
			 + "curator.client";

		private const string JAAS_LOGIN_ENTRY_NAME = "ZKSignerSecretProviderClient";

		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider)));

		private string path;

		/// <summary>Stores the next secret that will be used after the current one rolls over.
		/// 	</summary>
		/// <remarks>
		/// Stores the next secret that will be used after the current one rolls over.
		/// We do this to help with rollover performance by actually deciding the next
		/// secret at the previous rollover.  This allows us to switch to the next
		/// secret very quickly.  Afterwards, we have plenty of time to decide on the
		/// next secret.
		/// </remarks>
		private volatile byte[] nextSecret;

		private readonly java.util.Random rand;

		/// <summary>Stores the current version of the znode.</summary>
		private int zkVersion;

		/// <summary>Stores the next date that the rollover will occur.</summary>
		/// <remarks>
		/// Stores the next date that the rollover will occur.  This is only used
		/// for allowing new servers joining later to synchronize their rollover
		/// with everyone else.
		/// </remarks>
		private long nextRolloverDate;

		private long tokenValidity;

		private org.apache.curator.framework.CuratorFramework client;

		private bool shouldDisconnect;

		private static int INT_BYTES = int.SIZE / byte.SIZE;

		private static int LONG_BYTES = long.SIZE / byte.SIZE;

		private static int DATA_VERSION = 0;

		public ZKSignerSecretProvider()
			: base()
		{
			rand = new java.util.Random();
		}

		/// <summary>
		/// This constructor lets you set the seed of the Random Number Generator and
		/// is meant for testing.
		/// </summary>
		/// <param name="seed">the seed for the random number generator</param>
		[com.google.common.annotations.VisibleForTesting]
		public ZKSignerSecretProvider(long seed)
			: base()
		{
			rand = new java.util.Random(seed);
		}

		/// <exception cref="System.Exception"/>
		public override void init(java.util.Properties config, javax.servlet.ServletContext
			 servletContext, long tokenValidity)
		{
			object curatorClientObj = servletContext.getAttribute(ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE
				);
			if (curatorClientObj != null && curatorClientObj is org.apache.curator.framework.CuratorFramework)
			{
				client = (org.apache.curator.framework.CuratorFramework)curatorClientObj;
			}
			else
			{
				client = createCuratorClient(config);
				servletContext.setAttribute(ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE
					, client);
			}
			this.tokenValidity = tokenValidity;
			shouldDisconnect = bool.parseBoolean(config.getProperty(DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN
				, "true"));
			path = config.getProperty(ZOOKEEPER_PATH);
			if (path == null)
			{
				throw new System.ArgumentException(ZOOKEEPER_PATH + " must be specified");
			}
			try
			{
				nextRolloverDate = Sharpen.Runtime.currentTimeMillis() + tokenValidity;
				// everyone tries to do this, only one will succeed and only when the
				// znode doesn't already exist.  Everyone else will synchronize on the
				// data from the znode
				client.create().creatingParentsIfNeeded().forPath(path, generateZKData(generateRandomSecret
					(), generateRandomSecret(), null));
				zkVersion = 0;
				LOG.info("Creating secret znode");
			}
			catch (org.apache.zookeeper.KeeperException.NodeExistsException)
			{
				LOG.info("The secret znode already exists, retrieving data");
			}
			// Synchronize on the data from the znode
			// passing true tells it to parse out all the data for initing
			pullFromZK(true);
			long initialDelay = nextRolloverDate - Sharpen.Runtime.currentTimeMillis();
			// If it's in the past, try to find the next interval that we should
			// be using
			if (initialDelay < 1l)
			{
				int i = 1;
				while (initialDelay < 1l)
				{
					initialDelay = nextRolloverDate + tokenValidity * i - Sharpen.Runtime.currentTimeMillis
						();
					i++;
				}
			}
			base.startScheduler(initialDelay, tokenValidity);
		}

		/// <summary>Disconnects from ZooKeeper unless told not to.</summary>
		public override void destroy()
		{
			if (shouldDisconnect && client != null)
			{
				client.close();
			}
			base.destroy();
		}

		protected internal override void rollSecret()
		{
			lock (this)
			{
				base.rollSecret();
				// Try to push the information to ZooKeeper with a potential next secret.
				nextRolloverDate += tokenValidity;
				byte[][] secrets = base.getAllSecrets();
				pushToZK(generateRandomSecret(), secrets[0], secrets[1]);
				// Pull info from ZooKeeper to get the decided next secret
				// passing false tells it that we don't care about most of the data
				pullFromZK(false);
			}
		}

		protected internal override byte[] generateNewSecret()
		{
			// We simply return nextSecret because it's already been decided on
			return nextSecret;
		}

		/// <summary>Pushes proposed data to ZooKeeper.</summary>
		/// <remarks>
		/// Pushes proposed data to ZooKeeper.  If a different server pushes its data
		/// first, it gives up.
		/// </remarks>
		/// <param name="newSecret">The new secret to use</param>
		/// <param name="currentSecret">The current secret</param>
		/// <param name="previousSecret">The previous secret</param>
		private void pushToZK(byte[] newSecret, byte[] currentSecret, byte[] previousSecret
			)
		{
			lock (this)
			{
				byte[] bytes = generateZKData(newSecret, currentSecret, previousSecret);
				try
				{
					client.setData().withVersion(zkVersion).forPath(path, bytes);
				}
				catch (org.apache.zookeeper.KeeperException.BadVersionException)
				{
					LOG.debug("Unable to push to znode; another server already did it");
				}
				catch (System.Exception ex)
				{
					LOG.error("An unexpected exception occured pushing data to ZooKeeper", ex);
				}
			}
		}

		/// <summary>Serialize the data to attempt to push into ZooKeeper.</summary>
		/// <remarks>
		/// Serialize the data to attempt to push into ZooKeeper.  The format is this:
		/// <p>
		/// [DATA_VERSION, newSecretLength, newSecret, currentSecretLength, currentSecret, previousSecretLength, previousSecret, nextRolloverDate]
		/// <p>
		/// Only previousSecret can be null, in which case the format looks like this:
		/// <p>
		/// [DATA_VERSION, newSecretLength, newSecret, currentSecretLength, currentSecret, 0, nextRolloverDate]
		/// <p>
		/// </remarks>
		/// <param name="newSecret">The new secret to use</param>
		/// <param name="currentSecret">The current secret</param>
		/// <param name="previousSecret">The previous secret</param>
		/// <returns>The serialized data for ZooKeeper</returns>
		private byte[] generateZKData(byte[] newSecret, byte[] currentSecret, byte[] previousSecret
			)
		{
			lock (this)
			{
				int newSecretLength = newSecret.Length;
				int currentSecretLength = currentSecret.Length;
				int previousSecretLength = 0;
				if (previousSecret != null)
				{
					previousSecretLength = previousSecret.Length;
				}
				java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(INT_BYTES + INT_BYTES + newSecretLength
					 + INT_BYTES + currentSecretLength + INT_BYTES + previousSecretLength + LONG_BYTES
					);
				bb.putInt(DATA_VERSION);
				bb.putInt(newSecretLength);
				bb.put(newSecret);
				bb.putInt(currentSecretLength);
				bb.put(currentSecret);
				bb.putInt(previousSecretLength);
				if (previousSecretLength > 0)
				{
					bb.put(previousSecret);
				}
				bb.putLong(nextRolloverDate);
				return ((byte[])bb.array());
			}
		}

		/// <summary>Pulls data from ZooKeeper.</summary>
		/// <remarks>
		/// Pulls data from ZooKeeper.  If isInit is false, it will only parse the
		/// next secret and version.  If isInit is true, it will also parse the current
		/// and previous secrets, and the next rollover date; it will also init the
		/// secrets.  Hence, isInit should only be true on startup.
		/// </remarks>
		/// <param name="isInit">see description above</param>
		private void pullFromZK(bool isInit)
		{
			lock (this)
			{
				try
				{
					org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
					byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
					java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes);
					int dataVersion = bb.getInt();
					if (dataVersion > DATA_VERSION)
					{
						throw new System.InvalidOperationException("Cannot load data from ZooKeeper; it" 
							+ "was written with a newer version");
					}
					int nextSecretLength = bb.getInt();
					byte[] nextSecret = new byte[nextSecretLength];
					bb.get(nextSecret);
					this.nextSecret = nextSecret;
					zkVersion = stat.getVersion();
					if (isInit)
					{
						int currentSecretLength = bb.getInt();
						byte[] currentSecret = new byte[currentSecretLength];
						bb.get(currentSecret);
						int previousSecretLength = bb.getInt();
						byte[] previousSecret = null;
						if (previousSecretLength > 0)
						{
							previousSecret = new byte[previousSecretLength];
							bb.get(previousSecret);
						}
						base.initSecrets(currentSecret, previousSecret);
						nextRolloverDate = bb.getLong();
					}
				}
				catch (System.Exception ex)
				{
					LOG.error("An unexpected exception occurred while pulling data from" + "ZooKeeper"
						, ex);
				}
			}
		}

		private byte[] generateRandomSecret()
		{
			return Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong())
				, java.nio.charset.Charset.forName("UTF-8"));
		}

		/// <summary>This method creates the Curator client and connects to ZooKeeper.</summary>
		/// <param name="config">configuration properties</param>
		/// <returns>A Curator client</returns>
		/// <exception cref="System.Exception"/>
		protected internal virtual org.apache.curator.framework.CuratorFramework createCuratorClient
			(java.util.Properties config)
		{
			string connectionString = config.getProperty(ZOOKEEPER_CONNECTION_STRING, "localhost:2181"
				);
			org.apache.curator.RetryPolicy retryPolicy = new org.apache.curator.retry.ExponentialBackoffRetry
				(1000, 3);
			org.apache.curator.framework.api.ACLProvider aclProvider;
			string authType = config.getProperty(ZOOKEEPER_AUTH_TYPE, "none");
			if (authType.Equals("sasl"))
			{
				LOG.info("Connecting to ZooKeeper with SASL/Kerberos" + "and using 'sasl' ACLs");
				string principal = setJaasConfiguration(config);
				Sharpen.Runtime.setProperty(org.apache.zookeeper.client.ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY
					, JAAS_LOGIN_ENTRY_NAME);
				Sharpen.Runtime.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider"
					);
				aclProvider = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider.SASLOwnerACLProvider
					(principal);
			}
			else
			{
				// "none"
				LOG.info("Connecting to ZooKeeper without authentication");
				aclProvider = new org.apache.curator.framework.imps.DefaultACLProvider();
			}
			// open to everyone
			org.apache.curator.framework.CuratorFramework cf = org.apache.curator.framework.CuratorFrameworkFactory
				.builder().connectString(connectionString).retryPolicy(retryPolicy).aclProvider(
				aclProvider).build();
			cf.start();
			return cf;
		}

		/// <exception cref="System.Exception"/>
		private string setJaasConfiguration(java.util.Properties config)
		{
			string keytabFile = config.getProperty(ZOOKEEPER_KERBEROS_KEYTAB).Trim();
			if (keytabFile == null || keytabFile.Length == 0)
			{
				throw new System.ArgumentException(ZOOKEEPER_KERBEROS_KEYTAB + " must be specified"
					);
			}
			string principal = config.getProperty(ZOOKEEPER_KERBEROS_PRINCIPAL).Trim();
			if (principal == null || principal.Length == 0)
			{
				throw new System.ArgumentException(ZOOKEEPER_KERBEROS_PRINCIPAL + " must be specified"
					);
			}
			// This is equivalent to writing a jaas.conf file and setting the system
			// property, "java.security.auth.login.config", to point to it
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider.JaasConfiguration
				 jConf = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider.JaasConfiguration
				(JAAS_LOGIN_ENTRY_NAME, principal, keytabFile);
			javax.security.auth.login.Configuration.setConfiguration(jConf);
			return principal.split("[/@]")[0];
		}

		/// <summary>
		/// Simple implementation of an
		/// <see cref="org.apache.curator.framework.api.ACLProvider"/>
		/// that simply returns an ACL
		/// that gives all permissions only to a single principal.
		/// </summary>
		private class SASLOwnerACLProvider : org.apache.curator.framework.api.ACLProvider
		{
			private readonly System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> 
				saslACL;

			private SASLOwnerACLProvider(string principal)
			{
				this.saslACL = java.util.Collections.singletonList(new org.apache.zookeeper.data.ACL
					(org.apache.zookeeper.ZooDefs.Perms.ALL, new org.apache.zookeeper.data.Id("sasl"
					, principal)));
			}

			public virtual System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> getDefaultAcl
				()
			{
				return saslACL;
			}

			public virtual System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> getAclForPath
				(string path)
			{
				return saslACL;
			}
		}

		/// <summary>Creates a programmatic version of a jaas.conf file.</summary>
		/// <remarks>
		/// Creates a programmatic version of a jaas.conf file. This can be used
		/// instead of writing a jaas.conf file and setting the system property,
		/// "java.security.auth.login.config", to point to that file. It is meant to be
		/// used for connecting to ZooKeeper.
		/// </remarks>
		public class JaasConfiguration : javax.security.auth.login.Configuration
		{
			private static javax.security.auth.login.AppConfigurationEntry[] entry;

			private string entryName;

			/// <summary>
			/// Add an entry to the jaas configuration with the passed in name,
			/// principal, and keytab.
			/// </summary>
			/// <remarks>
			/// Add an entry to the jaas configuration with the passed in name,
			/// principal, and keytab. The other necessary options will be set for you.
			/// </remarks>
			/// <param name="entryName">The name of the entry (e.g. "Client")</param>
			/// <param name="principal">The principal of the user</param>
			/// <param name="keytab">The location of the keytab</param>
			public JaasConfiguration(string entryName, string principal, string keytab)
			{
				this.entryName = entryName;
				System.Collections.Generic.IDictionary<string, string> options = new System.Collections.Generic.Dictionary
					<string, string>();
				options["keyTab"] = keytab;
				options["principal"] = principal;
				options["useKeyTab"] = "true";
				options["storeKey"] = "true";
				options["useTicketCache"] = "false";
				options["refreshKrb5Config"] = "true";
				string jaasEnvVar = Sharpen.Runtime.getenv("HADOOP_JAAS_DEBUG");
				if (jaasEnvVar != null && Sharpen.Runtime.equalsIgnoreCase("true", jaasEnvVar))
				{
					options["debug"] = "true";
				}
				entry = new javax.security.auth.login.AppConfigurationEntry[] { new javax.security.auth.login.AppConfigurationEntry
					(getKrb5LoginModuleName(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
					.REQUIRED, options) };
			}

			public override javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry
				(string name)
			{
				return (entryName.Equals(name)) ? entry : null;
			}

			private string getKrb5LoginModuleName()
			{
				string krb5LoginModuleName;
				if (Sharpen.Runtime.getProperty("java.vendor").contains("IBM"))
				{
					krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
				}
				else
				{
					krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
				}
				return krb5LoginModuleName;
			}
		}
	}
}
