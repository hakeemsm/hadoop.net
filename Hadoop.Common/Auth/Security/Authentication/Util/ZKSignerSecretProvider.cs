using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Javax.Security.Auth.Login;
using Javax.Servlet;
using Org.Apache.Curator;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Curator.Framework.Imps;
using Org.Apache.Curator.Retry;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Client;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
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
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationFilter"
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
	public class ZKSignerSecretProvider : RolloverSignerSecretProvider
	{
		private const string ConfigPrefix = "signer.secret.provider.zookeeper.";

		/// <summary>Constant for the property that specifies the ZooKeeper connection string.
		/// 	</summary>
		public const string ZookeeperConnectionString = ConfigPrefix + "connection.string";

		/// <summary>Constant for the property that specifies the ZooKeeper path.</summary>
		public const string ZookeeperPath = ConfigPrefix + "path";

		/// <summary>Constant for the property that specifies the auth type to use.</summary>
		/// <remarks>
		/// Constant for the property that specifies the auth type to use.  Supported
		/// values are "none" and "sasl".  The default value is "none".
		/// </remarks>
		public const string ZookeeperAuthType = ConfigPrefix + "auth.type";

		/// <summary>Constant for the property that specifies the Kerberos keytab file.</summary>
		public const string ZookeeperKerberosKeytab = ConfigPrefix + "kerberos.keytab";

		/// <summary>Constant for the property that specifies the Kerberos principal.</summary>
		public const string ZookeeperKerberosPrincipal = ConfigPrefix + "kerberos.principal";

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
		public const string DisconnectFromZookeeperOnShutdown = ConfigPrefix + "disconnect.on.shutdown";

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
		public const string ZookeeperSignerSecretProviderCuratorClientAttribute = ConfigPrefix
			 + "curator.client";

		private const string JaasLoginEntryName = "ZKSignerSecretProviderClient";

		private static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Security.Authentication.Util.ZKSignerSecretProvider
			));

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

		private readonly Random rand;

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

		private CuratorFramework client;

		private bool shouldDisconnect;

		private static int IntBytes = int.Size / byte.Size;

		private static int LongBytes = long.Size / byte.Size;

		private static int DataVersion = 0;

		public ZKSignerSecretProvider()
			: base()
		{
			rand = new Random();
		}

		/// <summary>
		/// This constructor lets you set the seed of the Random Number Generator and
		/// is meant for testing.
		/// </summary>
		/// <param name="seed">the seed for the random number generator</param>
		[VisibleForTesting]
		public ZKSignerSecretProvider(long seed)
			: base()
		{
			rand = new Random(seed);
		}

		/// <exception cref="System.Exception"/>
		public override void Init(Properties config, ServletContext servletContext, long 
			tokenValidity)
		{
			object curatorClientObj = servletContext.GetAttribute(ZookeeperSignerSecretProviderCuratorClientAttribute
				);
			if (curatorClientObj != null && curatorClientObj is CuratorFramework)
			{
				client = (CuratorFramework)curatorClientObj;
			}
			else
			{
				client = CreateCuratorClient(config);
				servletContext.SetAttribute(ZookeeperSignerSecretProviderCuratorClientAttribute, 
					client);
			}
			this.tokenValidity = tokenValidity;
			shouldDisconnect = System.Boolean.Parse(config.GetProperty(DisconnectFromZookeeperOnShutdown
				, "true"));
			path = config.GetProperty(ZookeeperPath);
			if (path == null)
			{
				throw new ArgumentException(ZookeeperPath + " must be specified");
			}
			try
			{
				nextRolloverDate = Runtime.CurrentTimeMillis() + tokenValidity;
				// everyone tries to do this, only one will succeed and only when the
				// znode doesn't already exist.  Everyone else will synchronize on the
				// data from the znode
				client.Create().CreatingParentsIfNeeded().ForPath(path, GenerateZKData(GenerateRandomSecret
					(), GenerateRandomSecret(), null));
				zkVersion = 0;
				Log.Info("Creating secret znode");
			}
			catch (KeeperException.NodeExistsException)
			{
				Log.Info("The secret znode already exists, retrieving data");
			}
			// Synchronize on the data from the znode
			// passing true tells it to parse out all the data for initing
			PullFromZK(true);
			long initialDelay = nextRolloverDate - Runtime.CurrentTimeMillis();
			// If it's in the past, try to find the next interval that we should
			// be using
			if (initialDelay < 1l)
			{
				int i = 1;
				while (initialDelay < 1l)
				{
					initialDelay = nextRolloverDate + tokenValidity * i - Runtime.CurrentTimeMillis();
					i++;
				}
			}
			base.StartScheduler(initialDelay, tokenValidity);
		}

		/// <summary>Disconnects from ZooKeeper unless told not to.</summary>
		public override void Destroy()
		{
			if (shouldDisconnect && client != null)
			{
				client.Close();
			}
			base.Destroy();
		}

		protected internal override void RollSecret()
		{
			lock (this)
			{
				base.RollSecret();
				// Try to push the information to ZooKeeper with a potential next secret.
				nextRolloverDate += tokenValidity;
				byte[][] secrets = base.GetAllSecrets();
				PushToZK(GenerateRandomSecret(), secrets[0], secrets[1]);
				// Pull info from ZooKeeper to get the decided next secret
				// passing false tells it that we don't care about most of the data
				PullFromZK(false);
			}
		}

		protected internal override byte[] GenerateNewSecret()
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
		private void PushToZK(byte[] newSecret, byte[] currentSecret, byte[] previousSecret
			)
		{
			lock (this)
			{
				byte[] bytes = GenerateZKData(newSecret, currentSecret, previousSecret);
				try
				{
					client.SetData().WithVersion(zkVersion).ForPath(path, bytes);
				}
				catch (KeeperException.BadVersionException)
				{
					Log.Debug("Unable to push to znode; another server already did it");
				}
				catch (Exception ex)
				{
					Log.Error("An unexpected exception occured pushing data to ZooKeeper", ex);
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
		private byte[] GenerateZKData(byte[] newSecret, byte[] currentSecret, byte[] previousSecret
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
				ByteBuffer bb = ByteBuffer.Allocate(IntBytes + IntBytes + newSecretLength + IntBytes
					 + currentSecretLength + IntBytes + previousSecretLength + LongBytes);
				bb.PutInt(DataVersion);
				bb.PutInt(newSecretLength);
				bb.Put(newSecret);
				bb.PutInt(currentSecretLength);
				bb.Put(currentSecret);
				bb.PutInt(previousSecretLength);
				if (previousSecretLength > 0)
				{
					bb.Put(previousSecret);
				}
				bb.PutLong(nextRolloverDate);
				return ((byte[])bb.Array());
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
		private void PullFromZK(bool isInit)
		{
			lock (this)
			{
				try
				{
					Stat stat = new Stat();
					byte[] bytes = client.GetData().StoringStatIn(stat).ForPath(path);
					ByteBuffer bb = ByteBuffer.Wrap(bytes);
					int dataVersion = bb.GetInt();
					if (dataVersion > DataVersion)
					{
						throw new InvalidOperationException("Cannot load data from ZooKeeper; it" + "was written with a newer version"
							);
					}
					int nextSecretLength = bb.GetInt();
					byte[] nextSecret = new byte[nextSecretLength];
					bb.Get(nextSecret);
					this.nextSecret = nextSecret;
					zkVersion = stat.GetVersion();
					if (isInit)
					{
						int currentSecretLength = bb.GetInt();
						byte[] currentSecret = new byte[currentSecretLength];
						bb.Get(currentSecret);
						int previousSecretLength = bb.GetInt();
						byte[] previousSecret = null;
						if (previousSecretLength > 0)
						{
							previousSecret = new byte[previousSecretLength];
							bb.Get(previousSecret);
						}
						base.InitSecrets(currentSecret, previousSecret);
						nextRolloverDate = bb.GetLong();
					}
				}
				catch (Exception ex)
				{
					Log.Error("An unexpected exception occurred while pulling data from" + "ZooKeeper"
						, ex);
				}
			}
		}

		private byte[] GenerateRandomSecret()
		{
			return Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong())
				, Sharpen.Extensions.GetEncoding("UTF-8"));
		}

		/// <summary>This method creates the Curator client and connects to ZooKeeper.</summary>
		/// <param name="config">configuration properties</param>
		/// <returns>A Curator client</returns>
		/// <exception cref="System.Exception"/>
		protected internal virtual CuratorFramework CreateCuratorClient(Properties config
			)
		{
			string connectionString = config.GetProperty(ZookeeperConnectionString, "localhost:2181"
				);
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			ACLProvider aclProvider;
			string authType = config.GetProperty(ZookeeperAuthType, "none");
			if (authType.Equals("sasl"))
			{
				Log.Info("Connecting to ZooKeeper with SASL/Kerberos" + "and using 'sasl' ACLs");
				string principal = SetJaasConfiguration(config);
				Runtime.SetProperty(ZooKeeperSaslClient.LoginContextNameKey, JaasLoginEntryName);
				Runtime.SetProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider"
					);
				aclProvider = new ZKSignerSecretProvider.SASLOwnerACLProvider(principal);
			}
			else
			{
				// "none"
				Log.Info("Connecting to ZooKeeper without authentication");
				aclProvider = new DefaultACLProvider();
			}
			// open to everyone
			CuratorFramework cf = CuratorFrameworkFactory.Builder().ConnectString(connectionString
				).RetryPolicy(retryPolicy).AclProvider(aclProvider).Build();
			cf.Start();
			return cf;
		}

		/// <exception cref="System.Exception"/>
		private string SetJaasConfiguration(Properties config)
		{
			string keytabFile = config.GetProperty(ZookeeperKerberosKeytab).Trim();
			if (keytabFile == null || keytabFile.Length == 0)
			{
				throw new ArgumentException(ZookeeperKerberosKeytab + " must be specified");
			}
			string principal = config.GetProperty(ZookeeperKerberosPrincipal).Trim();
			if (principal == null || principal.Length == 0)
			{
				throw new ArgumentException(ZookeeperKerberosPrincipal + " must be specified");
			}
			// This is equivalent to writing a jaas.conf file and setting the system
			// property, "java.security.auth.login.config", to point to it
			ZKSignerSecretProvider.JaasConfiguration jConf = new ZKSignerSecretProvider.JaasConfiguration
				(JaasLoginEntryName, principal, keytabFile);
			Configuration.SetConfiguration(jConf);
			return principal.Split("[/@]")[0];
		}

		/// <summary>
		/// Simple implementation of an
		/// <see cref="Org.Apache.Curator.Framework.Api.ACLProvider"/>
		/// that simply returns an ACL
		/// that gives all permissions only to a single principal.
		/// </summary>
		private class SASLOwnerACLProvider : ACLProvider
		{
			private readonly IList<ACL> saslACL;

			private SASLOwnerACLProvider(string principal)
			{
				this.saslACL = Sharpen.Collections.SingletonList(new ACL(ZooDefs.Perms.All, new ID
					("sasl", principal)));
			}

			public virtual IList<ACL> GetDefaultAcl()
			{
				return saslACL;
			}

			public virtual IList<ACL> GetAclForPath(string path)
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
		public class JaasConfiguration : Configuration
		{
			private static AppConfigurationEntry[] entry;

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
				IDictionary<string, string> options = new Dictionary<string, string>();
				options["keyTab"] = keytab;
				options["principal"] = principal;
				options["useKeyTab"] = "true";
				options["storeKey"] = "true";
				options["useTicketCache"] = "false";
				options["refreshKrb5Config"] = "true";
				string jaasEnvVar = Runtime.Getenv("HADOOP_JAAS_DEBUG");
				if (jaasEnvVar != null && Sharpen.Runtime.EqualsIgnoreCase("true", jaasEnvVar))
				{
					options["debug"] = "true";
				}
				entry = new AppConfigurationEntry[] { new AppConfigurationEntry(GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				return (entryName.Equals(name)) ? entry : null;
			}

			private string GetKrb5LoginModuleName()
			{
				string krb5LoginModuleName;
				if (Runtime.GetProperty("java.vendor").Contains("IBM"))
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
