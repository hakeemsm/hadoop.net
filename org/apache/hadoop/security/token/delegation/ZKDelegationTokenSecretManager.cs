using Sharpen;

namespace org.apache.hadoop.security.token.delegation
{
	/// <summary>
	/// An implementation of
	/// <see cref="AbstractDelegationTokenSecretManager{TokenIdent}"/>
	/// that
	/// persists TokenIdentifiers and DelegationKeys in Zookeeper. This class can
	/// be used by HA (Highly available) services that consists of multiple nodes.
	/// This class ensures that Identifiers and Keys are replicated to all nodes of
	/// the service.
	/// </summary>
	public abstract class ZKDelegationTokenSecretManager<TokenIdent> : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
		<TokenIdent>
		where TokenIdent : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
	{
		private const string ZK_CONF_PREFIX = "zk-dt-secret-manager.";

		public const string ZK_DTSM_ZK_NUM_RETRIES = ZK_CONF_PREFIX + "zkNumRetries";

		public const string ZK_DTSM_ZK_SESSION_TIMEOUT = ZK_CONF_PREFIX + "zkSessionTimeout";

		public const string ZK_DTSM_ZK_CONNECTION_TIMEOUT = ZK_CONF_PREFIX + "zkConnectionTimeout";

		public const string ZK_DTSM_ZK_SHUTDOWN_TIMEOUT = ZK_CONF_PREFIX + "zkShutdownTimeout";

		public const string ZK_DTSM_ZNODE_WORKING_PATH = ZK_CONF_PREFIX + "znodeWorkingPath";

		public const string ZK_DTSM_ZK_AUTH_TYPE = ZK_CONF_PREFIX + "zkAuthType";

		public const string ZK_DTSM_ZK_CONNECTION_STRING = ZK_CONF_PREFIX + "zkConnectionString";

		public const string ZK_DTSM_ZK_KERBEROS_KEYTAB = ZK_CONF_PREFIX + "kerberos.keytab";

		public const string ZK_DTSM_ZK_KERBEROS_PRINCIPAL = ZK_CONF_PREFIX + "kerberos.principal";

		public const int ZK_DTSM_ZK_NUM_RETRIES_DEFAULT = 3;

		public const int ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT = 10000;

		public const int ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT = 10000;

		public const int ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT = 10000;

		public const string ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT = "zkdtsm";

		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager
			)));

		private const string JAAS_LOGIN_ENTRY_NAME = "ZKDelegationTokenSecretManagerClient";

		private const string ZK_DTSM_NAMESPACE = "ZKDTSMRoot";

		private const string ZK_DTSM_SEQNUM_ROOT = "/ZKDTSMSeqNumRoot";

		private const string ZK_DTSM_KEYID_ROOT = "/ZKDTSMKeyIdRoot";

		private const string ZK_DTSM_TOKENS_ROOT = "/ZKDTSMTokensRoot";

		private const string ZK_DTSM_MASTER_KEY_ROOT = "/ZKDTSMMasterKeyRoot";

		private const string DELEGATION_KEY_PREFIX = "DK_";

		private const string DELEGATION_TOKEN_PREFIX = "DT_";

		private static readonly java.lang.ThreadLocal<org.apache.curator.framework.CuratorFramework
			> CURATOR_TL = new java.lang.ThreadLocal<org.apache.curator.framework.CuratorFramework
			>();

		public static void setCurator(org.apache.curator.framework.CuratorFramework curator
			)
		{
			CURATOR_TL.set(curator);
		}

		private readonly bool isExternalClient;

		private readonly org.apache.curator.framework.CuratorFramework zkClient;

		private org.apache.curator.framework.recipes.shared.SharedCount delTokSeqCounter;

		private org.apache.curator.framework.recipes.shared.SharedCount keyIdSeqCounter;

		private org.apache.curator.framework.recipes.cache.PathChildrenCache keyCache;

		private org.apache.curator.framework.recipes.cache.PathChildrenCache tokenCache;

		private java.util.concurrent.ExecutorService listenerThreadPool;

		private readonly long shutdownTimeout;

		public ZKDelegationTokenSecretManager(org.apache.hadoop.conf.Configuration conf)
			: base(conf.getLong(org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.UPDATE_INTERVAL, org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.UPDATE_INTERVAL_DEFAULT) * 1000, conf.getLong(org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.MAX_LIFETIME, org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.MAX_LIFETIME_DEFAULT) * 1000, conf.getLong(org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.RENEW_INTERVAL, org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.RENEW_INTERVAL_DEFAULT * 1000), conf.getLong(org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.REMOVAL_SCAN_INTERVAL, org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				.REMOVAL_SCAN_INTERVAL_DEFAULT) * 1000)
		{
			shutdownTimeout = conf.getLong(ZK_DTSM_ZK_SHUTDOWN_TIMEOUT, ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT
				);
			if (CURATOR_TL.get() != null)
			{
				zkClient = CURATOR_TL.get().usingNamespace(conf.get(ZK_DTSM_ZNODE_WORKING_PATH, ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT
					) + "/" + ZK_DTSM_NAMESPACE);
				isExternalClient = true;
			}
			else
			{
				string connString = conf.get(ZK_DTSM_ZK_CONNECTION_STRING);
				com.google.common.@base.Preconditions.checkNotNull(connString, "Zookeeper connection string cannot be null"
					);
				string authType = conf.get(ZK_DTSM_ZK_AUTH_TYPE);
				// AuthType has to be explicitly set to 'none' or 'sasl'
				com.google.common.@base.Preconditions.checkNotNull(authType, "Zookeeper authType cannot be null !!"
					);
				com.google.common.@base.Preconditions.checkArgument(authType.Equals("sasl") || authType
					.Equals("none"), "Zookeeper authType must be one of [none, sasl]");
				org.apache.curator.framework.CuratorFrameworkFactory.Builder builder = null;
				try
				{
					org.apache.curator.framework.api.ACLProvider aclProvider = null;
					if (authType.Equals("sasl"))
					{
						LOG.info("Connecting to ZooKeeper with SASL/Kerberos" + "and using 'sasl' ACLs");
						string principal = setJaasConfiguration(conf);
						Sharpen.Runtime.setProperty(org.apache.zookeeper.client.ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY
							, JAAS_LOGIN_ENTRY_NAME);
						Sharpen.Runtime.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider"
							);
						aclProvider = new org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.SASLOwnerACLProvider
							(principal);
					}
					else
					{
						// "none"
						LOG.info("Connecting to ZooKeeper without authentication");
						aclProvider = new org.apache.curator.framework.imps.DefaultACLProvider();
					}
					// open to everyone
					int sessionT = conf.getInt(ZK_DTSM_ZK_SESSION_TIMEOUT, ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT
						);
					int numRetries = conf.getInt(ZK_DTSM_ZK_NUM_RETRIES, ZK_DTSM_ZK_NUM_RETRIES_DEFAULT
						);
					builder = org.apache.curator.framework.CuratorFrameworkFactory.builder().aclProvider
						(aclProvider).@namespace(conf.get(ZK_DTSM_ZNODE_WORKING_PATH, ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT
						) + "/" + ZK_DTSM_NAMESPACE).sessionTimeoutMs(sessionT).connectionTimeoutMs(conf
						.getInt(ZK_DTSM_ZK_CONNECTION_TIMEOUT, ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT)).retryPolicy
						(new org.apache.curator.retry.RetryNTimes(numRetries, sessionT / numRetries));
				}
				catch (System.Exception)
				{
					throw new System.Exception("Could not Load ZK acls or auth");
				}
				zkClient = builder.ensembleProvider(new org.apache.curator.ensemble.@fixed.FixedEnsembleProvider
					(connString)).build();
				isExternalClient = false;
			}
		}

		/// <exception cref="System.Exception"/>
		private string setJaasConfiguration(org.apache.hadoop.conf.Configuration config)
		{
			string keytabFile = config.get(ZK_DTSM_ZK_KERBEROS_KEYTAB, string.Empty).Trim();
			if (keytabFile == null || keytabFile.Length == 0)
			{
				throw new System.ArgumentException(ZK_DTSM_ZK_KERBEROS_KEYTAB + " must be specified"
					);
			}
			string principal = config.get(ZK_DTSM_ZK_KERBEROS_PRINCIPAL, string.Empty).Trim();
			if (principal == null || principal.Length == 0)
			{
				throw new System.ArgumentException(ZK_DTSM_ZK_KERBEROS_PRINCIPAL + " must be specified"
					);
			}
			org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration
				 jConf = new org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration
				(JAAS_LOGIN_ENTRY_NAME, principal, keytabFile);
			javax.security.auth.login.Configuration.setConfiguration(jConf);
			return principal.split("[/@]")[0];
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

		/// <exception cref="System.IO.IOException"/>
		public override void startThreads()
		{
			if (!isExternalClient)
			{
				try
				{
					zkClient.start();
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException("Could not start Curator Framework", e);
				}
			}
			else
			{
				// If namespace parents are implicitly created, they won't have ACLs.
				// So, let's explicitly create them.
				org.apache.curator.framework.CuratorFramework nullNsFw = zkClient.usingNamespace(
					null);
				org.apache.curator.utils.EnsurePath ensureNs = nullNsFw.newNamespaceAwareEnsurePath
					("/" + zkClient.getNamespace());
				try
				{
					ensureNs.ensure(nullNsFw.getZookeeperClient());
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException("Could not create namespace", e);
				}
			}
			listenerThreadPool = java.util.concurrent.Executors.newSingleThreadExecutor();
			try
			{
				delTokSeqCounter = new org.apache.curator.framework.recipes.shared.SharedCount(zkClient
					, ZK_DTSM_SEQNUM_ROOT, 0);
				if (delTokSeqCounter != null)
				{
					delTokSeqCounter.start();
				}
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Could not start Sequence Counter", e);
			}
			try
			{
				keyIdSeqCounter = new org.apache.curator.framework.recipes.shared.SharedCount(zkClient
					, ZK_DTSM_KEYID_ROOT, 0);
				if (keyIdSeqCounter != null)
				{
					keyIdSeqCounter.start();
				}
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Could not start KeyId Counter", e);
			}
			try
			{
				createPersistentNode(ZK_DTSM_MASTER_KEY_ROOT);
				createPersistentNode(ZK_DTSM_TOKENS_ROOT);
			}
			catch (System.Exception)
			{
				throw new System.Exception("Could not create ZK paths");
			}
			try
			{
				keyCache = new org.apache.curator.framework.recipes.cache.PathChildrenCache(zkClient
					, ZK_DTSM_MASTER_KEY_ROOT, true);
				if (keyCache != null)
				{
					keyCache.start(org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
						.BUILD_INITIAL_CACHE);
					keyCache.getListenable().addListener(new _PathChildrenCacheListener_340(this), listenerThreadPool
						);
				}
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Could not start PathChildrenCache for keys", e);
			}
			try
			{
				tokenCache = new org.apache.curator.framework.recipes.cache.PathChildrenCache(zkClient
					, ZK_DTSM_TOKENS_ROOT, true);
				if (tokenCache != null)
				{
					tokenCache.start(org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
						.BUILD_INITIAL_CACHE);
					tokenCache.getListenable().addListener(new _PathChildrenCacheListener_368(this), 
						listenerThreadPool);
				}
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Could not start PathChildrenCache for tokens", e
					);
			}
			base.startThreads();
		}

		private sealed class _PathChildrenCacheListener_340 : org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
		{
			public _PathChildrenCacheListener_340(ZKDelegationTokenSecretManager<TokenIdent> 
				_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public void childEvent(org.apache.curator.framework.CuratorFramework client, org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
				 @event)
			{
				switch (@event.getType())
				{
					case org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED
						:
					{
						this._enclosing.processKeyAddOrUpdate(@event.getData().getData());
						break;
					}

					case org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_UPDATED
						:
					{
						this._enclosing.processKeyAddOrUpdate(@event.getData().getData());
						break;
					}

					case org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_REMOVED
						:
					{
						this._enclosing.processKeyRemoved(@event.getData().getPath());
						break;
					}

					default:
					{
						break;
					}
				}
			}

			private readonly ZKDelegationTokenSecretManager<TokenIdent> _enclosing;
		}

		private sealed class _PathChildrenCacheListener_368 : org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
		{
			public _PathChildrenCacheListener_368(ZKDelegationTokenSecretManager<TokenIdent> 
				_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public void childEvent(org.apache.curator.framework.CuratorFramework client, org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
				 @event)
			{
				switch (@event.getType())
				{
					case org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED
						:
					{
						this._enclosing.processTokenAddOrUpdate(@event.getData());
						break;
					}

					case org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_UPDATED
						:
					{
						this._enclosing.processTokenAddOrUpdate(@event.getData());
						break;
					}

					case org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_REMOVED
						:
					{
						this._enclosing.processTokenRemoved(@event.getData());
						break;
					}

					default:
					{
						break;
					}
				}
			}

			private readonly ZKDelegationTokenSecretManager<TokenIdent> _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void processKeyAddOrUpdate(byte[] data)
		{
			java.io.ByteArrayInputStream bin = new java.io.ByteArrayInputStream(data);
			java.io.DataInputStream din = new java.io.DataInputStream(bin);
			org.apache.hadoop.security.token.delegation.DelegationKey key = new org.apache.hadoop.security.token.delegation.DelegationKey
				();
			key.readFields(din);
			lock (this)
			{
				allKeys[key.getKeyId()] = key;
			}
		}

		private void processKeyRemoved(string path)
		{
			int i = path.LastIndexOf('/');
			if (i > 0)
			{
				string tokSeg = Sharpen.Runtime.substring(path, i + 1);
				int j = tokSeg.IndexOf('_');
				if (j > 0)
				{
					int keyId = System.Convert.ToInt32(Sharpen.Runtime.substring(tokSeg, j + 1));
					lock (this)
					{
						Sharpen.Collections.Remove(allKeys, keyId);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void processTokenAddOrUpdate(org.apache.curator.framework.recipes.cache.ChildData
			 data)
		{
			java.io.ByteArrayInputStream bin = new java.io.ByteArrayInputStream(data.getData(
				));
			java.io.DataInputStream din = new java.io.DataInputStream(bin);
			TokenIdent ident = createIdentifier();
			ident.readFields(din);
			long renewDate = din.readLong();
			int pwdLen = din.readInt();
			byte[] password = new byte[pwdLen];
			int numRead = din.read(password, 0, pwdLen);
			if (numRead > -1)
			{
				org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					 tokenInfo = new org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
					(renewDate, password);
				lock (this)
				{
					currentTokens[ident] = tokenInfo;
					// The cancel task might be waiting
					Sharpen.Runtime.notifyAll(this);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void processTokenRemoved(org.apache.curator.framework.recipes.cache.ChildData
			 data)
		{
			java.io.ByteArrayInputStream bin = new java.io.ByteArrayInputStream(data.getData(
				));
			java.io.DataInputStream din = new java.io.DataInputStream(bin);
			TokenIdent ident = createIdentifier();
			ident.readFields(din);
			lock (this)
			{
				Sharpen.Collections.Remove(currentTokens, ident);
				// The cancel task might be waiting
				Sharpen.Runtime.notifyAll(this);
			}
		}

		public override void stopThreads()
		{
			base.stopThreads();
			try
			{
				if (tokenCache != null)
				{
					tokenCache.close();
				}
			}
			catch (System.Exception e)
			{
				LOG.error("Could not stop Delegation Token Cache", e);
			}
			try
			{
				if (delTokSeqCounter != null)
				{
					delTokSeqCounter.close();
				}
			}
			catch (System.Exception e)
			{
				LOG.error("Could not stop Delegation Token Counter", e);
			}
			try
			{
				if (keyIdSeqCounter != null)
				{
					keyIdSeqCounter.close();
				}
			}
			catch (System.Exception e)
			{
				LOG.error("Could not stop Key Id Counter", e);
			}
			try
			{
				if (keyCache != null)
				{
					keyCache.close();
				}
			}
			catch (System.Exception e)
			{
				LOG.error("Could not stop KeyCache", e);
			}
			try
			{
				if (!isExternalClient && (zkClient != null))
				{
					zkClient.close();
				}
			}
			catch (System.Exception e)
			{
				LOG.error("Could not stop Curator Framework", e);
			}
			if (listenerThreadPool != null)
			{
				listenerThreadPool.shutdown();
				try
				{
					// wait for existing tasks to terminate
					if (!listenerThreadPool.awaitTermination(shutdownTimeout, java.util.concurrent.TimeUnit
						.MILLISECONDS))
					{
						LOG.error("Forcing Listener threadPool to shutdown !!");
						listenerThreadPool.shutdownNow();
					}
				}
				catch (System.Exception)
				{
					listenerThreadPool.shutdownNow();
					java.lang.Thread.currentThread().interrupt();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void createPersistentNode(string nodePath)
		{
			try
			{
				zkClient.create().withMode(org.apache.zookeeper.CreateMode.PERSISTENT).forPath(nodePath
					);
			}
			catch (org.apache.zookeeper.KeeperException.NodeExistsException)
			{
				LOG.debug(nodePath + " znode already exists !!");
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException(nodePath + " znode could not be created !!", e);
			}
		}

		protected internal override int getDelegationTokenSeqNum()
		{
			return delTokSeqCounter.getCount();
		}

		/// <exception cref="System.Exception"/>
		private void incrSharedCount(org.apache.curator.framework.recipes.shared.SharedCount
			 sharedCount)
		{
			while (true)
			{
				// Loop until we successfully increment the counter
				org.apache.curator.framework.recipes.shared.VersionedValue<int> versionedValue = 
					sharedCount.getVersionedValue();
				if (sharedCount.trySetCount(versionedValue, versionedValue.getValue() + 1))
				{
					break;
				}
			}
		}

		protected internal override int incrementDelegationTokenSeqNum()
		{
			try
			{
				incrSharedCount(delTokSeqCounter);
			}
			catch (System.Exception e)
			{
				// The ExpirationThread is just finishing.. so dont do anything..
				LOG.debug("Thread interrupted while performing token counter increment", e);
				java.lang.Thread.currentThread().interrupt();
			}
			catch (System.Exception e)
			{
				throw new System.Exception("Could not increment shared counter !!", e);
			}
			return delTokSeqCounter.getCount();
		}

		protected internal override void setDelegationTokenSeqNum(int seqNum)
		{
			try
			{
				delTokSeqCounter.setCount(seqNum);
			}
			catch (System.Exception e)
			{
				throw new System.Exception("Could not set shared counter !!", e);
			}
		}

		protected internal override int getCurrentKeyId()
		{
			return keyIdSeqCounter.getCount();
		}

		protected internal override int incrementCurrentKeyId()
		{
			try
			{
				incrSharedCount(keyIdSeqCounter);
			}
			catch (System.Exception e)
			{
				// The ExpirationThread is just finishing.. so dont do anything..
				LOG.debug("Thread interrupted while performing keyId increment", e);
				java.lang.Thread.currentThread().interrupt();
			}
			catch (System.Exception e)
			{
				throw new System.Exception("Could not increment shared keyId counter !!", e);
			}
			return keyIdSeqCounter.getCount();
		}

		protected internal override org.apache.hadoop.security.token.delegation.DelegationKey
			 getDelegationKey(int keyId)
		{
			// First check if its I already have this key
			org.apache.hadoop.security.token.delegation.DelegationKey key = allKeys[keyId];
			// Then query ZK
			if (key == null)
			{
				try
				{
					key = getKeyFromZK(keyId);
					if (key != null)
					{
						allKeys[keyId] = key;
					}
				}
				catch (System.IO.IOException e)
				{
					LOG.error("Error retrieving key [" + keyId + "] from ZK", e);
				}
			}
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.security.token.delegation.DelegationKey getKeyFromZK(int
			 keyId)
		{
			string nodePath = getNodePath(ZK_DTSM_MASTER_KEY_ROOT, DELEGATION_KEY_PREFIX + keyId
				);
			try
			{
				byte[] data = zkClient.getData().forPath(nodePath);
				if ((data == null) || (data.Length == 0))
				{
					return null;
				}
				java.io.ByteArrayInputStream bin = new java.io.ByteArrayInputStream(data);
				java.io.DataInputStream din = new java.io.DataInputStream(bin);
				org.apache.hadoop.security.token.delegation.DelegationKey key = new org.apache.hadoop.security.token.delegation.DelegationKey
					();
				key.readFields(din);
				return key;
			}
			catch (org.apache.zookeeper.KeeperException.NoNodeException)
			{
				LOG.error("No node in path [" + nodePath + "]");
			}
			catch (System.Exception ex)
			{
				throw new System.IO.IOException(ex);
			}
			return null;
		}

		protected internal override org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 getTokenInfo(TokenIdent ident)
		{
			// First check if I have this..
			org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
				 tokenInfo = currentTokens[ident];
			// Then query ZK
			if (tokenInfo == null)
			{
				try
				{
					tokenInfo = getTokenInfoFromZK(ident);
					if (tokenInfo != null)
					{
						currentTokens[ident] = tokenInfo;
					}
				}
				catch (System.IO.IOException e)
				{
					LOG.error("Error retrieving tokenInfo [" + ident.getSequenceNumber() + "] from ZK"
						, e);
				}
			}
			return tokenInfo;
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 getTokenInfoFromZK(TokenIdent ident)
		{
			return getTokenInfoFromZK(ident, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 getTokenInfoFromZK(TokenIdent ident, bool quiet)
		{
			string nodePath = getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX + ident
				.getSequenceNumber());
			try
			{
				byte[] data = zkClient.getData().forPath(nodePath);
				if ((data == null) || (data.Length == 0))
				{
					return null;
				}
				java.io.ByteArrayInputStream bin = new java.io.ByteArrayInputStream(data);
				java.io.DataInputStream din = new java.io.DataInputStream(bin);
				createIdentifier().readFields(din);
				long renewDate = din.readLong();
				int pwdLen = din.readInt();
				byte[] password = new byte[pwdLen];
				int numRead = din.read(password, 0, pwdLen);
				if (numRead > -1)
				{
					org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
						 tokenInfo = new org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
						(renewDate, password);
					return tokenInfo;
				}
			}
			catch (org.apache.zookeeper.KeeperException.NoNodeException)
			{
				if (!quiet)
				{
					LOG.error("No node in path [" + nodePath + "]");
				}
			}
			catch (System.Exception ex)
			{
				throw new System.IO.IOException(ex);
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void storeDelegationKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			addOrUpdateDelegationKey(key, false);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void updateDelegationKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			addOrUpdateDelegationKey(key, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void addOrUpdateDelegationKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key, bool isUpdate)
		{
			string nodeCreatePath = getNodePath(ZK_DTSM_MASTER_KEY_ROOT, DELEGATION_KEY_PREFIX
				 + key.getKeyId());
			java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
			java.io.DataOutputStream fsOut = new java.io.DataOutputStream(os);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Storing ZKDTSMDelegationKey_" + key.getKeyId());
			}
			key.write(fsOut);
			try
			{
				if (zkClient.checkExists().forPath(nodeCreatePath) != null)
				{
					zkClient.setData().forPath(nodeCreatePath, os.toByteArray()).setVersion(-1);
					if (!isUpdate)
					{
						LOG.debug("Key with path [" + nodeCreatePath + "] already exists.. Updating !!");
					}
				}
				else
				{
					zkClient.create().withMode(org.apache.zookeeper.CreateMode.PERSISTENT).forPath(nodeCreatePath
						, os.toByteArray());
					if (isUpdate)
					{
						LOG.debug("Updating non existent Key path [" + nodeCreatePath + "].. Adding new !!"
							);
					}
				}
			}
			catch (org.apache.zookeeper.KeeperException.NodeExistsException)
			{
				LOG.debug(nodeCreatePath + " znode already exists !!");
			}
			catch (System.Exception ex)
			{
				throw new System.IO.IOException(ex);
			}
			finally
			{
				os.close();
			}
		}

		protected internal override void removeStoredMasterKey(org.apache.hadoop.security.token.delegation.DelegationKey
			 key)
		{
			string nodeRemovePath = getNodePath(ZK_DTSM_MASTER_KEY_ROOT, DELEGATION_KEY_PREFIX
				 + key.getKeyId());
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Removing ZKDTSMDelegationKey_" + key.getKeyId());
			}
			try
			{
				if (zkClient.checkExists().forPath(nodeRemovePath) != null)
				{
					while (zkClient.checkExists().forPath(nodeRemovePath) != null)
					{
						zkClient.delete().guaranteed().forPath(nodeRemovePath);
					}
				}
				else
				{
					LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
				}
			}
			catch (System.Exception)
			{
				LOG.debug(nodeRemovePath + " znode could not be removed!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void storeToken(TokenIdent ident, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			try
			{
				addOrUpdateToken(ident, tokenInfo, false);
			}
			catch (System.Exception e)
			{
				throw new System.Exception(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void updateToken(TokenIdent ident, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			string nodeRemovePath = getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX 
				+ ident.getSequenceNumber());
			try
			{
				if (zkClient.checkExists().forPath(nodeRemovePath) != null)
				{
					addOrUpdateToken(ident, tokenInfo, true);
				}
				else
				{
					addOrUpdateToken(ident, tokenInfo, false);
					LOG.debug("Attempted to update a non-existing znode " + nodeRemovePath);
				}
			}
			catch (System.Exception e)
			{
				throw new System.Exception("Could not update Stored Token ZKDTSMDelegationToken_"
					 + ident.getSequenceNumber(), e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void removeStoredToken(TokenIdent ident)
		{
			string nodeRemovePath = getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX 
				+ ident.getSequenceNumber());
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Removing ZKDTSMDelegationToken_" + ident.getSequenceNumber());
			}
			try
			{
				if (zkClient.checkExists().forPath(nodeRemovePath) != null)
				{
					while (zkClient.checkExists().forPath(nodeRemovePath) != null)
					{
						zkClient.delete().guaranteed().forPath(nodeRemovePath);
					}
				}
				else
				{
					LOG.debug("Attempted to remove a non-existing znode " + nodeRemovePath);
				}
			}
			catch (System.Exception e)
			{
				throw new System.Exception("Could not remove Stored Token ZKDTSMDelegationToken_"
					 + ident.getSequenceNumber(), e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override TokenIdent cancelToken(org.apache.hadoop.security.token.Token<TokenIdent
			> token, string canceller)
		{
			lock (this)
			{
				java.io.ByteArrayInputStream buf = new java.io.ByteArrayInputStream(token.getIdentifier
					());
				java.io.DataInputStream @in = new java.io.DataInputStream(buf);
				TokenIdent id = createIdentifier();
				id.readFields(@in);
				try
				{
					if (!currentTokens.Contains(id))
					{
						// See if token can be retrieved and placed in currentTokens
						getTokenInfo(id);
					}
					return base.cancelToken(token, canceller);
				}
				catch (System.Exception e)
				{
					LOG.error("Exception while checking if token exist !!", e);
					return id;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void addOrUpdateToken(TokenIdent ident, org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 info, bool isUpdate)
		{
			string nodeCreatePath = getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX 
				+ ident.getSequenceNumber());
			java.io.ByteArrayOutputStream tokenOs = new java.io.ByteArrayOutputStream();
			java.io.DataOutputStream tokenOut = new java.io.DataOutputStream(tokenOs);
			java.io.ByteArrayOutputStream seqOs = new java.io.ByteArrayOutputStream();
			try
			{
				ident.write(tokenOut);
				tokenOut.writeLong(info.getRenewDate());
				tokenOut.writeInt(info.getPassword().Length);
				tokenOut.write(info.getPassword());
				if (LOG.isDebugEnabled())
				{
					LOG.debug((isUpdate ? "Updating " : "Storing ") + "ZKDTSMDelegationToken_" + ident
						.getSequenceNumber());
				}
				if (isUpdate)
				{
					zkClient.setData().forPath(nodeCreatePath, tokenOs.toByteArray()).setVersion(-1);
				}
				else
				{
					zkClient.create().withMode(org.apache.zookeeper.CreateMode.PERSISTENT).forPath(nodeCreatePath
						, tokenOs.toByteArray());
				}
			}
			finally
			{
				seqOs.close();
			}
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

		[com.google.common.annotations.VisibleForTesting]
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		internal static string getNodePath(string root, string nodeName)
		{
			return (root + "/" + nodeName);
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual java.util.concurrent.ExecutorService getListenerThreadPool()
		{
			return listenerThreadPool;
		}
	}
}
