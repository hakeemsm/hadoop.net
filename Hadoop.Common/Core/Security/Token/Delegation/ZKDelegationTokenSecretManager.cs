using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Javax.Security.Auth.Login;
using Org.Apache.Curator.Ensemble.Fixed;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Curator.Framework.Imps;
using Org.Apache.Curator.Framework.Recipes.Cache;
using Org.Apache.Curator.Framework.Recipes.Shared;
using Org.Apache.Curator.Retry;
using Org.Apache.Curator.Utils;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Client;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation
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
	public abstract class ZKDelegationTokenSecretManager<TokenIdent> : AbstractDelegationTokenSecretManager
		<TokenIdent>
		where TokenIdent : AbstractDelegationTokenIdentifier
	{
		private const string ZkConfPrefix = "zk-dt-secret-manager.";

		public const string ZkDtsmZkNumRetries = ZkConfPrefix + "zkNumRetries";

		public const string ZkDtsmZkSessionTimeout = ZkConfPrefix + "zkSessionTimeout";

		public const string ZkDtsmZkConnectionTimeout = ZkConfPrefix + "zkConnectionTimeout";

		public const string ZkDtsmZkShutdownTimeout = ZkConfPrefix + "zkShutdownTimeout";

		public const string ZkDtsmZnodeWorkingPath = ZkConfPrefix + "znodeWorkingPath";

		public const string ZkDtsmZkAuthType = ZkConfPrefix + "zkAuthType";

		public const string ZkDtsmZkConnectionString = ZkConfPrefix + "zkConnectionString";

		public const string ZkDtsmZkKerberosKeytab = ZkConfPrefix + "kerberos.keytab";

		public const string ZkDtsmZkKerberosPrincipal = ZkConfPrefix + "kerberos.principal";

		public const int ZkDtsmZkNumRetriesDefault = 3;

		public const int ZkDtsmZkSessionTimeoutDefault = 10000;

		public const int ZkDtsmZkConnectionTimeoutDefault = 10000;

		public const int ZkDtsmZkShutdownTimeoutDefault = 10000;

		public const string ZkDtsmZnodeWorkingPathDeafult = "zkdtsm";

		private static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Security.Token.Delegation.ZKDelegationTokenSecretManager
			));

		private const string JaasLoginEntryName = "ZKDelegationTokenSecretManagerClient";

		private const string ZkDtsmNamespace = "ZKDTSMRoot";

		private const string ZkDtsmSeqnumRoot = "/ZKDTSMSeqNumRoot";

		private const string ZkDtsmKeyidRoot = "/ZKDTSMKeyIdRoot";

		private const string ZkDtsmTokensRoot = "/ZKDTSMTokensRoot";

		private const string ZkDtsmMasterKeyRoot = "/ZKDTSMMasterKeyRoot";

		private const string DelegationKeyPrefix = "DK_";

		private const string DelegationTokenPrefix = "DT_";

		private static readonly ThreadLocal<CuratorFramework> CuratorTl = new ThreadLocal
			<CuratorFramework>();

		public static void SetCurator(CuratorFramework curator)
		{
			CuratorTl.Set(curator);
		}

		private readonly bool isExternalClient;

		private readonly CuratorFramework zkClient;

		private SharedCount delTokSeqCounter;

		private SharedCount keyIdSeqCounter;

		private PathChildrenCache keyCache;

		private PathChildrenCache tokenCache;

		private ExecutorService listenerThreadPool;

		private readonly long shutdownTimeout;

		public ZKDelegationTokenSecretManager(Configuration conf)
			: base(conf.GetLong(DelegationTokenManager.UpdateInterval, DelegationTokenManager
				.UpdateIntervalDefault) * 1000, conf.GetLong(DelegationTokenManager.MaxLifetime, 
				DelegationTokenManager.MaxLifetimeDefault) * 1000, conf.GetLong(DelegationTokenManager
				.RenewInterval, DelegationTokenManager.RenewIntervalDefault * 1000), conf.GetLong
				(DelegationTokenManager.RemovalScanInterval, DelegationTokenManager.RemovalScanIntervalDefault
				) * 1000)
		{
			shutdownTimeout = conf.GetLong(ZkDtsmZkShutdownTimeout, ZkDtsmZkShutdownTimeoutDefault
				);
			if (CuratorTl.Get() != null)
			{
				zkClient = CuratorTl.Get().UsingNamespace(conf.Get(ZkDtsmZnodeWorkingPath, ZkDtsmZnodeWorkingPathDeafult
					) + "/" + ZkDtsmNamespace);
				isExternalClient = true;
			}
			else
			{
				string connString = conf.Get(ZkDtsmZkConnectionString);
				Preconditions.CheckNotNull(connString, "Zookeeper connection string cannot be null"
					);
				string authType = conf.Get(ZkDtsmZkAuthType);
				// AuthType has to be explicitly set to 'none' or 'sasl'
				Preconditions.CheckNotNull(authType, "Zookeeper authType cannot be null !!");
				Preconditions.CheckArgument(authType.Equals("sasl") || authType.Equals("none"), "Zookeeper authType must be one of [none, sasl]"
					);
				CuratorFrameworkFactory.Builder builder = null;
				try
				{
					ACLProvider aclProvider = null;
					if (authType.Equals("sasl"))
					{
						Log.Info("Connecting to ZooKeeper with SASL/Kerberos" + "and using 'sasl' ACLs");
						string principal = SetJaasConfiguration(conf);
						Runtime.SetProperty(ZooKeeperSaslClient.LoginContextNameKey, JaasLoginEntryName);
						Runtime.SetProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider"
							);
						aclProvider = new ZKDelegationTokenSecretManager.SASLOwnerACLProvider(principal);
					}
					else
					{
						// "none"
						Log.Info("Connecting to ZooKeeper without authentication");
						aclProvider = new DefaultACLProvider();
					}
					// open to everyone
					int sessionT = conf.GetInt(ZkDtsmZkSessionTimeout, ZkDtsmZkSessionTimeoutDefault);
					int numRetries = conf.GetInt(ZkDtsmZkNumRetries, ZkDtsmZkNumRetriesDefault);
					builder = CuratorFrameworkFactory.Builder().AclProvider(aclProvider).Namespace(conf
						.Get(ZkDtsmZnodeWorkingPath, ZkDtsmZnodeWorkingPathDeafult) + "/" + ZkDtsmNamespace
						).SessionTimeoutMs(sessionT).ConnectionTimeoutMs(conf.GetInt(ZkDtsmZkConnectionTimeout
						, ZkDtsmZkConnectionTimeoutDefault)).RetryPolicy(new RetryNTimes(numRetries, sessionT
						 / numRetries));
				}
				catch (Exception)
				{
					throw new RuntimeException("Could not Load ZK acls or auth");
				}
				zkClient = builder.EnsembleProvider(new FixedEnsembleProvider(connString)).Build(
					);
				isExternalClient = false;
			}
		}

		/// <exception cref="System.Exception"/>
		private string SetJaasConfiguration(Configuration config)
		{
			string keytabFile = config.Get(ZkDtsmZkKerberosKeytab, string.Empty).Trim();
			if (keytabFile == null || keytabFile.Length == 0)
			{
				throw new ArgumentException(ZkDtsmZkKerberosKeytab + " must be specified");
			}
			string principal = config.Get(ZkDtsmZkKerberosPrincipal, string.Empty).Trim();
			if (principal == null || principal.Length == 0)
			{
				throw new ArgumentException(ZkDtsmZkKerberosPrincipal + " must be specified");
			}
			ZKDelegationTokenSecretManager.JaasConfiguration jConf = new ZKDelegationTokenSecretManager.JaasConfiguration
				(JaasLoginEntryName, principal, keytabFile);
			Configuration.SetConfiguration(jConf);
			return principal.Split("[/@]")[0];
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

		/// <exception cref="System.IO.IOException"/>
		public override void StartThreads()
		{
			if (!isExternalClient)
			{
				try
				{
					zkClient.Start();
				}
				catch (Exception e)
				{
					throw new IOException("Could not start Curator Framework", e);
				}
			}
			else
			{
				// If namespace parents are implicitly created, they won't have ACLs.
				// So, let's explicitly create them.
				CuratorFramework nullNsFw = zkClient.UsingNamespace(null);
				EnsurePath ensureNs = nullNsFw.NewNamespaceAwareEnsurePath("/" + zkClient.GetNamespace
					());
				try
				{
					ensureNs.Ensure(nullNsFw.GetZookeeperClient());
				}
				catch (Exception e)
				{
					throw new IOException("Could not create namespace", e);
				}
			}
			listenerThreadPool = Executors.NewSingleThreadExecutor();
			try
			{
				delTokSeqCounter = new SharedCount(zkClient, ZkDtsmSeqnumRoot, 0);
				if (delTokSeqCounter != null)
				{
					delTokSeqCounter.Start();
				}
			}
			catch (Exception e)
			{
				throw new IOException("Could not start Sequence Counter", e);
			}
			try
			{
				keyIdSeqCounter = new SharedCount(zkClient, ZkDtsmKeyidRoot, 0);
				if (keyIdSeqCounter != null)
				{
					keyIdSeqCounter.Start();
				}
			}
			catch (Exception e)
			{
				throw new IOException("Could not start KeyId Counter", e);
			}
			try
			{
				CreatePersistentNode(ZkDtsmMasterKeyRoot);
				CreatePersistentNode(ZkDtsmTokensRoot);
			}
			catch (Exception)
			{
				throw new RuntimeException("Could not create ZK paths");
			}
			try
			{
				keyCache = new PathChildrenCache(zkClient, ZkDtsmMasterKeyRoot, true);
				if (keyCache != null)
				{
					keyCache.Start(PathChildrenCache.StartMode.BuildInitialCache);
					keyCache.GetListenable().AddListener(new _PathChildrenCacheListener_340(this), listenerThreadPool
						);
				}
			}
			catch (Exception e)
			{
				throw new IOException("Could not start PathChildrenCache for keys", e);
			}
			try
			{
				tokenCache = new PathChildrenCache(zkClient, ZkDtsmTokensRoot, true);
				if (tokenCache != null)
				{
					tokenCache.Start(PathChildrenCache.StartMode.BuildInitialCache);
					tokenCache.GetListenable().AddListener(new _PathChildrenCacheListener_368(this), 
						listenerThreadPool);
				}
			}
			catch (Exception e)
			{
				throw new IOException("Could not start PathChildrenCache for tokens", e);
			}
			base.StartThreads();
		}

		private sealed class _PathChildrenCacheListener_340 : PathChildrenCacheListener
		{
			public _PathChildrenCacheListener_340(ZKDelegationTokenSecretManager<TokenIdent> 
				_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public void ChildEvent(CuratorFramework client, PathChildrenCacheEvent @event)
			{
				switch (@event.GetType())
				{
					case PathChildrenCacheEvent.Type.ChildAdded:
					{
						this._enclosing.ProcessKeyAddOrUpdate(@event.GetData().GetData());
						break;
					}

					case PathChildrenCacheEvent.Type.ChildUpdated:
					{
						this._enclosing.ProcessKeyAddOrUpdate(@event.GetData().GetData());
						break;
					}

					case PathChildrenCacheEvent.Type.ChildRemoved:
					{
						this._enclosing.ProcessKeyRemoved(@event.GetData().GetPath());
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

		private sealed class _PathChildrenCacheListener_368 : PathChildrenCacheListener
		{
			public _PathChildrenCacheListener_368(ZKDelegationTokenSecretManager<TokenIdent> 
				_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public void ChildEvent(CuratorFramework client, PathChildrenCacheEvent @event)
			{
				switch (@event.GetType())
				{
					case PathChildrenCacheEvent.Type.ChildAdded:
					{
						this._enclosing.ProcessTokenAddOrUpdate(@event.GetData());
						break;
					}

					case PathChildrenCacheEvent.Type.ChildUpdated:
					{
						this._enclosing.ProcessTokenAddOrUpdate(@event.GetData());
						break;
					}

					case PathChildrenCacheEvent.Type.ChildRemoved:
					{
						this._enclosing.ProcessTokenRemoved(@event.GetData());
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
		private void ProcessKeyAddOrUpdate(byte[] data)
		{
			ByteArrayInputStream bin = new ByteArrayInputStream(data);
			DataInputStream din = new DataInputStream(bin);
			DelegationKey key = new DelegationKey();
			key.ReadFields(din);
			lock (this)
			{
				allKeys[key.GetKeyId()] = key;
			}
		}

		private void ProcessKeyRemoved(string path)
		{
			int i = path.LastIndexOf('/');
			if (i > 0)
			{
				string tokSeg = Sharpen.Runtime.Substring(path, i + 1);
				int j = tokSeg.IndexOf('_');
				if (j > 0)
				{
					int keyId = System.Convert.ToInt32(Sharpen.Runtime.Substring(tokSeg, j + 1));
					lock (this)
					{
						Sharpen.Collections.Remove(allKeys, keyId);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessTokenAddOrUpdate(ChildData data)
		{
			ByteArrayInputStream bin = new ByteArrayInputStream(data.GetData());
			DataInputStream din = new DataInputStream(bin);
			TokenIdent ident = CreateIdentifier();
			ident.ReadFields(din);
			long renewDate = din.ReadLong();
			int pwdLen = din.ReadInt();
			byte[] password = new byte[pwdLen];
			int numRead = din.Read(password, 0, pwdLen);
			if (numRead > -1)
			{
				AbstractDelegationTokenSecretManager.DelegationTokenInformation tokenInfo = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
					(renewDate, password);
				lock (this)
				{
					currentTokens[ident] = tokenInfo;
					// The cancel task might be waiting
					Sharpen.Runtime.NotifyAll(this);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessTokenRemoved(ChildData data)
		{
			ByteArrayInputStream bin = new ByteArrayInputStream(data.GetData());
			DataInputStream din = new DataInputStream(bin);
			TokenIdent ident = CreateIdentifier();
			ident.ReadFields(din);
			lock (this)
			{
				Sharpen.Collections.Remove(currentTokens, ident);
				// The cancel task might be waiting
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		public override void StopThreads()
		{
			base.StopThreads();
			try
			{
				if (tokenCache != null)
				{
					tokenCache.Close();
				}
			}
			catch (Exception e)
			{
				Log.Error("Could not stop Delegation Token Cache", e);
			}
			try
			{
				if (delTokSeqCounter != null)
				{
					delTokSeqCounter.Close();
				}
			}
			catch (Exception e)
			{
				Log.Error("Could not stop Delegation Token Counter", e);
			}
			try
			{
				if (keyIdSeqCounter != null)
				{
					keyIdSeqCounter.Close();
				}
			}
			catch (Exception e)
			{
				Log.Error("Could not stop Key Id Counter", e);
			}
			try
			{
				if (keyCache != null)
				{
					keyCache.Close();
				}
			}
			catch (Exception e)
			{
				Log.Error("Could not stop KeyCache", e);
			}
			try
			{
				if (!isExternalClient && (zkClient != null))
				{
					zkClient.Close();
				}
			}
			catch (Exception e)
			{
				Log.Error("Could not stop Curator Framework", e);
			}
			if (listenerThreadPool != null)
			{
				listenerThreadPool.Shutdown();
				try
				{
					// wait for existing tasks to terminate
					if (!listenerThreadPool.AwaitTermination(shutdownTimeout, TimeUnit.Milliseconds))
					{
						Log.Error("Forcing Listener threadPool to shutdown !!");
						listenerThreadPool.ShutdownNow();
					}
				}
				catch (Exception)
				{
					listenerThreadPool.ShutdownNow();
					Sharpen.Thread.CurrentThread().Interrupt();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void CreatePersistentNode(string nodePath)
		{
			try
			{
				zkClient.Create().WithMode(CreateMode.Persistent).ForPath(nodePath);
			}
			catch (KeeperException.NodeExistsException)
			{
				Log.Debug(nodePath + " znode already exists !!");
			}
			catch (Exception e)
			{
				throw new IOException(nodePath + " znode could not be created !!", e);
			}
		}

		protected internal override int GetDelegationTokenSeqNum()
		{
			return delTokSeqCounter.GetCount();
		}

		/// <exception cref="System.Exception"/>
		private void IncrSharedCount(SharedCount sharedCount)
		{
			while (true)
			{
				// Loop until we successfully increment the counter
				VersionedValue<int> versionedValue = sharedCount.GetVersionedValue();
				if (sharedCount.TrySetCount(versionedValue, versionedValue.GetValue() + 1))
				{
					break;
				}
			}
		}

		protected internal override int IncrementDelegationTokenSeqNum()
		{
			try
			{
				IncrSharedCount(delTokSeqCounter);
			}
			catch (Exception e)
			{
				// The ExpirationThread is just finishing.. so dont do anything..
				Log.Debug("Thread interrupted while performing token counter increment", e);
				Sharpen.Thread.CurrentThread().Interrupt();
			}
			catch (Exception e)
			{
				throw new RuntimeException("Could not increment shared counter !!", e);
			}
			return delTokSeqCounter.GetCount();
		}

		protected internal override void SetDelegationTokenSeqNum(int seqNum)
		{
			try
			{
				delTokSeqCounter.SetCount(seqNum);
			}
			catch (Exception e)
			{
				throw new RuntimeException("Could not set shared counter !!", e);
			}
		}

		protected internal override int GetCurrentKeyId()
		{
			return keyIdSeqCounter.GetCount();
		}

		protected internal override int IncrementCurrentKeyId()
		{
			try
			{
				IncrSharedCount(keyIdSeqCounter);
			}
			catch (Exception e)
			{
				// The ExpirationThread is just finishing.. so dont do anything..
				Log.Debug("Thread interrupted while performing keyId increment", e);
				Sharpen.Thread.CurrentThread().Interrupt();
			}
			catch (Exception e)
			{
				throw new RuntimeException("Could not increment shared keyId counter !!", e);
			}
			return keyIdSeqCounter.GetCount();
		}

		protected internal override DelegationKey GetDelegationKey(int keyId)
		{
			// First check if its I already have this key
			DelegationKey key = allKeys[keyId];
			// Then query ZK
			if (key == null)
			{
				try
				{
					key = GetKeyFromZK(keyId);
					if (key != null)
					{
						allKeys[keyId] = key;
					}
				}
				catch (IOException e)
				{
					Log.Error("Error retrieving key [" + keyId + "] from ZK", e);
				}
			}
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		private DelegationKey GetKeyFromZK(int keyId)
		{
			string nodePath = GetNodePath(ZkDtsmMasterKeyRoot, DelegationKeyPrefix + keyId);
			try
			{
				byte[] data = zkClient.GetData().ForPath(nodePath);
				if ((data == null) || (data.Length == 0))
				{
					return null;
				}
				ByteArrayInputStream bin = new ByteArrayInputStream(data);
				DataInputStream din = new DataInputStream(bin);
				DelegationKey key = new DelegationKey();
				key.ReadFields(din);
				return key;
			}
			catch (KeeperException.NoNodeException)
			{
				Log.Error("No node in path [" + nodePath + "]");
			}
			catch (Exception ex)
			{
				throw new IOException(ex);
			}
			return null;
		}

		protected internal override AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 GetTokenInfo(TokenIdent ident)
		{
			// First check if I have this..
			AbstractDelegationTokenSecretManager.DelegationTokenInformation tokenInfo = currentTokens
				[ident];
			// Then query ZK
			if (tokenInfo == null)
			{
				try
				{
					tokenInfo = GetTokenInfoFromZK(ident);
					if (tokenInfo != null)
					{
						currentTokens[ident] = tokenInfo;
					}
				}
				catch (IOException e)
				{
					Log.Error("Error retrieving tokenInfo [" + ident.GetSequenceNumber() + "] from ZK"
						, e);
				}
			}
			return tokenInfo;
		}

		/// <exception cref="System.IO.IOException"/>
		private AbstractDelegationTokenSecretManager.DelegationTokenInformation GetTokenInfoFromZK
			(TokenIdent ident)
		{
			return GetTokenInfoFromZK(ident, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private AbstractDelegationTokenSecretManager.DelegationTokenInformation GetTokenInfoFromZK
			(TokenIdent ident, bool quiet)
		{
			string nodePath = GetNodePath(ZkDtsmTokensRoot, DelegationTokenPrefix + ident.GetSequenceNumber
				());
			try
			{
				byte[] data = zkClient.GetData().ForPath(nodePath);
				if ((data == null) || (data.Length == 0))
				{
					return null;
				}
				ByteArrayInputStream bin = new ByteArrayInputStream(data);
				DataInputStream din = new DataInputStream(bin);
				CreateIdentifier().ReadFields(din);
				long renewDate = din.ReadLong();
				int pwdLen = din.ReadInt();
				byte[] password = new byte[pwdLen];
				int numRead = din.Read(password, 0, pwdLen);
				if (numRead > -1)
				{
					AbstractDelegationTokenSecretManager.DelegationTokenInformation tokenInfo = new AbstractDelegationTokenSecretManager.DelegationTokenInformation
						(renewDate, password);
					return tokenInfo;
				}
			}
			catch (KeeperException.NoNodeException)
			{
				if (!quiet)
				{
					Log.Error("No node in path [" + nodePath + "]");
				}
			}
			catch (Exception ex)
			{
				throw new IOException(ex);
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StoreDelegationKey(DelegationKey key)
		{
			AddOrUpdateDelegationKey(key, false);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void UpdateDelegationKey(DelegationKey key)
		{
			AddOrUpdateDelegationKey(key, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddOrUpdateDelegationKey(DelegationKey key, bool isUpdate)
		{
			string nodeCreatePath = GetNodePath(ZkDtsmMasterKeyRoot, DelegationKeyPrefix + key
				.GetKeyId());
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			DataOutputStream fsOut = new DataOutputStream(os);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing ZKDTSMDelegationKey_" + key.GetKeyId());
			}
			key.Write(fsOut);
			try
			{
				if (zkClient.CheckExists().ForPath(nodeCreatePath) != null)
				{
					zkClient.SetData().ForPath(nodeCreatePath, os.ToByteArray()).SetVersion(-1);
					if (!isUpdate)
					{
						Log.Debug("Key with path [" + nodeCreatePath + "] already exists.. Updating !!");
					}
				}
				else
				{
					zkClient.Create().WithMode(CreateMode.Persistent).ForPath(nodeCreatePath, os.ToByteArray
						());
					if (isUpdate)
					{
						Log.Debug("Updating non existent Key path [" + nodeCreatePath + "].. Adding new !!"
							);
					}
				}
			}
			catch (KeeperException.NodeExistsException)
			{
				Log.Debug(nodeCreatePath + " znode already exists !!");
			}
			catch (Exception ex)
			{
				throw new IOException(ex);
			}
			finally
			{
				os.Close();
			}
		}

		protected internal override void RemoveStoredMasterKey(DelegationKey key)
		{
			string nodeRemovePath = GetNodePath(ZkDtsmMasterKeyRoot, DelegationKeyPrefix + key
				.GetKeyId());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing ZKDTSMDelegationKey_" + key.GetKeyId());
			}
			try
			{
				if (zkClient.CheckExists().ForPath(nodeRemovePath) != null)
				{
					while (zkClient.CheckExists().ForPath(nodeRemovePath) != null)
					{
						zkClient.Delete().Guaranteed().ForPath(nodeRemovePath);
					}
				}
				else
				{
					Log.Debug("Attempted to delete a non-existing znode " + nodeRemovePath);
				}
			}
			catch (Exception)
			{
				Log.Debug(nodeRemovePath + " znode could not be removed!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StoreToken(TokenIdent ident, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			try
			{
				AddOrUpdateToken(ident, tokenInfo, false);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void UpdateToken(TokenIdent ident, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 tokenInfo)
		{
			string nodeRemovePath = GetNodePath(ZkDtsmTokensRoot, DelegationTokenPrefix + ident
				.GetSequenceNumber());
			try
			{
				if (zkClient.CheckExists().ForPath(nodeRemovePath) != null)
				{
					AddOrUpdateToken(ident, tokenInfo, true);
				}
				else
				{
					AddOrUpdateToken(ident, tokenInfo, false);
					Log.Debug("Attempted to update a non-existing znode " + nodeRemovePath);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException("Could not update Stored Token ZKDTSMDelegationToken_"
					 + ident.GetSequenceNumber(), e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void RemoveStoredToken(TokenIdent ident)
		{
			string nodeRemovePath = GetNodePath(ZkDtsmTokensRoot, DelegationTokenPrefix + ident
				.GetSequenceNumber());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing ZKDTSMDelegationToken_" + ident.GetSequenceNumber());
			}
			try
			{
				if (zkClient.CheckExists().ForPath(nodeRemovePath) != null)
				{
					while (zkClient.CheckExists().ForPath(nodeRemovePath) != null)
					{
						zkClient.Delete().Guaranteed().ForPath(nodeRemovePath);
					}
				}
				else
				{
					Log.Debug("Attempted to remove a non-existing znode " + nodeRemovePath);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException("Could not remove Stored Token ZKDTSMDelegationToken_"
					 + ident.GetSequenceNumber(), e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override TokenIdent CancelToken(Org.Apache.Hadoop.Security.Token.Token<TokenIdent
			> token, string canceller)
		{
			lock (this)
			{
				ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
				DataInputStream @in = new DataInputStream(buf);
				TokenIdent id = CreateIdentifier();
				id.ReadFields(@in);
				try
				{
					if (!currentTokens.Contains(id))
					{
						// See if token can be retrieved and placed in currentTokens
						GetTokenInfo(id);
					}
					return base.CancelToken(token, canceller);
				}
				catch (Exception e)
				{
					Log.Error("Exception while checking if token exist !!", e);
					return id;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void AddOrUpdateToken(TokenIdent ident, AbstractDelegationTokenSecretManager.DelegationTokenInformation
			 info, bool isUpdate)
		{
			string nodeCreatePath = GetNodePath(ZkDtsmTokensRoot, DelegationTokenPrefix + ident
				.GetSequenceNumber());
			ByteArrayOutputStream tokenOs = new ByteArrayOutputStream();
			DataOutputStream tokenOut = new DataOutputStream(tokenOs);
			ByteArrayOutputStream seqOs = new ByteArrayOutputStream();
			try
			{
				ident.Write(tokenOut);
				tokenOut.WriteLong(info.GetRenewDate());
				tokenOut.WriteInt(info.GetPassword().Length);
				tokenOut.Write(info.GetPassword());
				if (Log.IsDebugEnabled())
				{
					Log.Debug((isUpdate ? "Updating " : "Storing ") + "ZKDTSMDelegationToken_" + ident
						.GetSequenceNumber());
				}
				if (isUpdate)
				{
					zkClient.SetData().ForPath(nodeCreatePath, tokenOs.ToByteArray()).SetVersion(-1);
				}
				else
				{
					zkClient.Create().WithMode(CreateMode.Persistent).ForPath(nodeCreatePath, tokenOs
						.ToByteArray());
				}
			}
			finally
			{
				seqOs.Close();
			}
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

		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal static string GetNodePath(string root, string nodeName)
		{
			return (root + "/" + nodeName);
		}

		[VisibleForTesting]
		public virtual ExecutorService GetListenerThreadPool()
		{
			return listenerThreadPool;
		}
	}
}
