using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// Provides access to the <code>AccessControlList</code>s used by KMS,
	/// hot-reloading them if the <code>kms-acls.xml</code> file where the ACLs
	/// are defined has been updated.
	/// </summary>
	public class KMSACLs : Runnable, KeyAuthorizationKeyProvider.KeyACLs
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Crypto.Key.Kms.Server.KMSACLs
			));

		private const string UnauthorizedMsgWithKey = "User:%s not allowed to do '%s' on '%s'";

		private const string UnauthorizedMsgWithoutKey = "User:%s not allowed to do '%s'";

		[System.Serializable]
		public sealed class Type
		{
			public static readonly KMSACLs.Type Create = new KMSACLs.Type();

			public static readonly KMSACLs.Type Delete = new KMSACLs.Type();

			public static readonly KMSACLs.Type Rollover = new KMSACLs.Type();

			public static readonly KMSACLs.Type Get = new KMSACLs.Type();

			public static readonly KMSACLs.Type GetKeys = new KMSACLs.Type();

			public static readonly KMSACLs.Type GetMetadata = new KMSACLs.Type();

			public static readonly KMSACLs.Type SetKeyMaterial = new KMSACLs.Type();

			public static readonly KMSACLs.Type GenerateEek = new KMSACLs.Type();

			public static readonly KMSACLs.Type DecryptEek = new KMSACLs.Type();

			public string GetAclConfigKey()
			{
				return KMSConfiguration.ConfigPrefix + "acl." + this.ToString();
			}

			public string GetBlacklistConfigKey()
			{
				return KMSConfiguration.ConfigPrefix + "blacklist." + this.ToString();
			}
		}

		public const string AclDefault = AccessControlList.WildcardAclValue;

		public const int ReloaderSleepMillis = 1000;

		private volatile IDictionary<KMSACLs.Type, AccessControlList> acls;

		private volatile IDictionary<KMSACLs.Type, AccessControlList> blacklistedAcls;

		[VisibleForTesting]
		internal volatile IDictionary<string, Dictionary<KeyAuthorizationKeyProvider.KeyOpType
			, AccessControlList>> keyAcls;

		private readonly IDictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList
			> defaultKeyAcls = new Dictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList
			>();

		private readonly IDictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList
			> whitelistKeyAcls = new Dictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList
			>();

		private ScheduledExecutorService executorService;

		private long lastReload;

		internal KMSACLs(Configuration conf)
		{
			if (conf == null)
			{
				conf = LoadACLs();
			}
			SetKMSACLs(conf);
			SetKeyACLs(conf);
		}

		public KMSACLs()
			: this(null)
		{
		}

		private void SetKMSACLs(Configuration conf)
		{
			IDictionary<KMSACLs.Type, AccessControlList> tempAcls = new Dictionary<KMSACLs.Type
				, AccessControlList>();
			IDictionary<KMSACLs.Type, AccessControlList> tempBlacklist = new Dictionary<KMSACLs.Type
				, AccessControlList>();
			foreach (KMSACLs.Type aclType in KMSACLs.Type.Values())
			{
				string aclStr = conf.Get(aclType.GetAclConfigKey(), AclDefault);
				tempAcls[aclType] = new AccessControlList(aclStr);
				string blacklistStr = conf.Get(aclType.GetBlacklistConfigKey());
				if (blacklistStr != null)
				{
					// Only add if blacklist is present
					tempBlacklist[aclType] = new AccessControlList(blacklistStr);
					Log.Info("'{}' Blacklist '{}'", aclType, blacklistStr);
				}
				Log.Info("'{}' ACL '{}'", aclType, aclStr);
			}
			acls = tempAcls;
			blacklistedAcls = tempBlacklist;
		}

		private void SetKeyACLs(Configuration conf)
		{
			IDictionary<string, Dictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList
				>> tempKeyAcls = new Dictionary<string, Dictionary<KeyAuthorizationKeyProvider.KeyOpType
				, AccessControlList>>();
			IDictionary<string, string> allKeyACLS = conf.GetValByRegex(KMSConfiguration.KeyAclPrefixRegex
				);
			foreach (KeyValuePair<string, string> keyAcl in allKeyACLS)
			{
				string k = keyAcl.Key;
				// this should be of type "key.acl.<KEY_NAME>.<OP_TYPE>"
				int keyNameStarts = KMSConfiguration.KeyAclPrefix.Length;
				int keyNameEnds = k.LastIndexOf(".");
				if (keyNameStarts >= keyNameEnds)
				{
					Log.Warn("Invalid key name '{}'", k);
				}
				else
				{
					string aclStr = keyAcl.Value;
					string keyName = Sharpen.Runtime.Substring(k, keyNameStarts, keyNameEnds);
					string keyOp = Sharpen.Runtime.Substring(k, keyNameEnds + 1);
					KeyAuthorizationKeyProvider.KeyOpType aclType = null;
					try
					{
						aclType = KeyAuthorizationKeyProvider.KeyOpType.ValueOf(keyOp);
					}
					catch (ArgumentException)
					{
						Log.Warn("Invalid key Operation '{}'", keyOp);
					}
					if (aclType != null)
					{
						// On the assumption this will be single threaded.. else we need to
						// ConcurrentHashMap
						Dictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList> aclMap = tempKeyAcls
							[keyName];
						if (aclMap == null)
						{
							aclMap = new Dictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList>
								();
							tempKeyAcls[keyName] = aclMap;
						}
						aclMap[aclType] = new AccessControlList(aclStr);
						Log.Info("KEY_NAME '{}' KEY_OP '{}' ACL '{}'", keyName, aclType, aclStr);
					}
				}
			}
			keyAcls = tempKeyAcls;
			foreach (KeyAuthorizationKeyProvider.KeyOpType keyOp_1 in KeyAuthorizationKeyProvider.KeyOpType
				.Values())
			{
				if (!defaultKeyAcls.Contains(keyOp_1))
				{
					string confKey = KMSConfiguration.DefaultKeyAclPrefix + keyOp_1;
					string aclStr = conf.Get(confKey);
					if (aclStr != null)
					{
						if (keyOp_1 == KeyAuthorizationKeyProvider.KeyOpType.All)
						{
							// Ignore All operation for default key acl
							Log.Warn("Should not configure default key ACL for KEY_OP '{}'", keyOp_1);
						}
						else
						{
							if (aclStr.Equals("*"))
							{
								Log.Info("Default Key ACL for KEY_OP '{}' is set to '*'", keyOp_1);
							}
							defaultKeyAcls[keyOp_1] = new AccessControlList(aclStr);
						}
					}
				}
				if (!whitelistKeyAcls.Contains(keyOp_1))
				{
					string confKey = KMSConfiguration.WhitelistKeyAclPrefix + keyOp_1;
					string aclStr = conf.Get(confKey);
					if (aclStr != null)
					{
						if (keyOp_1 == KeyAuthorizationKeyProvider.KeyOpType.All)
						{
							// Ignore All operation for whitelist key acl
							Log.Warn("Should not configure whitelist key ACL for KEY_OP '{}'", keyOp_1);
						}
						else
						{
							if (aclStr.Equals("*"))
							{
								Log.Info("Whitelist Key ACL for KEY_OP '{}' is set to '*'", keyOp_1);
							}
							whitelistKeyAcls[keyOp_1] = new AccessControlList(aclStr);
						}
					}
				}
			}
		}

		public virtual void Run()
		{
			try
			{
				if (KMSConfiguration.IsACLsFileNewer(lastReload))
				{
					SetKMSACLs(LoadACLs());
					SetKeyACLs(LoadACLs());
				}
			}
			catch (Exception ex)
			{
				Log.Warn(string.Format("Could not reload ACLs file: '%s'", ex.ToString()), ex);
			}
		}

		public virtual void StartReloader()
		{
			lock (this)
			{
				if (executorService == null)
				{
					executorService = Executors.NewScheduledThreadPool(1);
					executorService.ScheduleAtFixedRate(this, ReloaderSleepMillis, ReloaderSleepMillis
						, TimeUnit.Milliseconds);
				}
			}
		}

		public virtual void StopReloader()
		{
			lock (this)
			{
				if (executorService != null)
				{
					executorService.ShutdownNow();
					executorService = null;
				}
			}
		}

		private Configuration LoadACLs()
		{
			Log.Debug("Loading ACLs file");
			lastReload = Runtime.CurrentTimeMillis();
			Configuration conf = KMSConfiguration.GetACLsConf();
			// triggering the resource loading.
			conf.Get(KMSACLs.Type.Create.GetAclConfigKey());
			return conf;
		}

		/// <summary>
		/// First Check if user is in ACL for the KMS operation, if yes, then
		/// return true if user is not present in any configured blacklist for
		/// the operation
		/// </summary>
		/// <param name="type">KMS Operation</param>
		/// <param name="ugi">UserGroupInformation of user</param>
		/// <returns>true is user has access</returns>
		public virtual bool HasAccess(KMSACLs.Type type, UserGroupInformation ugi)
		{
			bool access = acls[type].IsUserAllowed(ugi);
			if (access)
			{
				AccessControlList blacklist = blacklistedAcls[type];
				access = (blacklist == null) || !blacklist.IsUserInList(ugi);
			}
			return access;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public virtual void AssertAccess(KMSACLs.Type aclType, UserGroupInformation ugi, 
			KMS.KMSOp operation, string key)
		{
			if (!KMSWebApp.GetACLs().HasAccess(aclType, ugi))
			{
				KMSWebApp.GetUnauthorizedCallsMeter().Mark();
				KMSWebApp.GetKMSAudit().Unauthorized(ugi, operation, key);
				throw new AuthorizationException(string.Format((key != null) ? UnauthorizedMsgWithKey
					 : UnauthorizedMsgWithoutKey, ugi.GetShortUserName(), operation, key));
			}
		}

		public virtual bool HasAccessToKey(string keyName, UserGroupInformation ugi, KeyAuthorizationKeyProvider.KeyOpType
			 opType)
		{
			return CheckKeyAccess(keyName, ugi, opType) || CheckKeyAccess(whitelistKeyAcls, ugi
				, opType);
		}

		private bool CheckKeyAccess(string keyName, UserGroupInformation ugi, KeyAuthorizationKeyProvider.KeyOpType
			 opType)
		{
			IDictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList> keyAcl = keyAcls
				[keyName];
			if (keyAcl == null)
			{
				// If No key acl defined for this key, check to see if
				// there are key defaults configured for this operation
				keyAcl = defaultKeyAcls;
			}
			return CheckKeyAccess(keyAcl, ugi, opType);
		}

		private bool CheckKeyAccess(IDictionary<KeyAuthorizationKeyProvider.KeyOpType, AccessControlList
			> keyAcl, UserGroupInformation ugi, KeyAuthorizationKeyProvider.KeyOpType opType
			)
		{
			AccessControlList acl = keyAcl[opType];
			if (acl == null)
			{
				// If no acl is specified for this operation,
				// deny access
				return false;
			}
			else
			{
				return acl.IsUserAllowed(ugi);
			}
		}

		public virtual bool IsACLPresent(string keyName, KeyAuthorizationKeyProvider.KeyOpType
			 opType)
		{
			return (keyAcls.Contains(keyName) || defaultKeyAcls.Contains(opType) || whitelistKeyAcls
				.Contains(opType));
		}
	}
}
