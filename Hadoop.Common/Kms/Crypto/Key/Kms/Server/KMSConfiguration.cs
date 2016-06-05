using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>Utility class to load KMS configuration files.</summary>
	public class KMSConfiguration
	{
		public const string KmsConfigDir = "kms.config.dir";

		public const string KmsSiteXml = "kms-site.xml";

		public const string KmsAclsXml = "kms-acls.xml";

		public const string ConfigPrefix = "hadoop.kms.";

		public const string KeyAclPrefix = "key.acl.";

		public const string KeyAclPrefixRegex = "^key\\.acl\\..+";

		public const string DefaultKeyAclPrefix = "default.key.acl.";

		public const string WhitelistKeyAclPrefix = "whitelist.key.acl.";

		public const string KeyProviderUri = ConfigPrefix + "key.provider.uri";

		public const string KeyCacheEnable = ConfigPrefix + "cache.enable";

		public const string KeyCacheTimeoutKey = ConfigPrefix + "cache.timeout.ms";

		public const string CurrKeyCacheTimeoutKey = ConfigPrefix + "current.key.cache.timeout.ms";

		public const string KmsAuditAggregationWindow = ConfigPrefix + "audit.aggregation.window.ms";

		public const bool KeyCacheEnableDefault = true;

		public const long KeyCacheTimeoutDefault = 10 * 60 * 1000;

		public const long CurrKeyCacheTimeoutDefault = 30 * 1000;

		public const long KmsAuditAggregationWindowDefault = 10000;

		public const string KeyAuthorizationEnable = ConfigPrefix + "key.authorization.enable";

		public const bool KeyAuthorizationEnableDefault = true;

		// Property to set the backing KeyProvider
		// Property to Enable/Disable Caching
		// Timeout for the Key and Metadata Cache
		// TImeout for the Current Key cache
		// Delay for Audit logs that need aggregation
		// 10 mins
		// 30 secs
		// 10 secs
		// Property to Enable/Disable per Key authorization
		internal static Configuration GetConfiguration(bool loadHadoopDefaults, params string
			[] resources)
		{
			Configuration conf = new Configuration(loadHadoopDefaults);
			string confDir = Runtime.GetProperty(KmsConfigDir);
			if (confDir != null)
			{
				try
				{
					Path confPath = new Path(confDir);
					if (!confPath.IsUriPathAbsolute())
					{
						throw new RuntimeException("System property '" + KmsConfigDir + "' must be an absolute path: "
							 + confDir);
					}
					foreach (string resource in resources)
					{
						conf.AddResource(new Uri("file://" + new Path(confDir, resource).ToUri()));
					}
				}
				catch (UriFormatException ex)
				{
					throw new RuntimeException(ex);
				}
			}
			else
			{
				foreach (string resource in resources)
				{
					conf.AddResource(resource);
				}
			}
			return conf;
		}

		public static Configuration GetKMSConf()
		{
			return GetConfiguration(true, "core-site.xml", KmsSiteXml);
		}

		public static Configuration GetACLsConf()
		{
			return GetConfiguration(false, KmsAclsXml);
		}

		public static bool IsACLsFileNewer(long time)
		{
			bool newer = false;
			string confDir = Runtime.GetProperty(KmsConfigDir);
			if (confDir != null)
			{
				Path confPath = new Path(confDir);
				if (!confPath.IsUriPathAbsolute())
				{
					throw new RuntimeException("System property '" + KmsConfigDir + "' must be an absolute path: "
						 + confDir);
				}
				FilePath f = new FilePath(confDir, KmsAclsXml);
				// at least 100ms newer than time, we do this to ensure the file
				// has been properly closed/flushed
				newer = f.LastModified() - time > 100;
			}
			return newer;
		}
	}
}
