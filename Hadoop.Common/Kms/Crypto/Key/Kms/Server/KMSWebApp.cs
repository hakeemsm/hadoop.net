using System;
using Com.Codahale.Metrics;
using Hadoop.Common.Core.Conf;
using Javax.Servlet;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Slf4j;
using Org.Slf4j.Bridge;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class KMSWebApp : ServletContextListener
	{
		private const string Log4jProperties = "kms-log4j.properties";

		private const string MetricsPrefix = "hadoop.kms.";

		private const string AdminCallsMeter = MetricsPrefix + "admin.calls.meter";

		private const string KeyCallsMeter = MetricsPrefix + "key.calls.meter";

		private const string InvalidCallsMeter = MetricsPrefix + "invalid.calls.meter";

		private const string UnauthorizedCallsMeter = MetricsPrefix + "unauthorized.calls.meter";

		private const string UnauthenticatedCallsMeter = MetricsPrefix + "unauthenticated.calls.meter";

		private const string GenerateEekMeter = MetricsPrefix + "generate_eek.calls.meter";

		private const string DecryptEekMeter = MetricsPrefix + "decrypt_eek.calls.meter";

		private static Logger Log;

		private static MetricRegistry metricRegistry;

		private JmxReporter jmxReporter;

		private static Configuration kmsConf;

		private static KMSACLs kmsAcls;

		private static Meter adminCallsMeter;

		private static Meter keyCallsMeter;

		private static Meter unauthorizedCallsMeter;

		private static Meter unauthenticatedCallsMeter;

		private static Meter decryptEEKCallsMeter;

		private static Meter generateEEKCallsMeter;

		private static Meter invalidCallsMeter;

		private static KMSAudit kmsAudit;

		private static KeyProviderCryptoExtension keyProviderCryptoExtension;

		static KMSWebApp()
		{
			SLF4JBridgeHandler.RemoveHandlersForRootLogger();
			SLF4JBridgeHandler.Install();
		}

		private void InitLogging(string confDir)
		{
			if (Runtime.GetProperty("log4j.configuration") == null)
			{
				Runtime.SetProperty("log4j.defaultInitOverride", "true");
				bool fromClasspath = true;
				FilePath log4jConf = new FilePath(confDir, Log4jProperties).GetAbsoluteFile();
				if (log4jConf.Exists())
				{
					PropertyConfigurator.ConfigureAndWatch(log4jConf.GetPath(), 1000);
					fromClasspath = false;
				}
				else
				{
					ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
					Uri log4jUrl = cl.GetResource(Log4jProperties);
					if (log4jUrl != null)
					{
						PropertyConfigurator.Configure(log4jUrl);
					}
				}
				Log = LoggerFactory.GetLogger(typeof(KMSWebApp));
				Log.Debug("KMS log starting");
				if (fromClasspath)
				{
					Log.Warn("Log4j configuration file '{}' not found", Log4jProperties);
					Log.Warn("Logging with INFO level to standard output");
				}
			}
			else
			{
				Log = LoggerFactory.GetLogger(typeof(KMSWebApp));
			}
		}

		public virtual void ContextInitialized(ServletContextEvent sce)
		{
			try
			{
				string confDir = Runtime.GetProperty(KMSConfiguration.KmsConfigDir);
				if (confDir == null)
				{
					throw new RuntimeException("System property '" + KMSConfiguration.KmsConfigDir + 
						"' not defined");
				}
				kmsConf = KMSConfiguration.GetKMSConf();
				InitLogging(confDir);
				Log.Info("-------------------------------------------------------------");
				Log.Info("  Java runtime version : {}", Runtime.GetProperty("java.runtime.version"
					));
				Log.Info("  KMS Hadoop Version: " + VersionInfo.GetVersion());
				Log.Info("-------------------------------------------------------------");
				kmsAcls = new KMSACLs();
				kmsAcls.StartReloader();
				metricRegistry = new MetricRegistry();
				jmxReporter = JmxReporter.ForRegistry(metricRegistry).Build();
				jmxReporter.Start();
				generateEEKCallsMeter = metricRegistry.Register(GenerateEekMeter, new Meter());
				decryptEEKCallsMeter = metricRegistry.Register(DecryptEekMeter, new Meter());
				adminCallsMeter = metricRegistry.Register(AdminCallsMeter, new Meter());
				keyCallsMeter = metricRegistry.Register(KeyCallsMeter, new Meter());
				invalidCallsMeter = metricRegistry.Register(InvalidCallsMeter, new Meter());
				unauthorizedCallsMeter = metricRegistry.Register(UnauthorizedCallsMeter, new Meter
					());
				unauthenticatedCallsMeter = metricRegistry.Register(UnauthenticatedCallsMeter, new 
					Meter());
				kmsAudit = new KMSAudit(kmsConf.GetLong(KMSConfiguration.KmsAuditAggregationWindow
					, KMSConfiguration.KmsAuditAggregationWindowDefault));
				// this is required for the the JMXJsonServlet to work properly.
				// the JMXJsonServlet is behind the authentication filter,
				// thus the '*' ACL.
				sce.GetServletContext().SetAttribute(HttpServer2.ConfContextAttribute, kmsConf);
				sce.GetServletContext().SetAttribute(HttpServer2.AdminsAcl, new AccessControlList
					(AccessControlList.WildcardAclValue));
				// intializing the KeyProvider
				string providerString = kmsConf.Get(KMSConfiguration.KeyProviderUri);
				if (providerString == null)
				{
					throw new InvalidOperationException("No KeyProvider has been defined");
				}
				KeyProvider keyProvider = KeyProviderFactory.Get(new URI(providerString), kmsConf
					);
				if (kmsConf.GetBoolean(KMSConfiguration.KeyCacheEnable, KMSConfiguration.KeyCacheEnableDefault
					))
				{
					long keyTimeOutMillis = kmsConf.GetLong(KMSConfiguration.KeyCacheTimeoutKey, KMSConfiguration
						.KeyCacheTimeoutDefault);
					long currKeyTimeOutMillis = kmsConf.GetLong(KMSConfiguration.CurrKeyCacheTimeoutKey
						, KMSConfiguration.CurrKeyCacheTimeoutDefault);
					keyProvider = new CachingKeyProvider(keyProvider, keyTimeOutMillis, currKeyTimeOutMillis
						);
				}
				Log.Info("Initialized KeyProvider " + keyProvider);
				keyProviderCryptoExtension = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
					(keyProvider);
				keyProviderCryptoExtension = new EagerKeyGeneratorKeyProviderCryptoExtension(kmsConf
					, keyProviderCryptoExtension);
				if (kmsConf.GetBoolean(KMSConfiguration.KeyAuthorizationEnable, KMSConfiguration.
					KeyAuthorizationEnableDefault))
				{
					keyProviderCryptoExtension = new KeyAuthorizationKeyProvider(keyProviderCryptoExtension
						, kmsAcls);
				}
				Log.Info("Initialized KeyProviderCryptoExtension " + keyProviderCryptoExtension);
				int defaultBitlength = kmsConf.GetInt(KeyProvider.DefaultBitlengthName, KeyProvider
					.DefaultBitlength);
				Log.Info("Default key bitlength is {}", defaultBitlength);
				Log.Info("KMS Started");
			}
			catch (Exception ex)
			{
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("ERROR: Hadoop KMS could not be started");
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("REASON: " + ex.ToString());
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("Stacktrace:");
				System.Console.Out.WriteLine("---------------------------------------------------"
					);
				Sharpen.Runtime.PrintStackTrace(ex, System.Console.Out);
				System.Console.Out.WriteLine("---------------------------------------------------"
					);
				System.Console.Out.WriteLine();
				System.Environment.Exit(1);
			}
		}

		public virtual void ContextDestroyed(ServletContextEvent sce)
		{
			kmsAudit.Shutdown();
			kmsAcls.StopReloader();
			jmxReporter.Stop();
			jmxReporter.Close();
			metricRegistry = null;
			Log.Info("KMS Stopped");
		}

		public static Configuration GetConfiguration()
		{
			return new Configuration(kmsConf);
		}

		public static KMSACLs GetACLs()
		{
			return kmsAcls;
		}

		public static Meter GetAdminCallsMeter()
		{
			return adminCallsMeter;
		}

		public static Meter GetKeyCallsMeter()
		{
			return keyCallsMeter;
		}

		public static Meter GetInvalidCallsMeter()
		{
			return invalidCallsMeter;
		}

		public static Meter GetGenerateEEKCallsMeter()
		{
			return generateEEKCallsMeter;
		}

		public static Meter GetDecryptEEKCallsMeter()
		{
			return decryptEEKCallsMeter;
		}

		public static Meter GetUnauthorizedCallsMeter()
		{
			return unauthorizedCallsMeter;
		}

		public static Meter GetUnauthenticatedCallsMeter()
		{
			return unauthenticatedCallsMeter;
		}

		public static KeyProviderCryptoExtension GetKeyProvider()
		{
			return keyProviderCryptoExtension;
		}

		public static KMSAudit GetKMSAudit()
		{
			return kmsAudit;
		}
	}
}
