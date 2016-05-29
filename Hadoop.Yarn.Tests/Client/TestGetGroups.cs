using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestGetGroups : GetGroupsTestBase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestGetGroups));

		private static ResourceManager resourceManager;

		private static Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpResourceManager()
		{
			conf = new YarnConfiguration();
			resourceManager = new _ResourceManager_47();
			resourceManager.Init(conf);
			new _Thread_53().Start();
			int waitCount = 0;
			while (resourceManager.GetServiceState() == Service.STATE.Inited && waitCount++ <
				 10)
			{
				Log.Info("Waiting for RM to start...");
				Sharpen.Thread.Sleep(1000);
			}
			if (resourceManager.GetServiceState() != Service.STATE.Started)
			{
				throw new IOException("ResourceManager failed to start. Final state is " + resourceManager
					.GetServiceState());
			}
			Log.Info("ResourceManager RMAdmin address: " + conf.Get(YarnConfiguration.RmAdminAddress
				));
		}

		private sealed class _ResourceManager_47 : ResourceManager
		{
			public _ResourceManager_47()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void DoSecureLogin()
			{
			}
		}

		private sealed class _Thread_53 : Sharpen.Thread
		{
			public _Thread_53()
			{
			}

			public override void Run()
			{
				TestGetGroups.resourceManager.Start();
			}
		}

		[SetUp]
		public virtual void SetUpConf()
		{
			base.conf = this.conf;
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownResourceManager()
		{
			if (resourceManager != null)
			{
				Log.Info("Stopping ResourceManager...");
				resourceManager.Stop();
			}
		}

		protected override Tool GetTool(TextWriter o)
		{
			return new GetGroupsForTesting(conf, o);
		}
	}
}
