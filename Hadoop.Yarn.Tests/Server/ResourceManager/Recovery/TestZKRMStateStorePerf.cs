using System;
using System.Collections;
using System.Collections.Generic;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class TestZKRMStateStorePerf : RMStateStoreTestBase, Tool
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestZKRMStateStore));

		internal readonly string version = "0.1";

		private int ZkPerfNumAppDefault = 1000;

		private int ZkPerfNumAppattemptPerApp = 10;

		private readonly long clusterTimeStamp = 1352994193343L;

		private static readonly string Usage = "Usage: " + typeof(TestZKRMStateStorePerf)
			.Name + " -appSize numberOfApplications" + " -appAttemptSize numberOfApplicationAttempts"
			 + " [-hostPort Host:Port]" + " [-workingZnode rootZnodeForTesting]\n";

		private YarnConfiguration conf = null;

		private string workingZnode = "/Test";

		private ZKRMStateStore store;

		private AMRMTokenSecretManager appTokenMgr;

		private ClientToAMTokenSecretManagerInRM clientToAMTokenMgr;

		// Configurable variables for performance test
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUpZKServer()
		{
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			if (store != null)
			{
				store.Stop();
			}
			if (appTokenMgr != null)
			{
				appTokenMgr.Stop();
			}
			base.TearDown();
		}

		private void InitStore(string hostPort)
		{
			Optional<string> optHostPort = Optional.FromNullable(hostPort);
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmZkAddress, optHostPort.Or(this.hostPort));
			conf.Set(YarnConfiguration.ZkRmStateStoreParentPath, workingZnode);
			store = new ZKRMStateStore();
			store.Init(conf);
			store.Start();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(store);
			appTokenMgr = new AMRMTokenSecretManager(conf, rmContext);
			appTokenMgr.Start();
			clientToAMTokenMgr = new ClientToAMTokenSecretManagerInRM();
		}

		public virtual int Run(string[] args)
		{
			Log.Info("Starting ZKRMStateStorePerf ver." + version);
			int numApp = ZkPerfNumAppDefault;
			int numAppAttemptPerApp = ZkPerfNumAppattemptPerApp;
			string hostPort = null;
			bool launchLocalZK = true;
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine("Missing arguments.");
				return -1;
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-appsize"))
				{
					numApp = System.Convert.ToInt32(args[++i]);
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-appattemptsize"))
					{
						numAppAttemptPerApp = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-hostPort"))
						{
							hostPort = args[++i];
							launchLocalZK = false;
						}
						else
						{
							if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-workingZnode"))
							{
								workingZnode = args[++i];
							}
							else
							{
								System.Console.Error.WriteLine("Illegal argument: " + args[i]);
								return -1;
							}
						}
					}
				}
			}
			if (launchLocalZK)
			{
				try
				{
					SetUp();
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("failed to setup. : " + e.Message);
					return -1;
				}
			}
			InitStore(hostPort);
			long submitTime = Runtime.CurrentTimeMillis();
			long startTime = Runtime.CurrentTimeMillis() + 1234;
			AList<ApplicationId> applicationIds = new AList<ApplicationId>();
			AList<RMApp> rmApps = new AList<RMApp>();
			AList<ApplicationAttemptId> attemptIds = new AList<ApplicationAttemptId>();
			Dictionary<ApplicationId, ICollection<ApplicationAttemptId>> appIdsToAttemptId = 
				new Dictionary<ApplicationId, ICollection<ApplicationAttemptId>>();
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			for (int i_1 = 0; i_1 < numApp; i_1++)
			{
				ApplicationId appId = ApplicationId.NewInstance(clusterTimeStamp, i_1);
				applicationIds.AddItem(appId);
				AList<ApplicationAttemptId> attemptIdsForThisApp = new AList<ApplicationAttemptId
					>();
				for (int j = 0; j < numAppAttemptPerApp; j++)
				{
					ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, j);
					attemptIdsForThisApp.AddItem(attemptId);
				}
				appIdsToAttemptId[appId] = new LinkedHashSet(attemptIdsForThisApp);
				Sharpen.Collections.AddAll(attemptIds, attemptIdsForThisApp);
			}
			foreach (ApplicationId appId_1 in applicationIds)
			{
				RMApp app = null;
				try
				{
					app = StoreApp(store, appId_1, submitTime, startTime);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("failed to create Application Znode. : " + e.Message
						);
					return -1;
				}
				WaitNotify(dispatcher);
				rmApps.AddItem(app);
			}
			foreach (ApplicationAttemptId attemptId_1 in attemptIds)
			{
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> tokenId = GenerateAMRMToken
					(attemptId_1, appTokenMgr);
				SecretKey clientTokenKey = clientToAMTokenMgr.CreateMasterKey(attemptId_1);
				try
				{
					StoreAttempt(store, attemptId_1, ContainerId.NewContainerId(attemptId_1, 0L).ToString
						(), tokenId, clientTokenKey, dispatcher);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("failed to create AppAttempt Znode. : " + e.Message
						);
					return -1;
				}
			}
			long storeStart = Runtime.CurrentTimeMillis();
			try
			{
				store.LoadState();
			}
			catch (Exception e)
			{
				System.Console.Error.WriteLine("failed to locaState from ZKRMStateStore. : " + e.
					Message);
				return -1;
			}
			long storeEnd = Runtime.CurrentTimeMillis();
			long loadTime = storeEnd - storeStart;
			string resultMsg = "ZKRMStateStore takes " + loadTime + " msec to loadState.";
			Log.Info(resultMsg);
			System.Console.Out.WriteLine(resultMsg);
			// cleanup
			try
			{
				foreach (RMApp app in rmApps)
				{
					ApplicationStateData appState = ApplicationStateData.NewInstance(app.GetSubmitTime
						(), app.GetStartTime(), app.GetApplicationSubmissionContext(), app.GetUser());
					ApplicationId appId = app.GetApplicationId();
					IDictionary m = Org.Mockito.Mockito.Mock<IDictionary>();
					Org.Mockito.Mockito.When(m.Keys).ThenReturn(appIdsToAttemptId[appId_1]);
					appState.attempts = m;
					store.RemoveApplicationStateInternal(appState);
				}
			}
			catch (Exception e)
			{
				System.Console.Error.WriteLine("failed to cleanup. : " + e.Message);
				return -1;
			}
			return 0;
		}

		public virtual void SetConf(Configuration conf)
		{
		}

		// currently this function is just ignored
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void PerfZKRMStateStore()
		{
			string[] args = new string[] { "-appSize", ZkPerfNumAppDefault.ToString(), "-appAttemptSize"
				, ZkPerfNumAppattemptPerApp.ToString() };
			Run(args);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestZKRMStateStorePerf perf = new TestZKRMStateStorePerf();
			int res = -1;
			try
			{
				res = ToolRunner.Run(perf, args);
			}
			catch (Exception e)
			{
				System.Console.Error.Write(StringUtils.StringifyException(e));
				res = -2;
			}
			if (res == -1)
			{
				System.Console.Error.Write(Usage);
			}
			System.Environment.Exit(res);
		}
	}
}
