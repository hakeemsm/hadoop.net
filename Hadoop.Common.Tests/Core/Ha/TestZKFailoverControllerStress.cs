using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>Stress test for ZKFailoverController.</summary>
	/// <remarks>
	/// Stress test for ZKFailoverController.
	/// Starts multiple ZKFCs for dummy services, and then performs many automatic
	/// failovers. While doing so, ensures that a fake "shared resource"
	/// (simulating the shared edits dir) is only owned by one service at a time.
	/// </remarks>
	public class TestZKFailoverControllerStress : ClientBaseWithFixes
	{
		private const int StressRuntimeSecs = 30;

		private const int ExtraTimeoutSecs = 10;

		private Configuration conf;

		private MiniZKFCCluster cluster;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupConfAndServices()
		{
			conf = new Configuration();
			conf.Set(ZKFailoverController.ZkQuorumKey, hostPort);
			this.cluster = new MiniZKFCCluster(conf, GetServer(serverFactory));
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void StopCluster()
		{
			if (cluster != null)
			{
				cluster.Stop();
			}
		}

		/// <summary>
		/// Simply fail back and forth between two services for the
		/// configured amount of time, via expiring their ZK sessions.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestExpireBackAndForth()
		{
			cluster.Start();
			long st = Time.Now();
			long runFor = StressRuntimeSecs * 1000;
			int i = 0;
			while (Time.Now() - st < runFor)
			{
				// flip flop the services back and forth
				int from = i % 2;
				int to = (i + 1) % 2;
				// Expire one service, it should fail over to the other
				Log.Info("Failing over via expiration from " + from + " to " + to);
				cluster.ExpireAndVerifyFailover(from, to);
				i++;
			}
		}

		/// <summary>Randomly expire the ZK sessions of the two ZKFCs.</summary>
		/// <remarks>
		/// Randomly expire the ZK sessions of the two ZKFCs. This differs
		/// from the above test in that it is not a controlled failover -
		/// we just do random expirations and expect neither one to ever
		/// generate fatal exceptions.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomExpirations()
		{
			cluster.Start();
			long st = Time.Now();
			long runFor = StressRuntimeSecs * 1000;
			Random r = new Random();
			while (Time.Now() - st < runFor)
			{
				cluster.GetTestContext().CheckException();
				int targetIdx = r.Next(2);
				ActiveStandbyElector target = cluster.GetElector(targetIdx);
				long sessId = target.GetZKSessionIdForTests();
				if (sessId != -1)
				{
					Log.Info(string.Format("Expiring session %x for svc %d", sessId, targetIdx));
					GetServer(serverFactory).CloseSession(sessId);
				}
				Sharpen.Thread.Sleep(r.Next(300));
			}
		}

		/// <summary>
		/// Have the services fail their health checks half the time,
		/// causing the master role to bounce back and forth in the
		/// cluster.
		/// </summary>
		/// <remarks>
		/// Have the services fail their health checks half the time,
		/// causing the master role to bounce back and forth in the
		/// cluster. Meanwhile, causes ZK to disconnect clients every
		/// 50ms, to trigger the retry code and failures to become active.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomHealthAndDisconnects()
		{
			long runFor = StressRuntimeSecs * 1000;
			Org.Mockito.Mockito.DoAnswer(new TestZKFailoverControllerStress.RandomlyThrow(0))
				.When(cluster.GetService(0).proxy).MonitorHealth();
			Org.Mockito.Mockito.DoAnswer(new TestZKFailoverControllerStress.RandomlyThrow(1))
				.When(cluster.GetService(1).proxy).MonitorHealth();
			conf.SetInt(CommonConfigurationKeys.HaFcElectorZkOpRetriesKey, 100);
			// Don't start until after the above mocking. Otherwise we can get
			// Mockito errors if the HM calls the proxy in the middle of
			// setting up the mock.
			cluster.Start();
			long st = Time.Now();
			while (Time.Now() - st < runFor)
			{
				cluster.GetTestContext().CheckException();
				serverFactory.CloseAll();
				Sharpen.Thread.Sleep(50);
			}
		}

		/// <summary>Randomly throw an exception half the time the method is called</summary>
		private class RandomlyThrow : Org.Mockito.Stubbing.Answer
		{
			private Random r = new Random();

			private readonly int svcIdx;

			public RandomlyThrow(int svcIdx)
			{
				this.svcIdx = svcIdx;
			}

			/// <exception cref="System.Exception"/>
			public virtual object Answer(InvocationOnMock invocation)
			{
				if (r.NextBoolean())
				{
					Log.Info("Throwing an exception for svc " + svcIdx);
					throw new HealthCheckFailedException("random failure");
				}
				return invocation.CallRealMethod();
			}
		}
	}
}
