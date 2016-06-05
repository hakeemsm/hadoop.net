using System;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Test;
using Org.Apache.Zookeeper.Data;
using Org.Apache.Zookeeper.Server;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Harness for starting two dummy ZK FailoverControllers, associated with
	/// DummyHAServices.
	/// </summary>
	/// <remarks>
	/// Harness for starting two dummy ZK FailoverControllers, associated with
	/// DummyHAServices. This harness starts two such ZKFCs, designated by
	/// indexes 0 and 1, and provides utilities for building tests around them.
	/// </remarks>
	public class MiniZKFCCluster
	{
		private readonly MultithreadedTestUtil.TestContext ctx;

		private readonly ZooKeeperServer zks;

		private DummyHAService[] svcs;

		private MiniZKFCCluster.DummyZKFCThread[] thrs;

		private Configuration conf;

		private DummySharedResource sharedResource = new DummySharedResource();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.MiniZKFCCluster
			));

		public MiniZKFCCluster(Configuration conf, ZooKeeperServer zks)
		{
			this.conf = conf;
			// Fast check interval so tests run faster
			conf.SetInt(CommonConfigurationKeys.HaHmCheckIntervalKey, 50);
			conf.SetInt(CommonConfigurationKeys.HaHmConnectRetryIntervalKey, 50);
			conf.SetInt(CommonConfigurationKeys.HaHmSleepAfterDisconnectKey, 50);
			svcs = new DummyHAService[2];
			svcs[0] = new DummyHAService(HAServiceProtocol.HAServiceState.Initializing, new IPEndPoint
				("svc1", 1234));
			svcs[0].SetSharedResource(sharedResource);
			svcs[1] = new DummyHAService(HAServiceProtocol.HAServiceState.Initializing, new IPEndPoint
				("svc2", 1234));
			svcs[1].SetSharedResource(sharedResource);
			this.ctx = new MultithreadedTestUtil.TestContext();
			this.zks = zks;
		}

		/// <summary>Set up two services and their failover controllers.</summary>
		/// <remarks>
		/// Set up two services and their failover controllers. svc1 is started
		/// first, so that it enters ACTIVE state, and then svc2 is started,
		/// which enters STANDBY
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void Start()
		{
			// Format the base dir, should succeed
			thrs = new MiniZKFCCluster.DummyZKFCThread[2];
			thrs[0] = new MiniZKFCCluster.DummyZKFCThread(this, ctx, svcs[0]);
			Assert.Equal(0, thrs[0].zkfc.Run(new string[] { "-formatZK" })
				);
			ctx.AddThread(thrs[0]);
			thrs[0].Start();
			Log.Info("Waiting for svc0 to enter active state");
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
			Log.Info("Adding svc1");
			thrs[1] = new MiniZKFCCluster.DummyZKFCThread(this, ctx, svcs[1]);
			thrs[1].Start();
			WaitForHAState(1, HAServiceProtocol.HAServiceState.Standby);
		}

		/// <summary>Stop the services.</summary>
		/// <exception cref="System.Exception">if either of the services had encountered a fatal error
		/// 	</exception>
		public virtual void Stop()
		{
			foreach (MiniZKFCCluster.DummyZKFCThread thr in thrs)
			{
				if (thr != null)
				{
					thr.Interrupt();
				}
			}
			if (ctx != null)
			{
				ctx.Stop();
			}
			sharedResource.AssertNoViolations();
		}

		/// <returns>
		/// the TestContext implementation used internally. This allows more
		/// threads to be added to the context, etc.
		/// </returns>
		public virtual MultithreadedTestUtil.TestContext GetTestContext()
		{
			return ctx;
		}

		public virtual DummyHAService GetService(int i)
		{
			return svcs[i];
		}

		public virtual ActiveStandbyElector GetElector(int i)
		{
			return thrs[i].zkfc.GetElectorForTests();
		}

		public virtual MiniZKFCCluster.DummyZKFC GetZkfc(int i)
		{
			return thrs[i].zkfc;
		}

		public virtual void SetHealthy(int idx, bool healthy)
		{
			svcs[idx].isHealthy = healthy;
		}

		public virtual void SetFailToBecomeActive(int idx, bool doFail)
		{
			svcs[idx].failToBecomeActive = doFail;
		}

		public virtual void SetFailToBecomeStandby(int idx, bool doFail)
		{
			svcs[idx].failToBecomeStandby = doFail;
		}

		public virtual void SetFailToFence(int idx, bool doFail)
		{
			svcs[idx].failToFence = doFail;
		}

		public virtual void SetUnreachable(int idx, bool unreachable)
		{
			svcs[idx].actUnreachable = unreachable;
		}

		/// <summary>Wait for the given HA service to enter the given HA state.</summary>
		/// <remarks>
		/// Wait for the given HA service to enter the given HA state.
		/// This is based on the state of ZKFC, not the state of HA service.
		/// There could be difference between the two. For example,
		/// When the service becomes unhealthy, ZKFC will quit ZK election and
		/// transition to HAServiceState.INITIALIZING and remain in that state
		/// until the service becomes healthy.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void WaitForHAState(int idx, HAServiceProtocol.HAServiceState state
			)
		{
			MiniZKFCCluster.DummyZKFC svc = GetZkfc(idx);
			while (svc.GetServiceState() != state)
			{
				ctx.CheckException();
				Sharpen.Thread.Sleep(50);
			}
		}

		/// <summary>Wait for the ZKFC to be notified of a change in health state.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void WaitForHealthState(int idx, HealthMonitor.State state)
		{
			ZKFCTestUtil.WaitForHealthState(thrs[idx].zkfc, state, ctx);
		}

		/// <summary>Wait for the given elector to enter the given elector state.</summary>
		/// <param name="idx">the service index (0 or 1)</param>
		/// <param name="state">the state to wait for</param>
		/// <exception cref="System.Exception">
		/// if it times out, or an exception occurs on one
		/// of the ZKFC threads while waiting.
		/// </exception>
		public virtual void WaitForElectorState(int idx, ActiveStandbyElector.State state
			)
		{
			ActiveStandbyElectorTestUtil.WaitForElectorState(ctx, GetElector(idx), state);
		}

		/// <summary>Expire the ZK session of the given service.</summary>
		/// <remarks>
		/// Expire the ZK session of the given service. This requires
		/// (and asserts) that the given service be the current active.
		/// </remarks>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException.NoNodeException">if no service holds the lock
		/// 	</exception>
		public virtual void ExpireActiveLockHolder(int idx)
		{
			Stat stat = new Stat();
			byte[] data = zks.GetZKDatabase().GetData(MiniZKFCCluster.DummyZKFC.LockZnode, stat
				, null);
			Assert.AssertArrayEquals(Ints.ToByteArray(svcs[idx].index), data);
			long session = stat.GetEphemeralOwner();
			Log.Info("Expiring svc " + idx + "'s zookeeper session " + session);
			zks.CloseSession(session);
		}

		/// <summary>Wait for the given HA service to become the active lock holder.</summary>
		/// <remarks>
		/// Wait for the given HA service to become the active lock holder.
		/// If the passed svc is null, waits for there to be no active
		/// lock holder.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void WaitForActiveLockHolder(int idx)
		{
			DummyHAService svc = idx == null ? null : svcs[idx];
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(ctx, zks, MiniZKFCCluster.DummyZKFC
				.ScopedParentZnode, (idx == null) ? null : Ints.ToByteArray(svc.index));
		}

		/// <summary>
		/// Expires the ZK session associated with service 'fromIdx', and waits
		/// until service 'toIdx' takes over.
		/// </summary>
		/// <exception cref="System.Exception">if the target service does not become active</exception>
		public virtual void ExpireAndVerifyFailover(int fromIdx, int toIdx)
		{
			Preconditions.CheckArgument(fromIdx != toIdx);
			GetElector(fromIdx).PreventSessionReestablishmentForTests();
			try
			{
				ExpireActiveLockHolder(fromIdx);
				WaitForHAState(fromIdx, HAServiceProtocol.HAServiceState.Standby);
				WaitForHAState(toIdx, HAServiceProtocol.HAServiceState.Active);
			}
			finally
			{
				GetElector(fromIdx).AllowSessionReestablishmentForTests();
			}
		}

		/// <summary>
		/// Test-thread which runs a ZK Failover Controller corresponding
		/// to a given dummy service.
		/// </summary>
		private class DummyZKFCThread : MultithreadedTestUtil.TestingThread
		{
			private readonly MiniZKFCCluster.DummyZKFC zkfc;

			public DummyZKFCThread(MiniZKFCCluster _enclosing, MultithreadedTestUtil.TestContext
				 ctx, DummyHAService svc)
				: base(ctx)
			{
				this._enclosing = _enclosing;
				this.zkfc = new MiniZKFCCluster.DummyZKFC(this._enclosing.conf, svc);
			}

			/// <exception cref="System.Exception"/>
			public override void DoWork()
			{
				try
				{
					Assert.Equal(0, this.zkfc.Run(new string[0]));
				}
				catch (Exception)
				{
				}
			}

			private readonly MiniZKFCCluster _enclosing;
			// Interrupted by main thread, that's OK.
		}

		internal class DummyZKFC : ZKFailoverController
		{
			private const string DummyCluster = "dummy-cluster";

			public const string ScopedParentZnode = ZKFailoverController.ZkParentZnodeDefault
				 + "/" + DummyCluster;

			private const string LockZnode = ScopedParentZnode + "/" + ActiveStandbyElector.LockFilename;

			private readonly DummyHAService localTarget;

			public DummyZKFC(Configuration conf, DummyHAService localTarget)
				: base(conf, localTarget)
			{
				this.localTarget = localTarget;
			}

			protected internal override byte[] TargetToData(HAServiceTarget target)
			{
				return Ints.ToByteArray(((DummyHAService)target).index);
			}

			protected internal override HAServiceTarget DataToTarget(byte[] data)
			{
				int index = Ints.FromByteArray(data);
				return DummyHAService.GetInstance(index);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void LoginAsFCUser()
			{
			}

			protected internal override string GetScopeInsideParentNode()
			{
				return DummyCluster;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			protected internal override void CheckRpcAdminAccess()
			{
			}

			protected internal override IPEndPoint GetRpcAddressToBindTo()
			{
				return new IPEndPoint(0);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void InitRPC()
			{
				base.InitRPC();
				localTarget.zkfcProxy = this.GetRpcServerForTests();
			}

			protected internal override PolicyProvider GetPolicyProvider()
			{
				return null;
			}
		}
	}
}
