using System;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.HA.ProtocolPB;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Test-only implementation of
	/// <see cref="HAServiceTarget"/>
	/// , which returns
	/// a mock implementation.
	/// </summary>
	internal class DummyHAService : HAServiceTarget
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.DummyHAService
			));

		private const string DummyFenceKey = "dummy.fence.key";

		internal volatile HAServiceProtocol.HAServiceState state;

		internal HAServiceProtocol proxy;

		internal ZKFCProtocol zkfcProxy = null;

		internal NodeFencer fencer;

		internal IPEndPoint address;

		internal bool isHealthy = true;

		internal bool actUnreachable = false;

		internal bool failToBecomeActive;

		internal bool failToBecomeStandby;

		internal bool failToFence;

		internal DummySharedResource sharedResource;

		public int fenceCount = 0;

		public int activeTransitionCount = 0;

		internal bool testWithProtoBufRPC = false;

		internal static AList<Org.Apache.Hadoop.HA.DummyHAService> instances = Lists.NewArrayList
			();

		internal int index;

		internal DummyHAService(HAServiceProtocol.HAServiceState state, IPEndPoint address
			)
			: this(state, address, false)
		{
		}

		internal DummyHAService(HAServiceProtocol.HAServiceState state, IPEndPoint address
			, bool testWithProtoBufRPC)
		{
			this.state = state;
			this.testWithProtoBufRPC = testWithProtoBufRPC;
			if (testWithProtoBufRPC)
			{
				this.address = StartAndGetRPCServerAddress(address);
			}
			else
			{
				this.address = address;
			}
			Configuration conf = new Configuration();
			this.proxy = MakeMock(conf, CommonConfigurationKeys.HaHmRpcTimeoutDefault);
			try
			{
				conf.Set(DummyFenceKey, typeof(DummyHAService.DummyFencer).FullName);
				this.fencer = Org.Mockito.Mockito.Spy(NodeFencer.Create(conf, DummyFenceKey));
			}
			catch (BadFencingConfigurationException e)
			{
				throw new RuntimeException(e);
			}
			lock (instances)
			{
				instances.AddItem(this);
				this.index = instances.Count;
			}
		}

		public virtual void SetSharedResource(DummySharedResource rsrc)
		{
			this.sharedResource = rsrc;
		}

		private IPEndPoint StartAndGetRPCServerAddress(IPEndPoint serverAddress)
		{
			Configuration conf = new Configuration();
			try
			{
				RPC.SetProtocolEngine(conf, typeof(HAServiceProtocolPB), typeof(ProtobufRpcEngine
					));
				HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator = new HAServiceProtocolServerSideTranslatorPB
					(new DummyHAService.MockHAProtocolImpl(this));
				BlockingService haPbService = HAServiceProtocolProtos.HAServiceProtocolService.NewReflectiveBlockingService
					(haServiceProtocolXlator);
				Server server = new RPC.Builder(conf).SetProtocol(typeof(HAServiceProtocolPB)).SetInstance
					(haPbService).SetBindAddress(serverAddress.GetHostName()).SetPort(serverAddress.
					Port).Build();
				server.Start();
				return NetUtils.GetConnectAddress(server);
			}
			catch (IOException)
			{
				return null;
			}
		}

		private HAServiceProtocol MakeMock(Configuration conf, int timeoutMs)
		{
			HAServiceProtocol service;
			if (!testWithProtoBufRPC)
			{
				service = new DummyHAService.MockHAProtocolImpl(this);
			}
			else
			{
				try
				{
					service = base.GetProxy(conf, timeoutMs);
				}
				catch (IOException)
				{
					return null;
				}
			}
			return Org.Mockito.Mockito.Spy(service);
		}

		public override IPEndPoint GetAddress()
		{
			return address;
		}

		public override IPEndPoint GetZKFCAddress()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override HAServiceProtocol GetProxy(Configuration conf, int timeout)
		{
			if (testWithProtoBufRPC)
			{
				proxy = MakeMock(conf, timeout);
			}
			return proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ZKFCProtocol GetZKFCProxy(Configuration conf, int timeout)
		{
			System.Diagnostics.Debug.Assert(zkfcProxy != null);
			return zkfcProxy;
		}

		public override NodeFencer GetFencer()
		{
			return fencer;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public override void CheckFencingConfigured()
		{
		}

		public override bool IsAutoFailoverEnabled()
		{
			return true;
		}

		public override string ToString()
		{
			return "DummyHAService #" + index;
		}

		public static HAServiceTarget GetInstance(int serial)
		{
			return instances[serial - 1];
		}

		private class MockHAProtocolImpl : HAServiceProtocol, IDisposable
		{
			/// <exception cref="Org.Apache.Hadoop.HA.HealthCheckFailedException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void MonitorHealth()
			{
				this.CheckUnreachable();
				if (!this._enclosing.isHealthy)
				{
					throw new HealthCheckFailedException("not healthy");
				}
			}

			/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void TransitionToActive(HAServiceProtocol.StateChangeRequestInfo req
				)
			{
				this._enclosing.activeTransitionCount++;
				this.CheckUnreachable();
				if (this._enclosing.failToBecomeActive)
				{
					throw new ServiceFailedException("injected failure");
				}
				if (this._enclosing.sharedResource != null)
				{
					this._enclosing.sharedResource.Take(this._enclosing);
				}
				this._enclosing.state = HAServiceProtocol.HAServiceState.Active;
			}

			/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void TransitionToStandby(HAServiceProtocol.StateChangeRequestInfo 
				req)
			{
				this.CheckUnreachable();
				if (this._enclosing.failToBecomeStandby)
				{
					throw new ServiceFailedException("injected failure");
				}
				if (this._enclosing.sharedResource != null)
				{
					this._enclosing.sharedResource.Release(this._enclosing);
				}
				this._enclosing.state = HAServiceProtocol.HAServiceState.Standby;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual HAServiceStatus GetServiceStatus()
			{
				this.CheckUnreachable();
				HAServiceStatus ret = new HAServiceStatus(this._enclosing.state);
				if (this._enclosing.state == HAServiceProtocol.HAServiceState.Standby || this._enclosing
					.state == HAServiceProtocol.HAServiceState.Active)
				{
					ret.SetReadyToBecomeActive();
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckUnreachable()
			{
				if (this._enclosing.actUnreachable)
				{
					throw new IOException("Connection refused (fake)");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			internal MockHAProtocolImpl(DummyHAService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DummyHAService _enclosing;
		}

		public class DummyFencer : FenceMethod
		{
			/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
			public virtual void CheckArgs(string args)
			{
			}

			/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
			public virtual bool TryFence(HAServiceTarget target, string args)
			{
				Log.Info("tryFence(" + target + ")");
				DummyHAService svc = (DummyHAService)target;
				lock (svc)
				{
					svc.fenceCount++;
				}
				if (svc.failToFence)
				{
					Log.Info("Injected failure to fence");
					return false;
				}
				svc.sharedResource.Release(svc);
				return true;
			}
		}
	}
}
