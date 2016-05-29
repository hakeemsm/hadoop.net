using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>
	/// This class allows a node manager to run without without communicating with a
	/// real RM.
	/// </summary>
	public class MockNodeStatusUpdater : NodeStatusUpdaterImpl
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.MockNodeStatusUpdater
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private ResourceTracker resourceTracker;

		public MockNodeStatusUpdater(Context context, Dispatcher dispatcher, NodeHealthCheckerService
			 healthChecker, NodeManagerMetrics metrics)
			: base(context, dispatcher, healthChecker, metrics)
		{
			resourceTracker = CreateResourceTracker();
		}

		protected internal virtual ResourceTracker CreateResourceTracker()
		{
			return new MockNodeStatusUpdater.MockResourceTracker();
		}

		protected internal override ResourceTracker GetRMClient()
		{
			return resourceTracker;
		}

		protected internal override void StopRMProxy()
		{
			return;
		}

		protected internal class MockResourceTracker : ResourceTracker
		{
			private int heartBeatID;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				RegisterNodeManagerResponse response = recordFactory.NewRecordInstance<RegisterNodeManagerResponse
					>();
				MasterKey masterKey = new MasterKeyPBImpl();
				masterKey.SetKeyId(123);
				masterKey.SetBytes(ByteBuffer.Wrap(new byte[] { 123 }));
				response.SetContainerTokenMasterKey(masterKey);
				response.SetNMTokenMasterKey(masterKey);
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				NodeStatus nodeStatus = request.GetNodeStatus();
				Log.Info("Got heartbeat number " + heartBeatID);
				nodeStatus.SetResponseId(heartBeatID++);
				NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
					(heartBeatID, null, null, null, null, null, 1000L);
				return nhResponse;
			}
		}
	}
}
