using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class LocalRMInterface : ResourceTracker
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

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
			NodeHeartbeatResponse response = recordFactory.NewRecordInstance<NodeHeartbeatResponse
				>();
			return response;
		}
	}
}
