using Sharpen;

namespace org.apache.hadoop.ipc.protocolPB
{
	public class RefreshCallQueueProtocolServerSideTranslatorPB : org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB
	{
		private readonly org.apache.hadoop.ipc.RefreshCallQueueProtocol impl;

		private static readonly org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto
			 VOID_REFRESH_CALL_QUEUE_RESPONSE = ((org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto
			)org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto
			.newBuilder().build());

		public RefreshCallQueueProtocolServerSideTranslatorPB(org.apache.hadoop.ipc.RefreshCallQueueProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto
			 refreshCallQueue(com.google.protobuf.RpcController controller, org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			 request)
		{
			try
			{
				impl.refreshCallQueue();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
			return VOID_REFRESH_CALL_QUEUE_RESPONSE;
		}
	}
}
