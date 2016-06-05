using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Proto;


namespace Org.Apache.Hadoop.Ipc.ProtocolPB
{
	public class RefreshCallQueueProtocolServerSideTranslatorPB : RefreshCallQueueProtocolPB
	{
		private readonly RefreshCallQueueProtocol impl;

		private static readonly RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto
			 VoidRefreshCallQueueResponse = ((RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto
			)RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto.NewBuilder().Build
			());

		public RefreshCallQueueProtocolServerSideTranslatorPB(RefreshCallQueueProtocol impl
			)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual RefreshCallQueueProtocolProtos.RefreshCallQueueResponseProto RefreshCallQueue
			(RpcController controller, RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			 request)
		{
			try
			{
				impl.RefreshCallQueue();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshCallQueueResponse;
		}
	}
}
