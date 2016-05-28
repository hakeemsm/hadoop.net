using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// Implementation for protobuf service that forwards requests
	/// received on
	/// <see cref="InterDatanodeProtocolPB"/>
	/// to the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.InterDatanodeProtocol"/>
	/// server implementation.
	/// </summary>
	public class InterDatanodeProtocolServerSideTranslatorPB : InterDatanodeProtocolPB
	{
		private readonly InterDatanodeProtocol impl;

		public InterDatanodeProtocolServerSideTranslatorPB(InterDatanodeProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto InitReplicaRecovery
			(RpcController unused, InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto
			 request)
		{
			BlockRecoveryCommand.RecoveringBlock b = PBHelper.Convert(request.GetBlock());
			ReplicaRecoveryInfo r;
			try
			{
				r = impl.InitReplicaRecovery(b);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			if (r == null)
			{
				return ((InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto)InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto
					.NewBuilder().SetReplicaFound(false).Build());
			}
			else
			{
				return ((InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto)InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto
					.NewBuilder().SetReplicaFound(true).SetBlock(PBHelper.Convert(r)).SetState(PBHelper
					.Convert(r.GetOriginalReplicaState())).Build());
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryResponseProto
			 UpdateReplicaUnderRecovery(RpcController unused, InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto
			 request)
		{
			string storageID;
			try
			{
				storageID = impl.UpdateReplicaUnderRecovery(PBHelper.Convert(request.GetBlock()), 
					request.GetRecoveryId(), request.GetNewBlockId(), request.GetNewLength());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryResponseProto)InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryResponseProto
				.NewBuilder().SetStorageUuid(storageID).Build());
		}
	}
}
