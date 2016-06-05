using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="DatanodeProtocolPB"/>
	/// .
	/// </summary>
	public class DatanodeProtocolClientSideTranslatorPB : ProtocolMetaInterface, DatanodeProtocol
		, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private readonly DatanodeProtocolPB rpcProxy;

		private static readonly HdfsProtos.VersionRequestProto VoidVersionRequest = ((HdfsProtos.VersionRequestProto
			)HdfsProtos.VersionRequestProto.NewBuilder().Build());

		private static readonly RpcController NullController = null;

		[VisibleForTesting]
		public DatanodeProtocolClientSideTranslatorPB(DatanodeProtocolPB rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public DatanodeProtocolClientSideTranslatorPB(IPEndPoint nameNodeAddr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(DatanodeProtocolPB), typeof(ProtobufRpcEngine)
				);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			rpcProxy = CreateNamenode(nameNodeAddr, conf, ugi);
		}

		/// <exception cref="System.IO.IOException"/>
		private static DatanodeProtocolPB CreateNamenode(IPEndPoint nameNodeAddr, Configuration
			 conf, UserGroupInformation ugi)
		{
			return RPC.GetProtocolProxy<DatanodeProtocolPB>(RPC.GetProtocolVersion(typeof(DatanodeProtocolPB
				)), nameNodeAddr, ugi, conf, NetUtils.GetSocketFactory(conf, typeof(DatanodeProtocolPB
				)), Client.GetPingInterval(conf), null).GetProxy();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeRegistration RegisterDatanode(DatanodeRegistration registration
			)
		{
			DatanodeProtocolProtos.RegisterDatanodeRequestProto.Builder builder = DatanodeProtocolProtos.RegisterDatanodeRequestProto
				.NewBuilder().SetRegistration(PBHelper.Convert(registration));
			DatanodeProtocolProtos.RegisterDatanodeResponseProto resp;
			try
			{
				resp = rpcProxy.RegisterDatanode(NullController, ((DatanodeProtocolProtos.RegisterDatanodeRequestProto
					)builder.Build()));
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
			return PBHelper.Convert(resp.GetRegistration());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HeartbeatResponse SendHeartbeat(DatanodeRegistration registration, 
			StorageReport[] reports, long cacheCapacity, long cacheUsed, int xmitsInProgress
			, int xceiverCount, int failedVolumes, VolumeFailureSummary volumeFailureSummary
			)
		{
			DatanodeProtocolProtos.HeartbeatRequestProto.Builder builder = DatanodeProtocolProtos.HeartbeatRequestProto
				.NewBuilder().SetRegistration(PBHelper.Convert(registration)).SetXmitsInProgress
				(xmitsInProgress).SetXceiverCount(xceiverCount).SetFailedVolumes(failedVolumes);
			builder.AddAllReports(PBHelper.ConvertStorageReports(reports));
			if (cacheCapacity != 0)
			{
				builder.SetCacheCapacity(cacheCapacity);
			}
			if (cacheUsed != 0)
			{
				builder.SetCacheUsed(cacheUsed);
			}
			if (volumeFailureSummary != null)
			{
				builder.SetVolumeFailureSummary(PBHelper.ConvertVolumeFailureSummary(volumeFailureSummary
					));
			}
			DatanodeProtocolProtos.HeartbeatResponseProto resp;
			try
			{
				resp = rpcProxy.SendHeartbeat(NullController, ((DatanodeProtocolProtos.HeartbeatRequestProto
					)builder.Build()));
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
			DatanodeCommand[] cmds = new DatanodeCommand[resp.GetCmdsList().Count];
			int index = 0;
			foreach (DatanodeProtocolProtos.DatanodeCommandProto p in resp.GetCmdsList())
			{
				cmds[index] = PBHelper.Convert(p);
				index++;
			}
			RollingUpgradeStatus rollingUpdateStatus = null;
			// Use v2 semantics if available.
			if (resp.HasRollingUpgradeStatusV2())
			{
				rollingUpdateStatus = PBHelper.Convert(resp.GetRollingUpgradeStatusV2());
			}
			else
			{
				if (resp.HasRollingUpgradeStatus())
				{
					rollingUpdateStatus = PBHelper.Convert(resp.GetRollingUpgradeStatus());
				}
			}
			return new HeartbeatResponse(cmds, PBHelper.Convert(resp.GetHaStatus()), rollingUpdateStatus
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeCommand BlockReport(DatanodeRegistration registration, string
			 poolId, StorageBlockReport[] reports, BlockReportContext context)
		{
			DatanodeProtocolProtos.BlockReportRequestProto.Builder builder = DatanodeProtocolProtos.BlockReportRequestProto
				.NewBuilder().SetRegistration(PBHelper.Convert(registration)).SetBlockPoolId(poolId
				);
			bool useBlocksBuffer = registration.GetNamespaceInfo().IsCapabilitySupported(NamespaceInfo.Capability
				.StorageBlockReportBuffers);
			foreach (StorageBlockReport r in reports)
			{
				DatanodeProtocolProtos.StorageBlockReportProto.Builder reportBuilder = DatanodeProtocolProtos.StorageBlockReportProto
					.NewBuilder().SetStorage(PBHelper.Convert(r.GetStorage()));
				BlockListAsLongs blocks = r.GetBlocks();
				if (useBlocksBuffer)
				{
					reportBuilder.SetNumberOfBlocks(blocks.GetNumberOfBlocks());
					reportBuilder.AddAllBlocksBuffers(blocks.GetBlocksBuffers());
				}
				else
				{
					foreach (long value in blocks.GetBlockListAsLongs())
					{
						reportBuilder.AddBlocks(value);
					}
				}
				builder.AddReports(((DatanodeProtocolProtos.StorageBlockReportProto)reportBuilder
					.Build()));
			}
			builder.SetContext(PBHelper.Convert(context));
			DatanodeProtocolProtos.BlockReportResponseProto resp;
			try
			{
				resp = rpcProxy.BlockReport(NullController, ((DatanodeProtocolProtos.BlockReportRequestProto
					)builder.Build()));
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
			return resp.HasCmd() ? PBHelper.Convert(resp.GetCmd()) : null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeCommand CacheReport(DatanodeRegistration registration, string
			 poolId, IList<long> blockIds)
		{
			DatanodeProtocolProtos.CacheReportRequestProto.Builder builder = DatanodeProtocolProtos.CacheReportRequestProto
				.NewBuilder().SetRegistration(PBHelper.Convert(registration)).SetBlockPoolId(poolId
				);
			foreach (long blockId in blockIds)
			{
				builder.AddBlocks(blockId);
			}
			DatanodeProtocolProtos.CacheReportResponseProto resp;
			try
			{
				resp = rpcProxy.CacheReport(NullController, ((DatanodeProtocolProtos.CacheReportRequestProto
					)builder.Build()));
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
			if (resp.HasCmd())
			{
				return PBHelper.Convert(resp.GetCmd());
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void BlockReceivedAndDeleted(DatanodeRegistration registration, string
			 poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
		{
			DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto.Builder builder = DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto
				.NewBuilder().SetRegistration(PBHelper.Convert(registration)).SetBlockPoolId(poolId
				);
			foreach (StorageReceivedDeletedBlocks storageBlock in receivedAndDeletedBlocks)
			{
				DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto.Builder repBuilder = DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto
					.NewBuilder();
				repBuilder.SetStorageUuid(storageBlock.GetStorage().GetStorageID());
				// Set for wire compatibility.
				repBuilder.SetStorage(PBHelper.Convert(storageBlock.GetStorage()));
				foreach (ReceivedDeletedBlockInfo rdBlock in storageBlock.GetBlocks())
				{
					repBuilder.AddBlocks(PBHelper.Convert(rdBlock));
				}
				builder.AddBlocks(((DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto)repBuilder
					.Build()));
			}
			try
			{
				rpcProxy.BlockReceivedAndDeleted(NullController, ((DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto
					)builder.Build()));
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ErrorReport(DatanodeRegistration registration, int errorCode, 
			string msg)
		{
			DatanodeProtocolProtos.ErrorReportRequestProto req = ((DatanodeProtocolProtos.ErrorReportRequestProto
				)DatanodeProtocolProtos.ErrorReportRequestProto.NewBuilder().SetRegistartion(PBHelper
				.Convert(registration)).SetErrorCode(errorCode).SetMsg(msg).Build());
			try
			{
				rpcProxy.ErrorReport(NullController, req);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamespaceInfo VersionRequest()
		{
			try
			{
				return PBHelper.Convert(rpcProxy.VersionRequest(NullController, VoidVersionRequest
					).GetInfo());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportBadBlocks(LocatedBlock[] blocks)
		{
			DatanodeProtocolProtos.ReportBadBlocksRequestProto.Builder builder = DatanodeProtocolProtos.ReportBadBlocksRequestProto
				.NewBuilder();
			for (int i = 0; i < blocks.Length; i++)
			{
				builder.AddBlocks(i, PBHelper.Convert(blocks[i]));
			}
			DatanodeProtocolProtos.ReportBadBlocksRequestProto req = ((DatanodeProtocolProtos.ReportBadBlocksRequestProto
				)builder.Build());
			try
			{
				rpcProxy.ReportBadBlocks(NullController, req);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CommitBlockSynchronization(ExtendedBlock block, long newgenerationstamp
			, long newlength, bool closeFile, bool deleteblock, DatanodeID[] newtargets, string
			[] newtargetstorages)
		{
			DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto.Builder builder = DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto
				.NewBuilder().SetBlock(PBHelper.Convert(block)).SetNewGenStamp(newgenerationstamp
				).SetNewLength(newlength).SetCloseFile(closeFile).SetDeleteBlock(deleteblock);
			for (int i = 0; i < newtargets.Length; i++)
			{
				builder.AddNewTaragets(PBHelper.Convert(newtargets[i]));
				builder.AddNewTargetStorages(newtargetstorages[i]);
			}
			DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto req = ((DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto
				)builder.Build());
			try
			{
				rpcProxy.CommitBlockSynchronization(NullController, req);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			// ProtocolMetaInterface
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(DatanodeProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(DatanodeProtocolPB)), methodName
				);
		}
	}
}
