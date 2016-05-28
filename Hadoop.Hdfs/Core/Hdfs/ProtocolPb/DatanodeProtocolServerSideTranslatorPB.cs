using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	public class DatanodeProtocolServerSideTranslatorPB : DatanodeProtocolPB
	{
		private readonly DatanodeProtocol impl;

		private static readonly DatanodeProtocolProtos.ErrorReportResponseProto VoidErrorReportResponseProto
			 = ((DatanodeProtocolProtos.ErrorReportResponseProto)DatanodeProtocolProtos.ErrorReportResponseProto
			.NewBuilder().Build());

		private static readonly DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto
			 VoidBlockReceivedAndDeleteResponse = ((DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto
			)DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto.NewBuilder().Build(
			));

		private static readonly DatanodeProtocolProtos.ReportBadBlocksResponseProto VoidReportBadBlockResponse
			 = ((DatanodeProtocolProtos.ReportBadBlocksResponseProto)DatanodeProtocolProtos.ReportBadBlocksResponseProto
			.NewBuilder().Build());

		private static readonly DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto
			 VoidCommitBlockSynchronizationResponseProto = ((DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto
			)DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto.NewBuilder().Build
			());

		public DatanodeProtocolServerSideTranslatorPB(DatanodeProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.RegisterDatanodeResponseProto RegisterDatanode
			(RpcController controller, DatanodeProtocolProtos.RegisterDatanodeRequestProto request
			)
		{
			DatanodeRegistration registration = PBHelper.Convert(request.GetRegistration());
			DatanodeRegistration registrationResp;
			try
			{
				registrationResp = impl.RegisterDatanode(registration);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((DatanodeProtocolProtos.RegisterDatanodeResponseProto)DatanodeProtocolProtos.RegisterDatanodeResponseProto
				.NewBuilder().SetRegistration(PBHelper.Convert(registrationResp)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.HeartbeatResponseProto SendHeartbeat(RpcController
			 controller, DatanodeProtocolProtos.HeartbeatRequestProto request)
		{
			HeartbeatResponse response;
			try
			{
				StorageReport[] report = PBHelper.ConvertStorageReports(request.GetReportsList());
				VolumeFailureSummary volumeFailureSummary = request.HasVolumeFailureSummary() ? PBHelper
					.ConvertVolumeFailureSummary(request.GetVolumeFailureSummary()) : null;
				response = impl.SendHeartbeat(PBHelper.Convert(request.GetRegistration()), report
					, request.GetCacheCapacity(), request.GetCacheUsed(), request.GetXmitsInProgress
					(), request.GetXceiverCount(), request.GetFailedVolumes(), volumeFailureSummary);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			DatanodeProtocolProtos.HeartbeatResponseProto.Builder builder = DatanodeProtocolProtos.HeartbeatResponseProto
				.NewBuilder();
			DatanodeCommand[] cmds = response.GetCommands();
			if (cmds != null)
			{
				for (int i = 0; i < cmds.Length; i++)
				{
					if (cmds[i] != null)
					{
						builder.AddCmds(PBHelper.Convert(cmds[i]));
					}
				}
			}
			builder.SetHaStatus(PBHelper.Convert(response.GetNameNodeHaState()));
			RollingUpgradeStatus rollingUpdateStatus = response.GetRollingUpdateStatus();
			if (rollingUpdateStatus != null)
			{
				// V2 is always set for newer datanodes.
				// To be compatible with older datanodes, V1 is set to null
				//  if the RU was finalized.
				HdfsProtos.RollingUpgradeStatusProto rus = PBHelper.ConvertRollingUpgradeStatus(rollingUpdateStatus
					);
				builder.SetRollingUpgradeStatusV2(rus);
				if (!rollingUpdateStatus.IsFinalized())
				{
					builder.SetRollingUpgradeStatus(rus);
				}
			}
			return ((DatanodeProtocolProtos.HeartbeatResponseProto)builder.Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.BlockReportResponseProto BlockReport(RpcController
			 controller, DatanodeProtocolProtos.BlockReportRequestProto request)
		{
			DatanodeCommand cmd = null;
			StorageBlockReport[] report = new StorageBlockReport[request.GetReportsCount()];
			int index = 0;
			foreach (DatanodeProtocolProtos.StorageBlockReportProto s in request.GetReportsList
				())
			{
				BlockListAsLongs blocks;
				if (s.HasNumberOfBlocks())
				{
					// new style buffer based reports
					int num = (int)s.GetNumberOfBlocks();
					Preconditions.CheckState(s.GetBlocksCount() == 0, "cannot send both blocks list and buffers"
						);
					blocks = BlockListAsLongs.DecodeBuffers(num, s.GetBlocksBuffersList());
				}
				else
				{
					blocks = BlockListAsLongs.DecodeLongs(s.GetBlocksList());
				}
				report[index++] = new StorageBlockReport(PBHelper.Convert(s.GetStorage()), blocks
					);
			}
			try
			{
				cmd = impl.BlockReport(PBHelper.Convert(request.GetRegistration()), request.GetBlockPoolId
					(), report, request.HasContext() ? PBHelper.Convert(request.GetContext()) : null
					);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			DatanodeProtocolProtos.BlockReportResponseProto.Builder builder = DatanodeProtocolProtos.BlockReportResponseProto
				.NewBuilder();
			if (cmd != null)
			{
				builder.SetCmd(PBHelper.Convert(cmd));
			}
			return ((DatanodeProtocolProtos.BlockReportResponseProto)builder.Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.CacheReportResponseProto CacheReport(RpcController
			 controller, DatanodeProtocolProtos.CacheReportRequestProto request)
		{
			DatanodeCommand cmd = null;
			try
			{
				cmd = impl.CacheReport(PBHelper.Convert(request.GetRegistration()), request.GetBlockPoolId
					(), request.GetBlocksList());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			DatanodeProtocolProtos.CacheReportResponseProto.Builder builder = DatanodeProtocolProtos.CacheReportResponseProto
				.NewBuilder();
			if (cmd != null)
			{
				builder.SetCmd(PBHelper.Convert(cmd));
			}
			return ((DatanodeProtocolProtos.CacheReportResponseProto)builder.Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto BlockReceivedAndDeleted
			(RpcController controller, DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto
			 request)
		{
			IList<DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto> sBlocks = request
				.GetBlocksList();
			StorageReceivedDeletedBlocks[] info = new StorageReceivedDeletedBlocks[sBlocks.Count
				];
			for (int i = 0; i < sBlocks.Count; i++)
			{
				DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto sBlock = sBlocks[i];
				IList<DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto> list = sBlock.GetBlocksList
					();
				ReceivedDeletedBlockInfo[] rdBlocks = new ReceivedDeletedBlockInfo[list.Count];
				for (int j = 0; j < list.Count; j++)
				{
					rdBlocks[j] = PBHelper.Convert(list[j]);
				}
				if (sBlock.HasStorage())
				{
					info[i] = new StorageReceivedDeletedBlocks(PBHelper.Convert(sBlock.GetStorage()), 
						rdBlocks);
				}
				else
				{
					info[i] = new StorageReceivedDeletedBlocks(sBlock.GetStorageUuid(), rdBlocks);
				}
			}
			try
			{
				impl.BlockReceivedAndDeleted(PBHelper.Convert(request.GetRegistration()), request
					.GetBlockPoolId(), info);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidBlockReceivedAndDeleteResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.ErrorReportResponseProto ErrorReport(RpcController
			 controller, DatanodeProtocolProtos.ErrorReportRequestProto request)
		{
			try
			{
				impl.ErrorReport(PBHelper.Convert(request.GetRegistartion()), request.GetErrorCode
					(), request.GetMsg());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidErrorReportResponseProto;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HdfsProtos.VersionResponseProto VersionRequest(RpcController controller
			, HdfsProtos.VersionRequestProto request)
		{
			NamespaceInfo info;
			try
			{
				info = impl.VersionRequest();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((HdfsProtos.VersionResponseProto)HdfsProtos.VersionResponseProto.NewBuilder
				().SetInfo(PBHelper.Convert(info)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.ReportBadBlocksResponseProto ReportBadBlocks
			(RpcController controller, DatanodeProtocolProtos.ReportBadBlocksRequestProto request
			)
		{
			IList<HdfsProtos.LocatedBlockProto> lbps = request.GetBlocksList();
			LocatedBlock[] blocks = new LocatedBlock[lbps.Count];
			for (int i = 0; i < lbps.Count; i++)
			{
				blocks[i] = PBHelper.Convert(lbps[i]);
			}
			try
			{
				impl.ReportBadBlocks(blocks);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidReportBadBlockResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto CommitBlockSynchronization
			(RpcController controller, DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto
			 request)
		{
			IList<HdfsProtos.DatanodeIDProto> dnprotos = request.GetNewTaragetsList();
			DatanodeID[] dns = new DatanodeID[dnprotos.Count];
			for (int i = 0; i < dnprotos.Count; i++)
			{
				dns[i] = PBHelper.Convert(dnprotos[i]);
			}
			IList<string> sidprotos = request.GetNewTargetStoragesList();
			string[] storageIDs = Sharpen.Collections.ToArray(sidprotos, new string[sidprotos
				.Count]);
			try
			{
				impl.CommitBlockSynchronization(PBHelper.Convert(request.GetBlock()), request.GetNewGenStamp
					(), request.GetNewLength(), request.GetCloseFile(), request.GetDeleteBlock(), dns
					, storageIDs);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidCommitBlockSynchronizationResponseProto;
		}
	}
}
