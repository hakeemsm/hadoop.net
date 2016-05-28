using System.Collections.Generic;
using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>This class is used on the server side.</summary>
	/// <remarks>
	/// This class is used on the server side. Calls come across the wire for the
	/// for protocol
	/// <see cref="ClientNamenodeProtocolPB"/>
	/// .
	/// This class translates the PB data types
	/// to the native data types used inside the NN as specified in the generic
	/// ClientProtocol.
	/// </remarks>
	public class ClientNamenodeProtocolServerSideTranslatorPB : ClientNamenodeProtocolPB
	{
		private readonly ClientProtocol server;

		internal static readonly ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto
			 VoidDeleteSnapshotResponse = ((ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto
			)ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto.NewBuilder().Build());

		internal static readonly ClientNamenodeProtocolProtos.RenameSnapshotResponseProto
			 VoidRenameSnapshotResponse = ((ClientNamenodeProtocolProtos.RenameSnapshotResponseProto
			)ClientNamenodeProtocolProtos.RenameSnapshotResponseProto.NewBuilder().Build());

		internal static readonly ClientNamenodeProtocolProtos.AllowSnapshotResponseProto 
			VoidAllowSnapshotResponse = ((ClientNamenodeProtocolProtos.AllowSnapshotResponseProto
			)ClientNamenodeProtocolProtos.AllowSnapshotResponseProto.NewBuilder().Build());

		internal static readonly ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto
			 VoidDisallowSnapshotResponse = ((ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto
			)ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto.NewBuilder().Build()
			);

		internal static readonly ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto
			 NullGetSnapshottableDirListingResponse = ((ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto
			)ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto.NewBuilder
			().Build());

		internal static readonly ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto
			 VoidSetStoragePolicyResponse = ((ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto
			)ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto.NewBuilder().Build()
			);

		private static readonly ClientNamenodeProtocolProtos.CreateResponseProto VoidCreateResponse
			 = ((ClientNamenodeProtocolProtos.CreateResponseProto)ClientNamenodeProtocolProtos.CreateResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SetPermissionResponseProto VoidSetPermResponse
			 = ((ClientNamenodeProtocolProtos.SetPermissionResponseProto)ClientNamenodeProtocolProtos.SetPermissionResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SetOwnerResponseProto VoidSetOwnerResponse
			 = ((ClientNamenodeProtocolProtos.SetOwnerResponseProto)ClientNamenodeProtocolProtos.SetOwnerResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.AbandonBlockResponseProto VoidAddBlockResponse
			 = ((ClientNamenodeProtocolProtos.AbandonBlockResponseProto)ClientNamenodeProtocolProtos.AbandonBlockResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto
			 VoidRepBadBlockResponse = ((ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto
			)ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.ConcatResponseProto VoidConcatResponse
			 = ((ClientNamenodeProtocolProtos.ConcatResponseProto)ClientNamenodeProtocolProtos.ConcatResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.Rename2ResponseProto VoidRename2Response
			 = ((ClientNamenodeProtocolProtos.Rename2ResponseProto)ClientNamenodeProtocolProtos.Rename2ResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.GetListingResponseProto VoidGetlistingResponse
			 = ((ClientNamenodeProtocolProtos.GetListingResponseProto)ClientNamenodeProtocolProtos.GetListingResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.RenewLeaseResponseProto VoidRenewleaseResponse
			 = ((ClientNamenodeProtocolProtos.RenewLeaseResponseProto)ClientNamenodeProtocolProtos.RenewLeaseResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SaveNamespaceResponseProto VoidSavenamespaceResponse
			 = ((ClientNamenodeProtocolProtos.SaveNamespaceResponseProto)ClientNamenodeProtocolProtos.SaveNamespaceResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.RefreshNodesResponseProto VoidRefreshnodesResponse
			 = ((ClientNamenodeProtocolProtos.RefreshNodesResponseProto)ClientNamenodeProtocolProtos.RefreshNodesResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto
			 VoidFinalizeupgradeResponse = ((ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto
			)ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.MetaSaveResponseProto VoidMetasaveResponse
			 = ((ClientNamenodeProtocolProtos.MetaSaveResponseProto)ClientNamenodeProtocolProtos.MetaSaveResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.GetFileInfoResponseProto VoidGetfileinfoResponse
			 = ((ClientNamenodeProtocolProtos.GetFileInfoResponseProto)ClientNamenodeProtocolProtos.GetFileInfoResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto
			 VoidGetfilelinkinfoResponse = ((ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto
			)ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SetQuotaResponseProto VoidSetquotaResponse
			 = ((ClientNamenodeProtocolProtos.SetQuotaResponseProto)ClientNamenodeProtocolProtos.SetQuotaResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.FsyncResponseProto VoidFsyncResponse
			 = ((ClientNamenodeProtocolProtos.FsyncResponseProto)ClientNamenodeProtocolProtos.FsyncResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SetTimesResponseProto VoidSettimesResponse
			 = ((ClientNamenodeProtocolProtos.SetTimesResponseProto)ClientNamenodeProtocolProtos.SetTimesResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.CreateSymlinkResponseProto VoidCreatesymlinkResponse
			 = ((ClientNamenodeProtocolProtos.CreateSymlinkResponseProto)ClientNamenodeProtocolProtos.CreateSymlinkResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.UpdatePipelineResponseProto 
			VoidUpdatepipelineResponse = ((ClientNamenodeProtocolProtos.UpdatePipelineResponseProto
			)ClientNamenodeProtocolProtos.UpdatePipelineResponseProto.NewBuilder().Build());

		private static readonly SecurityProtos.CancelDelegationTokenResponseProto VoidCanceldelegationtokenResponse
			 = ((SecurityProtos.CancelDelegationTokenResponseProto)SecurityProtos.CancelDelegationTokenResponseProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto
			 VoidSetbalancerbandwidthResponse = ((ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto
			)ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto.NewBuilder().Build
			());

		private static readonly AclProtos.SetAclResponseProto VoidSetaclResponse = AclProtos.SetAclResponseProto
			.GetDefaultInstance();

		private static readonly AclProtos.ModifyAclEntriesResponseProto VoidModifyaclentriesResponse
			 = AclProtos.ModifyAclEntriesResponseProto.GetDefaultInstance();

		private static readonly AclProtos.RemoveAclEntriesResponseProto VoidRemoveaclentriesResponse
			 = AclProtos.RemoveAclEntriesResponseProto.GetDefaultInstance();

		private static readonly AclProtos.RemoveDefaultAclResponseProto VoidRemovedefaultaclResponse
			 = AclProtos.RemoveDefaultAclResponseProto.GetDefaultInstance();

		private static readonly AclProtos.RemoveAclResponseProto VoidRemoveaclResponse = 
			AclProtos.RemoveAclResponseProto.GetDefaultInstance();

		private static readonly XAttrProtos.SetXAttrResponseProto VoidSetxattrResponse = 
			XAttrProtos.SetXAttrResponseProto.GetDefaultInstance();

		private static readonly XAttrProtos.RemoveXAttrResponseProto VoidRemovexattrResponse
			 = XAttrProtos.RemoveXAttrResponseProto.GetDefaultInstance();

		private static readonly ClientNamenodeProtocolProtos.CheckAccessResponseProto VoidCheckaccessResponse
			 = ClientNamenodeProtocolProtos.CheckAccessResponseProto.GetDefaultInstance();

		/// <summary>Constructor</summary>
		/// <param name="server">- the NN server</param>
		/// <exception cref="System.IO.IOException"/>
		public ClientNamenodeProtocolServerSideTranslatorPB(ClientProtocol server)
		{
			this.server = server;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto GetBlockLocations
			(RpcController controller, ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto
			 req)
		{
			try
			{
				LocatedBlocks b = server.GetBlockLocations(req.GetSrc(), req.GetOffset(), req.GetLength
					());
				ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.Builder builder = ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto
					.NewBuilder();
				if (b != null)
				{
					builder.SetLocations(PBHelper.Convert(b)).Build();
				}
				return ((ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto)builder.Build
					());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto GetServerDefaults
			(RpcController controller, ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto
			 req)
		{
			try
			{
				FsServerDefaults result = server.GetServerDefaults();
				return ((ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto)ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto
					.NewBuilder().SetServerDefaults(PBHelper.Convert(result)).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.CreateResponseProto Create(RpcController
			 controller, ClientNamenodeProtocolProtos.CreateRequestProto req)
		{
			try
			{
				HdfsFileStatus result = server.Create(req.GetSrc(), PBHelper.Convert(req.GetMasked
					()), req.GetClientName(), PBHelper.ConvertCreateFlag(req.GetCreateFlag()), req.GetCreateParent
					(), (short)req.GetReplication(), req.GetBlockSize(), PBHelper.ConvertCryptoProtocolVersions
					(req.GetCryptoProtocolVersionList()));
				if (result != null)
				{
					return ((ClientNamenodeProtocolProtos.CreateResponseProto)ClientNamenodeProtocolProtos.CreateResponseProto
						.NewBuilder().SetFs(PBHelper.Convert(result)).Build());
				}
				return VoidCreateResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.AppendResponseProto Append(RpcController
			 controller, ClientNamenodeProtocolProtos.AppendRequestProto req)
		{
			try
			{
				EnumSetWritable<CreateFlag> flags = req.HasFlag() ? PBHelper.ConvertCreateFlag(req
					.GetFlag()) : new EnumSetWritable<CreateFlag>(EnumSet.Of(CreateFlag.Append));
				LastBlockWithStatus result = server.Append(req.GetSrc(), req.GetClientName(), flags
					);
				ClientNamenodeProtocolProtos.AppendResponseProto.Builder builder = ClientNamenodeProtocolProtos.AppendResponseProto
					.NewBuilder();
				if (result.GetLastBlock() != null)
				{
					builder.SetBlock(PBHelper.Convert(result.GetLastBlock()));
				}
				if (result.GetFileStatus() != null)
				{
					builder.SetStat(PBHelper.Convert(result.GetFileStatus()));
				}
				return ((ClientNamenodeProtocolProtos.AppendResponseProto)builder.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetReplicationResponseProto SetReplication
			(RpcController controller, ClientNamenodeProtocolProtos.SetReplicationRequestProto
			 req)
		{
			try
			{
				bool result = server.SetReplication(req.GetSrc(), (short)req.GetReplication());
				return ((ClientNamenodeProtocolProtos.SetReplicationResponseProto)ClientNamenodeProtocolProtos.SetReplicationResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetPermissionResponseProto SetPermission
			(RpcController controller, ClientNamenodeProtocolProtos.SetPermissionRequestProto
			 req)
		{
			try
			{
				server.SetPermission(req.GetSrc(), PBHelper.Convert(req.GetPermission()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidSetPermResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetOwnerResponseProto SetOwner(RpcController
			 controller, ClientNamenodeProtocolProtos.SetOwnerRequestProto req)
		{
			try
			{
				server.SetOwner(req.GetSrc(), req.HasUsername() ? req.GetUsername() : null, req.HasGroupname
					() ? req.GetGroupname() : null);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidSetOwnerResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.AbandonBlockResponseProto AbandonBlock
			(RpcController controller, ClientNamenodeProtocolProtos.AbandonBlockRequestProto
			 req)
		{
			try
			{
				server.AbandonBlock(PBHelper.Convert(req.GetB()), req.GetFileId(), req.GetSrc(), 
					req.GetHolder());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidAddBlockResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.AddBlockResponseProto AddBlock(RpcController
			 controller, ClientNamenodeProtocolProtos.AddBlockRequestProto req)
		{
			try
			{
				IList<HdfsProtos.DatanodeInfoProto> excl = req.GetExcludeNodesList();
				IList<string> favor = req.GetFavoredNodesList();
				LocatedBlock result = server.AddBlock(req.GetSrc(), req.GetClientName(), req.HasPrevious
					() ? PBHelper.Convert(req.GetPrevious()) : null, (excl == null || excl.Count == 
					0) ? null : PBHelper.Convert(Sharpen.Collections.ToArray(excl, new HdfsProtos.DatanodeInfoProto
					[excl.Count])), req.GetFileId(), (favor == null || favor.Count == 0) ? null : Sharpen.Collections.ToArray
					(favor, new string[favor.Count]));
				return ((ClientNamenodeProtocolProtos.AddBlockResponseProto)ClientNamenodeProtocolProtos.AddBlockResponseProto
					.NewBuilder().SetBlock(PBHelper.Convert(result)).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto GetAdditionalDatanode
			(RpcController controller, ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto
			 req)
		{
			try
			{
				IList<HdfsProtos.DatanodeInfoProto> existingList = req.GetExistingsList();
				IList<string> existingStorageIDsList = req.GetExistingStorageUuidsList();
				IList<HdfsProtos.DatanodeInfoProto> excludesList = req.GetExcludesList();
				LocatedBlock result = server.GetAdditionalDatanode(req.GetSrc(), req.GetFileId(), 
					PBHelper.Convert(req.GetBlk()), PBHelper.Convert(Sharpen.Collections.ToArray(existingList
					, new HdfsProtos.DatanodeInfoProto[existingList.Count])), Sharpen.Collections.ToArray
					(existingStorageIDsList, new string[existingStorageIDsList.Count]), PBHelper.Convert
					(Sharpen.Collections.ToArray(excludesList, new HdfsProtos.DatanodeInfoProto[excludesList
					.Count])), req.GetNumAdditionalNodes(), req.GetClientName());
				return ((ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto)ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto
					.NewBuilder().SetBlock(PBHelper.Convert(result)).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.CompleteResponseProto Complete(RpcController
			 controller, ClientNamenodeProtocolProtos.CompleteRequestProto req)
		{
			try
			{
				bool result = server.Complete(req.GetSrc(), req.GetClientName(), req.HasLast() ? 
					PBHelper.Convert(req.GetLast()) : null, req.HasFileId() ? req.GetFileId() : INodeId
					.GrandfatherInodeId);
				return ((ClientNamenodeProtocolProtos.CompleteResponseProto)ClientNamenodeProtocolProtos.CompleteResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto ReportBadBlocks
			(RpcController controller, ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto
			 req)
		{
			try
			{
				IList<HdfsProtos.LocatedBlockProto> bl = req.GetBlocksList();
				server.ReportBadBlocks(PBHelper.ConvertLocatedBlock(Sharpen.Collections.ToArray(bl
					, new HdfsProtos.LocatedBlockProto[bl.Count])));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRepBadBlockResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ConcatResponseProto Concat(RpcController
			 controller, ClientNamenodeProtocolProtos.ConcatRequestProto req)
		{
			try
			{
				IList<string> srcs = req.GetSrcsList();
				server.Concat(req.GetTrg(), Sharpen.Collections.ToArray(srcs, new string[srcs.Count
					]));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidConcatResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RenameResponseProto Rename(RpcController
			 controller, ClientNamenodeProtocolProtos.RenameRequestProto req)
		{
			try
			{
				bool result = server.Rename(req.GetSrc(), req.GetDst());
				return ((ClientNamenodeProtocolProtos.RenameResponseProto)ClientNamenodeProtocolProtos.RenameResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.Rename2ResponseProto Rename2(RpcController
			 controller, ClientNamenodeProtocolProtos.Rename2RequestProto req)
		{
			try
			{
				server.Rename2(req.GetSrc(), req.GetDst(), req.GetOverwriteDest() ? Options.Rename
					.Overwrite : Options.Rename.None);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRename2Response;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.TruncateResponseProto Truncate(RpcController
			 controller, ClientNamenodeProtocolProtos.TruncateRequestProto req)
		{
			try
			{
				bool result = server.Truncate(req.GetSrc(), req.GetNewLength(), req.GetClientName
					());
				return ((ClientNamenodeProtocolProtos.TruncateResponseProto)ClientNamenodeProtocolProtos.TruncateResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.DeleteResponseProto Delete(RpcController
			 controller, ClientNamenodeProtocolProtos.DeleteRequestProto req)
		{
			try
			{
				bool result = server.Delete(req.GetSrc(), req.GetRecursive());
				return ((ClientNamenodeProtocolProtos.DeleteResponseProto)ClientNamenodeProtocolProtos.DeleteResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.MkdirsResponseProto Mkdirs(RpcController
			 controller, ClientNamenodeProtocolProtos.MkdirsRequestProto req)
		{
			try
			{
				bool result = server.Mkdirs(req.GetSrc(), PBHelper.Convert(req.GetMasked()), req.
					GetCreateParent());
				return ((ClientNamenodeProtocolProtos.MkdirsResponseProto)ClientNamenodeProtocolProtos.MkdirsResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetListingResponseProto GetListing(RpcController
			 controller, ClientNamenodeProtocolProtos.GetListingRequestProto req)
		{
			try
			{
				DirectoryListing result = server.GetListing(req.GetSrc(), req.GetStartAfter().ToByteArray
					(), req.GetNeedLocation());
				if (result != null)
				{
					return ((ClientNamenodeProtocolProtos.GetListingResponseProto)ClientNamenodeProtocolProtos.GetListingResponseProto
						.NewBuilder().SetDirList(PBHelper.Convert(result)).Build());
				}
				else
				{
					return VoidGetlistingResponse;
				}
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RenewLeaseResponseProto RenewLease(RpcController
			 controller, ClientNamenodeProtocolProtos.RenewLeaseRequestProto req)
		{
			try
			{
				server.RenewLease(req.GetClientName());
				return VoidRenewleaseResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RecoverLeaseResponseProto RecoverLease
			(RpcController controller, ClientNamenodeProtocolProtos.RecoverLeaseRequestProto
			 req)
		{
			try
			{
				bool result = server.RecoverLease(req.GetSrc(), req.GetClientName());
				return ((ClientNamenodeProtocolProtos.RecoverLeaseResponseProto)ClientNamenodeProtocolProtos.RecoverLeaseResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto RestoreFailedStorage
			(RpcController controller, ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto
			 req)
		{
			try
			{
				bool result = server.RestoreFailedStorage(req.GetArg());
				return ((ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto)ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetFsStatsResponseProto GetFsStats(RpcController
			 controller, ClientNamenodeProtocolProtos.GetFsStatusRequestProto req)
		{
			try
			{
				return PBHelper.Convert(server.GetStats());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto GetDatanodeReport
			(RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto
			 req)
		{
			try
			{
				IList<HdfsProtos.DatanodeInfoProto> result = PBHelper.Convert(server.GetDatanodeReport
					(PBHelper.Convert(req.GetType())));
				return ((ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto)ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto
					.NewBuilder().AddAllDi(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto
			 GetDatanodeStorageReport(RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto
			 req)
		{
			try
			{
				IList<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> reports = PBHelper
					.ConvertDatanodeStorageReports(server.GetDatanodeStorageReport(PBHelper.Convert(
					req.GetType())));
				return ((ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto)ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto
					.NewBuilder().AddAllDatanodeStorageReports(reports).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto GetPreferredBlockSize
			(RpcController controller, ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto
			 req)
		{
			try
			{
				long result = server.GetPreferredBlockSize(req.GetFilename());
				return ((ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto)ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto
					.NewBuilder().SetBsize(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetSafeModeResponseProto SetSafeMode(
			RpcController controller, ClientNamenodeProtocolProtos.SetSafeModeRequestProto req
			)
		{
			try
			{
				bool result = server.SetSafeMode(PBHelper.Convert(req.GetAction()), req.GetChecked
					());
				return ((ClientNamenodeProtocolProtos.SetSafeModeResponseProto)ClientNamenodeProtocolProtos.SetSafeModeResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SaveNamespaceResponseProto SaveNamespace
			(RpcController controller, ClientNamenodeProtocolProtos.SaveNamespaceRequestProto
			 req)
		{
			try
			{
				server.SaveNamespace();
				return VoidSavenamespaceResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RollEditsResponseProto RollEdits(RpcController
			 controller, ClientNamenodeProtocolProtos.RollEditsRequestProto request)
		{
			try
			{
				long txid = server.RollEdits();
				return ((ClientNamenodeProtocolProtos.RollEditsResponseProto)ClientNamenodeProtocolProtos.RollEditsResponseProto
					.NewBuilder().SetNewSegmentTxId(txid).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RefreshNodesResponseProto RefreshNodes
			(RpcController controller, ClientNamenodeProtocolProtos.RefreshNodesRequestProto
			 req)
		{
			try
			{
				server.RefreshNodes();
				return VoidRefreshnodesResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto FinalizeUpgrade
			(RpcController controller, ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto
			 req)
		{
			try
			{
				server.FinalizeUpgrade();
				return VoidFinalizeupgradeResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RollingUpgradeResponseProto RollingUpgrade
			(RpcController controller, ClientNamenodeProtocolProtos.RollingUpgradeRequestProto
			 req)
		{
			try
			{
				RollingUpgradeInfo info = server.RollingUpgrade(PBHelper.Convert(req.GetAction())
					);
				ClientNamenodeProtocolProtos.RollingUpgradeResponseProto.Builder b = ClientNamenodeProtocolProtos.RollingUpgradeResponseProto
					.NewBuilder();
				if (info != null)
				{
					b.SetRollingUpgradeInfo(PBHelper.Convert(info));
				}
				return ((ClientNamenodeProtocolProtos.RollingUpgradeResponseProto)b.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto ListCorruptFileBlocks
			(RpcController controller, ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto
			 req)
		{
			try
			{
				CorruptFileBlocks result = server.ListCorruptFileBlocks(req.GetPath(), req.HasCookie
					() ? req.GetCookie() : null);
				return ((ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto)ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto
					.NewBuilder().SetCorrupt(PBHelper.Convert(result)).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.MetaSaveResponseProto MetaSave(RpcController
			 controller, ClientNamenodeProtocolProtos.MetaSaveRequestProto req)
		{
			try
			{
				server.MetaSave(req.GetFilename());
				return VoidMetasaveResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetFileInfoResponseProto GetFileInfo(
			RpcController controller, ClientNamenodeProtocolProtos.GetFileInfoRequestProto req
			)
		{
			try
			{
				HdfsFileStatus result = server.GetFileInfo(req.GetSrc());
				if (result != null)
				{
					return ((ClientNamenodeProtocolProtos.GetFileInfoResponseProto)ClientNamenodeProtocolProtos.GetFileInfoResponseProto
						.NewBuilder().SetFs(PBHelper.Convert(result)).Build());
				}
				return VoidGetfileinfoResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto GetFileLinkInfo
			(RpcController controller, ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto
			 req)
		{
			try
			{
				HdfsFileStatus result = server.GetFileLinkInfo(req.GetSrc());
				if (result != null)
				{
					return ((ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto)ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto
						.NewBuilder().SetFs(PBHelper.Convert(result)).Build());
				}
				else
				{
					return VoidGetfilelinkinfoResponse;
				}
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetContentSummaryResponseProto GetContentSummary
			(RpcController controller, ClientNamenodeProtocolProtos.GetContentSummaryRequestProto
			 req)
		{
			try
			{
				ContentSummary result = server.GetContentSummary(req.GetPath());
				return ((ClientNamenodeProtocolProtos.GetContentSummaryResponseProto)ClientNamenodeProtocolProtos.GetContentSummaryResponseProto
					.NewBuilder().SetSummary(PBHelper.Convert(result)).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetQuotaResponseProto SetQuota(RpcController
			 controller, ClientNamenodeProtocolProtos.SetQuotaRequestProto req)
		{
			try
			{
				server.SetQuota(req.GetPath(), req.GetNamespaceQuota(), req.GetStoragespaceQuota(
					), req.HasStorageType() ? PBHelper.ConvertStorageType(req.GetStorageType()) : null
					);
				return VoidSetquotaResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.FsyncResponseProto Fsync(RpcController
			 controller, ClientNamenodeProtocolProtos.FsyncRequestProto req)
		{
			try
			{
				server.Fsync(req.GetSrc(), req.GetFileId(), req.GetClient(), req.GetLastBlockLength
					());
				return VoidFsyncResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetTimesResponseProto SetTimes(RpcController
			 controller, ClientNamenodeProtocolProtos.SetTimesRequestProto req)
		{
			try
			{
				server.SetTimes(req.GetSrc(), req.GetMtime(), req.GetAtime());
				return VoidSettimesResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.CreateSymlinkResponseProto CreateSymlink
			(RpcController controller, ClientNamenodeProtocolProtos.CreateSymlinkRequestProto
			 req)
		{
			try
			{
				server.CreateSymlink(req.GetTarget(), req.GetLink(), PBHelper.Convert(req.GetDirPerm
					()), req.GetCreateParent());
				return VoidCreatesymlinkResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetLinkTargetResponseProto GetLinkTarget
			(RpcController controller, ClientNamenodeProtocolProtos.GetLinkTargetRequestProto
			 req)
		{
			try
			{
				string result = server.GetLinkTarget(req.GetPath());
				ClientNamenodeProtocolProtos.GetLinkTargetResponseProto.Builder builder = ClientNamenodeProtocolProtos.GetLinkTargetResponseProto
					.NewBuilder();
				if (result != null)
				{
					builder.SetTargetPath(result);
				}
				return ((ClientNamenodeProtocolProtos.GetLinkTargetResponseProto)builder.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto UpdateBlockForPipeline
			(RpcController controller, ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto
			 req)
		{
			try
			{
				HdfsProtos.LocatedBlockProto result = PBHelper.Convert(server.UpdateBlockForPipeline
					(PBHelper.Convert(req.GetBlock()), req.GetClientName()));
				return ((ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto)ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto
					.NewBuilder().SetBlock(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.UpdatePipelineResponseProto UpdatePipeline
			(RpcController controller, ClientNamenodeProtocolProtos.UpdatePipelineRequestProto
			 req)
		{
			try
			{
				IList<HdfsProtos.DatanodeIDProto> newNodes = req.GetNewNodesList();
				IList<string> newStorageIDs = req.GetStorageIDsList();
				server.UpdatePipeline(req.GetClientName(), PBHelper.Convert(req.GetOldBlock()), PBHelper
					.Convert(req.GetNewBlock()), PBHelper.Convert(Sharpen.Collections.ToArray(newNodes
					, new HdfsProtos.DatanodeIDProto[newNodes.Count])), Sharpen.Collections.ToArray(
					newStorageIDs, new string[newStorageIDs.Count]));
				return VoidUpdatepipelineResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual SecurityProtos.GetDelegationTokenResponseProto GetDelegationToken(
			RpcController controller, SecurityProtos.GetDelegationTokenRequestProto req)
		{
			try
			{
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = server.
					GetDelegationToken(new Text(req.GetRenewer()));
				SecurityProtos.GetDelegationTokenResponseProto.Builder rspBuilder = SecurityProtos.GetDelegationTokenResponseProto
					.NewBuilder();
				if (token != null)
				{
					rspBuilder.SetToken(PBHelper.Convert(token));
				}
				return ((SecurityProtos.GetDelegationTokenResponseProto)rspBuilder.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual SecurityProtos.RenewDelegationTokenResponseProto RenewDelegationToken
			(RpcController controller, SecurityProtos.RenewDelegationTokenRequestProto req)
		{
			try
			{
				long result = server.RenewDelegationToken(PBHelper.ConvertDelegationToken(req.GetToken
					()));
				return ((SecurityProtos.RenewDelegationTokenResponseProto)SecurityProtos.RenewDelegationTokenResponseProto
					.NewBuilder().SetNewExpiryTime(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual SecurityProtos.CancelDelegationTokenResponseProto CancelDelegationToken
			(RpcController controller, SecurityProtos.CancelDelegationTokenRequestProto req)
		{
			try
			{
				server.CancelDelegationToken(PBHelper.ConvertDelegationToken(req.GetToken()));
				return VoidCanceldelegationtokenResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto SetBalancerBandwidth
			(RpcController controller, ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto
			 req)
		{
			try
			{
				server.SetBalancerBandwidth(req.GetBandwidth());
				return VoidSetbalancerbandwidthResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto GetDataEncryptionKey
			(RpcController controller, ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto
			 request)
		{
			try
			{
				ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto.Builder builder = 
					ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto.NewBuilder();
				DataEncryptionKey encryptionKey = server.GetDataEncryptionKey();
				if (encryptionKey != null)
				{
					builder.SetDataEncryptionKey(PBHelper.Convert(encryptionKey));
				}
				return ((ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto)builder.Build
					());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.CreateSnapshotResponseProto CreateSnapshot
			(RpcController controller, ClientNamenodeProtocolProtos.CreateSnapshotRequestProto
			 req)
		{
			try
			{
				ClientNamenodeProtocolProtos.CreateSnapshotResponseProto.Builder builder = ClientNamenodeProtocolProtos.CreateSnapshotResponseProto
					.NewBuilder();
				string snapshotPath = server.CreateSnapshot(req.GetSnapshotRoot(), req.HasSnapshotName
					() ? req.GetSnapshotName() : null);
				if (snapshotPath != null)
				{
					builder.SetSnapshotPath(snapshotPath);
				}
				return ((ClientNamenodeProtocolProtos.CreateSnapshotResponseProto)builder.Build()
					);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto DeleteSnapshot
			(RpcController controller, ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto
			 req)
		{
			try
			{
				server.DeleteSnapshot(req.GetSnapshotRoot(), req.GetSnapshotName());
				return VoidDeleteSnapshotResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.AllowSnapshotResponseProto AllowSnapshot
			(RpcController controller, ClientNamenodeProtocolProtos.AllowSnapshotRequestProto
			 req)
		{
			try
			{
				server.AllowSnapshot(req.GetSnapshotRoot());
				return VoidAllowSnapshotResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto DisallowSnapshot
			(RpcController controller, ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto
			 req)
		{
			try
			{
				server.DisallowSnapshot(req.GetSnapshotRoot());
				return VoidDisallowSnapshotResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RenameSnapshotResponseProto RenameSnapshot
			(RpcController controller, ClientNamenodeProtocolProtos.RenameSnapshotRequestProto
			 request)
		{
			try
			{
				server.RenameSnapshot(request.GetSnapshotRoot(), request.GetSnapshotOldName(), request
					.GetSnapshotNewName());
				return VoidRenameSnapshotResponse;
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto
			 GetSnapshottableDirListing(RpcController controller, ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto
			 request)
		{
			try
			{
				SnapshottableDirectoryStatus[] result = server.GetSnapshottableDirListing();
				if (result != null)
				{
					return ((ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto)ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto
						.NewBuilder().SetSnapshottableDirList(PBHelper.Convert(result)).Build());
				}
				else
				{
					return NullGetSnapshottableDirListingResponse;
				}
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto GetSnapshotDiffReport
			(RpcController controller, ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto
			 request)
		{
			try
			{
				SnapshotDiffReport report = server.GetSnapshotDiffReport(request.GetSnapshotRoot(
					), request.GetFromSnapshot(), request.GetToSnapshot());
				return ((ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto)ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto
					.NewBuilder().SetDiffReport(PBHelper.Convert(report)).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.IsFileClosedResponseProto IsFileClosed
			(RpcController controller, ClientNamenodeProtocolProtos.IsFileClosedRequestProto
			 request)
		{
			try
			{
				bool result = server.IsFileClosed(request.GetSrc());
				return ((ClientNamenodeProtocolProtos.IsFileClosedResponseProto)ClientNamenodeProtocolProtos.IsFileClosedResponseProto
					.NewBuilder().SetResult(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto AddCacheDirective
			(RpcController controller, ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto
			 request)
		{
			try
			{
				long id = server.AddCacheDirective(PBHelper.Convert(request.GetInfo()), PBHelper.
					ConvertCacheFlags(request.GetCacheFlags()));
				return ((ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto)ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto
					.NewBuilder().SetId(id).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto ModifyCacheDirective
			(RpcController controller, ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto
			 request)
		{
			try
			{
				server.ModifyCacheDirective(PBHelper.Convert(request.GetInfo()), PBHelper.ConvertCacheFlags
					(request.GetCacheFlags()));
				return ((ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto)ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto
					.NewBuilder().Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto RemoveCacheDirective
			(RpcController controller, ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto
			 request)
		{
			try
			{
				server.RemoveCacheDirective(request.GetId());
				return ((ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto)ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto
					.NewBuilder().Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto ListCacheDirectives
			(RpcController controller, ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto
			 request)
		{
			try
			{
				CacheDirectiveInfo filter = PBHelper.Convert(request.GetFilter());
				BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> entries = server.ListCacheDirectives
					(request.GetPrevId(), filter);
				ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto.Builder builder = ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto
					.NewBuilder();
				builder.SetHasMore(entries.HasMore());
				for (int i = 0; i < n; i++)
				{
					builder.AddElements(PBHelper.Convert(entries.Get(i)));
				}
				return ((ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto)builder.Build
					());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.AddCachePoolResponseProto AddCachePool
			(RpcController controller, ClientNamenodeProtocolProtos.AddCachePoolRequestProto
			 request)
		{
			try
			{
				server.AddCachePool(PBHelper.Convert(request.GetInfo()));
				return ((ClientNamenodeProtocolProtos.AddCachePoolResponseProto)ClientNamenodeProtocolProtos.AddCachePoolResponseProto
					.NewBuilder().Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto ModifyCachePool
			(RpcController controller, ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto
			 request)
		{
			try
			{
				server.ModifyCachePool(PBHelper.Convert(request.GetInfo()));
				return ((ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto)ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto
					.NewBuilder().Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto RemoveCachePool
			(RpcController controller, ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto
			 request)
		{
			try
			{
				server.RemoveCachePool(request.GetPoolName());
				return ((ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto)ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto
					.NewBuilder().Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.ListCachePoolsResponseProto ListCachePools
			(RpcController controller, ClientNamenodeProtocolProtos.ListCachePoolsRequestProto
			 request)
		{
			try
			{
				BatchedRemoteIterator.BatchedEntries<CachePoolEntry> entries = server.ListCachePools
					(request.GetPrevPoolName());
				ClientNamenodeProtocolProtos.ListCachePoolsResponseProto.Builder responseBuilder = 
					ClientNamenodeProtocolProtos.ListCachePoolsResponseProto.NewBuilder();
				responseBuilder.SetHasMore(entries.HasMore());
				for (int i = 0; i < n; i++)
				{
					responseBuilder.AddEntries(PBHelper.Convert(entries.Get(i)));
				}
				return ((ClientNamenodeProtocolProtos.ListCachePoolsResponseProto)responseBuilder
					.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual AclProtos.ModifyAclEntriesResponseProto ModifyAclEntries(RpcController
			 controller, AclProtos.ModifyAclEntriesRequestProto req)
		{
			try
			{
				server.ModifyAclEntries(req.GetSrc(), PBHelper.ConvertAclEntry(req.GetAclSpecList
					()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidModifyaclentriesResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual AclProtos.RemoveAclEntriesResponseProto RemoveAclEntries(RpcController
			 controller, AclProtos.RemoveAclEntriesRequestProto req)
		{
			try
			{
				server.RemoveAclEntries(req.GetSrc(), PBHelper.ConvertAclEntry(req.GetAclSpecList
					()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRemoveaclentriesResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual AclProtos.RemoveDefaultAclResponseProto RemoveDefaultAcl(RpcController
			 controller, AclProtos.RemoveDefaultAclRequestProto req)
		{
			try
			{
				server.RemoveDefaultAcl(req.GetSrc());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRemovedefaultaclResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual AclProtos.RemoveAclResponseProto RemoveAcl(RpcController controller
			, AclProtos.RemoveAclRequestProto req)
		{
			try
			{
				server.RemoveAcl(req.GetSrc());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRemoveaclResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual AclProtos.SetAclResponseProto SetAcl(RpcController controller, AclProtos.SetAclRequestProto
			 req)
		{
			try
			{
				server.SetAcl(req.GetSrc(), PBHelper.ConvertAclEntry(req.GetAclSpecList()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidSetaclResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual AclProtos.GetAclStatusResponseProto GetAclStatus(RpcController controller
			, AclProtos.GetAclStatusRequestProto req)
		{
			try
			{
				return PBHelper.Convert(server.GetAclStatus(req.GetSrc()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual EncryptionZonesProtos.CreateEncryptionZoneResponseProto CreateEncryptionZone
			(RpcController controller, EncryptionZonesProtos.CreateEncryptionZoneRequestProto
			 req)
		{
			try
			{
				server.CreateEncryptionZone(req.GetSrc(), req.GetKeyName());
				return ((EncryptionZonesProtos.CreateEncryptionZoneResponseProto)EncryptionZonesProtos.CreateEncryptionZoneResponseProto
					.NewBuilder().Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual EncryptionZonesProtos.GetEZForPathResponseProto GetEZForPath(RpcController
			 controller, EncryptionZonesProtos.GetEZForPathRequestProto req)
		{
			try
			{
				EncryptionZonesProtos.GetEZForPathResponseProto.Builder builder = EncryptionZonesProtos.GetEZForPathResponseProto
					.NewBuilder();
				EncryptionZone ret = server.GetEZForPath(req.GetSrc());
				if (ret != null)
				{
					builder.SetZone(PBHelper.Convert(ret));
				}
				return ((EncryptionZonesProtos.GetEZForPathResponseProto)builder.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual EncryptionZonesProtos.ListEncryptionZonesResponseProto ListEncryptionZones
			(RpcController controller, EncryptionZonesProtos.ListEncryptionZonesRequestProto
			 req)
		{
			try
			{
				BatchedRemoteIterator.BatchedEntries<EncryptionZone> entries = server.ListEncryptionZones
					(req.GetId());
				EncryptionZonesProtos.ListEncryptionZonesResponseProto.Builder builder = EncryptionZonesProtos.ListEncryptionZonesResponseProto
					.NewBuilder();
				builder.SetHasMore(entries.HasMore());
				for (int i = 0; i < entries.Size(); i++)
				{
					builder.AddZones(PBHelper.Convert(entries.Get(i)));
				}
				return ((EncryptionZonesProtos.ListEncryptionZonesResponseProto)builder.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual XAttrProtos.SetXAttrResponseProto SetXAttr(RpcController controller
			, XAttrProtos.SetXAttrRequestProto req)
		{
			try
			{
				server.SetXAttr(req.GetSrc(), PBHelper.ConvertXAttr(req.GetXAttr()), PBHelper.Convert
					(req.GetFlag()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidSetxattrResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual XAttrProtos.GetXAttrsResponseProto GetXAttrs(RpcController controller
			, XAttrProtos.GetXAttrsRequestProto req)
		{
			try
			{
				return PBHelper.ConvertXAttrsResponse(server.GetXAttrs(req.GetSrc(), PBHelper.ConvertXAttrs
					(req.GetXAttrsList())));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual XAttrProtos.ListXAttrsResponseProto ListXAttrs(RpcController controller
			, XAttrProtos.ListXAttrsRequestProto req)
		{
			try
			{
				return PBHelper.ConvertListXAttrsResponse(server.ListXAttrs(req.GetSrc()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual XAttrProtos.RemoveXAttrResponseProto RemoveXAttr(RpcController controller
			, XAttrProtos.RemoveXAttrRequestProto req)
		{
			try
			{
				server.RemoveXAttr(req.GetSrc(), PBHelper.ConvertXAttr(req.GetXAttr()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRemovexattrResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.CheckAccessResponseProto CheckAccess(
			RpcController controller, ClientNamenodeProtocolProtos.CheckAccessRequestProto req
			)
		{
			try
			{
				server.CheckAccess(req.GetPath(), PBHelper.Convert(req.GetMode()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidCheckaccessResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto SetStoragePolicy
			(RpcController controller, ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto
			 request)
		{
			try
			{
				server.SetStoragePolicy(request.GetSrc(), request.GetPolicyName());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidSetStoragePolicyResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto GetStoragePolicies
			(RpcController controller, ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto
			 request)
		{
			try
			{
				BlockStoragePolicy[] policies = server.GetStoragePolicies();
				ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.Builder builder = ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto
					.NewBuilder();
				if (policies == null)
				{
					return ((ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto)builder.Build
						());
				}
				foreach (BlockStoragePolicy policy in policies)
				{
					builder.AddPolicies(PBHelper.Convert(policy));
				}
				return ((ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto)builder.Build
					());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto GetCurrentEditLogTxid
			(RpcController controller, ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto
			 req)
		{
			try
			{
				return ((ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto)ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto
					.NewBuilder().SetTxid(server.GetCurrentEditLogTxid()).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto GetEditsFromTxid
			(RpcController controller, ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto
			 req)
		{
			try
			{
				return PBHelper.ConvertEditsResponse(server.GetEditsFromTxid(req.GetTxid()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
