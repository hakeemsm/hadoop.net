using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// This class forwards NN's ClientProtocol calls as RPC calls to the NN server
	/// while translating from the parameter types used in ClientProtocol to the
	/// new PB types.
	/// </summary>
	public class ClientNamenodeProtocolTranslatorPB : ProtocolMetaInterface, ClientProtocol
		, IDisposable, ProtocolTranslator
	{
		private readonly ClientNamenodeProtocolPB rpcProxy;

		internal static readonly ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto
			 VoidGetServerDefaultRequest = ((ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto
			)ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto.NewBuilder().Build()
			);

		private static readonly ClientNamenodeProtocolProtos.GetFsStatusRequestProto VoidGetFsstatusRequest
			 = ((ClientNamenodeProtocolProtos.GetFsStatusRequestProto)ClientNamenodeProtocolProtos.GetFsStatusRequestProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.SaveNamespaceRequestProto VoidSaveNamespaceRequest
			 = ((ClientNamenodeProtocolProtos.SaveNamespaceRequestProto)ClientNamenodeProtocolProtos.SaveNamespaceRequestProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.RollEditsRequestProto VoidRolleditsRequest
			 = ClientNamenodeProtocolProtos.RollEditsRequestProto.GetDefaultInstance();

		private static readonly ClientNamenodeProtocolProtos.RefreshNodesRequestProto VoidRefreshNodesRequest
			 = ((ClientNamenodeProtocolProtos.RefreshNodesRequestProto)ClientNamenodeProtocolProtos.RefreshNodesRequestProto
			.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto 
			VoidFinalizeUpgradeRequest = ((ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto
			)ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto.NewBuilder().Build());

		private static readonly ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto
			 VoidGetDataEncryptionkeyRequest = ((ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto
			)ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto.NewBuilder().Build
			());

		private static readonly ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto
			 VoidGetStoragePoliciesRequest = ((ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto
			)ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto.NewBuilder().Build(
			));

		public ClientNamenodeProtocolTranslatorPB(ClientNamenodeProtocolPB proxy)
		{
			rpcProxy = proxy;
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlocks GetBlockLocations(string src, long offset, long length
			)
		{
			ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req = ((ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto
				)ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto.NewBuilder().SetSrc(
				src).SetOffset(offset).SetLength(length).Build());
			try
			{
				ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto resp = rpcProxy.GetBlockLocations
					(null, req);
				return resp.HasLocations() ? PBHelper.Convert(resp.GetLocations()) : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FsServerDefaults GetServerDefaults()
		{
			ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req = VoidGetServerDefaultRequest;
			try
			{
				return PBHelper.Convert(rpcProxy.GetServerDefaults(null, req).GetServerDefaults()
					);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AlreadyBeingCreatedException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus Create(string src, FsPermission masked, string clientName
			, EnumSetWritable<CreateFlag> flag, bool createParent, short replication, long blockSize
			, CryptoProtocolVersion[] supportedVersions)
		{
			ClientNamenodeProtocolProtos.CreateRequestProto.Builder builder = ClientNamenodeProtocolProtos.CreateRequestProto
				.NewBuilder().SetSrc(src).SetMasked(PBHelper.Convert(masked)).SetClientName(clientName
				).SetCreateFlag(PBHelper.ConvertCreateFlag(flag)).SetCreateParent(createParent).
				SetReplication(replication).SetBlockSize(blockSize);
			builder.AddAllCryptoProtocolVersion(PBHelper.Convert(supportedVersions));
			ClientNamenodeProtocolProtos.CreateRequestProto req = ((ClientNamenodeProtocolProtos.CreateRequestProto
				)builder.Build());
			try
			{
				ClientNamenodeProtocolProtos.CreateResponseProto res = rpcProxy.Create(null, req);
				return res.HasFs() ? PBHelper.Convert(res.GetFs()) : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual bool Truncate(string src, long newLength, string clientName)
		{
			ClientNamenodeProtocolProtos.TruncateRequestProto req = ((ClientNamenodeProtocolProtos.TruncateRequestProto
				)ClientNamenodeProtocolProtos.TruncateRequestProto.NewBuilder().SetSrc(src).SetNewLength
				(newLength).SetClientName(clientName).Build());
			try
			{
				return rpcProxy.Truncate(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual LastBlockWithStatus Append(string src, string clientName, EnumSetWritable
			<CreateFlag> flag)
		{
			ClientNamenodeProtocolProtos.AppendRequestProto req = ((ClientNamenodeProtocolProtos.AppendRequestProto
				)ClientNamenodeProtocolProtos.AppendRequestProto.NewBuilder().SetSrc(src).SetClientName
				(clientName).SetFlag(PBHelper.ConvertCreateFlag(flag)).Build());
			try
			{
				ClientNamenodeProtocolProtos.AppendResponseProto res = rpcProxy.Append(null, req);
				LocatedBlock lastBlock = res.HasBlock() ? PBHelper.Convert(res.GetBlock()) : null;
				HdfsFileStatus stat = (res.HasStat()) ? PBHelper.Convert(res.GetStat()) : null;
				return new LastBlockWithStatus(lastBlock, stat);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetReplication(string src, short replication)
		{
			ClientNamenodeProtocolProtos.SetReplicationRequestProto req = ((ClientNamenodeProtocolProtos.SetReplicationRequestProto
				)ClientNamenodeProtocolProtos.SetReplicationRequestProto.NewBuilder().SetSrc(src
				).SetReplication(replication).Build());
			try
			{
				return rpcProxy.SetReplication(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetPermission(string src, FsPermission permission)
		{
			ClientNamenodeProtocolProtos.SetPermissionRequestProto req = ((ClientNamenodeProtocolProtos.SetPermissionRequestProto
				)ClientNamenodeProtocolProtos.SetPermissionRequestProto.NewBuilder().SetSrc(src)
				.SetPermission(PBHelper.Convert(permission)).Build());
			try
			{
				rpcProxy.SetPermission(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetOwner(string src, string username, string groupname)
		{
			ClientNamenodeProtocolProtos.SetOwnerRequestProto.Builder req = ClientNamenodeProtocolProtos.SetOwnerRequestProto
				.NewBuilder().SetSrc(src);
			if (username != null)
			{
				req.SetUsername(username);
			}
			if (groupname != null)
			{
				req.SetGroupname(groupname);
			}
			try
			{
				rpcProxy.SetOwner(null, ((ClientNamenodeProtocolProtos.SetOwnerRequestProto)req.Build
					()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AbandonBlock(ExtendedBlock b, long fileId, string src, string
			 holder)
		{
			ClientNamenodeProtocolProtos.AbandonBlockRequestProto req = ((ClientNamenodeProtocolProtos.AbandonBlockRequestProto
				)ClientNamenodeProtocolProtos.AbandonBlockRequestProto.NewBuilder().SetB(PBHelper
				.Convert(b)).SetSrc(src).SetHolder(holder).SetFileId(fileId).Build());
			try
			{
				rpcProxy.AbandonBlock(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NotReplicatedYetException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock AddBlock(string src, string clientName, ExtendedBlock
			 previous, DatanodeInfo[] excludeNodes, long fileId, string[] favoredNodes)
		{
			ClientNamenodeProtocolProtos.AddBlockRequestProto.Builder req = ClientNamenodeProtocolProtos.AddBlockRequestProto
				.NewBuilder().SetSrc(src).SetClientName(clientName).SetFileId(fileId);
			if (previous != null)
			{
				req.SetPrevious(PBHelper.Convert(previous));
			}
			if (excludeNodes != null)
			{
				req.AddAllExcludeNodes(PBHelper.Convert(excludeNodes));
			}
			if (favoredNodes != null)
			{
				req.AddAllFavoredNodes(Arrays.AsList(favoredNodes));
			}
			try
			{
				return PBHelper.Convert(rpcProxy.AddBlock(null, ((ClientNamenodeProtocolProtos.AddBlockRequestProto
					)req.Build())).GetBlock());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock GetAdditionalDatanode(string src, long fileId, ExtendedBlock
			 blk, DatanodeInfo[] existings, string[] existingStorageIDs, DatanodeInfo[] excludes
			, int numAdditionalNodes, string clientName)
		{
			ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto req = ((ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto
				)ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto.NewBuilder().SetSrc
				(src).SetFileId(fileId).SetBlk(PBHelper.Convert(blk)).AddAllExistings(PBHelper.Convert
				(existings)).AddAllExistingStorageUuids(Arrays.AsList(existingStorageIDs)).AddAllExcludes
				(PBHelper.Convert(excludes)).SetNumAdditionalNodes(numAdditionalNodes).SetClientName
				(clientName).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetAdditionalDatanode(null, req).GetBlock());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Complete(string src, string clientName, ExtendedBlock last, long
			 fileId)
		{
			ClientNamenodeProtocolProtos.CompleteRequestProto.Builder req = ClientNamenodeProtocolProtos.CompleteRequestProto
				.NewBuilder().SetSrc(src).SetClientName(clientName).SetFileId(fileId);
			if (last != null)
			{
				req.SetLast(PBHelper.Convert(last));
			}
			try
			{
				return rpcProxy.Complete(null, ((ClientNamenodeProtocolProtos.CompleteRequestProto
					)req.Build())).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportBadBlocks(LocatedBlock[] blocks)
		{
			ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto req = ((ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto
				)ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto.NewBuilder().AddAllBlocks
				(Arrays.AsList(PBHelper.ConvertLocatedBlock(blocks))).Build());
			try
			{
				rpcProxy.ReportBadBlocks(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Rename(string src, string dst)
		{
			ClientNamenodeProtocolProtos.RenameRequestProto req = ((ClientNamenodeProtocolProtos.RenameRequestProto
				)ClientNamenodeProtocolProtos.RenameRequestProto.NewBuilder().SetSrc(src).SetDst
				(dst).Build());
			try
			{
				return rpcProxy.Rename(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Rename2(string src, string dst, params Options.Rename[] options
			)
		{
			bool overwrite = false;
			if (options != null)
			{
				foreach (Options.Rename option in options)
				{
					if (option == Options.Rename.Overwrite)
					{
						overwrite = true;
					}
				}
			}
			ClientNamenodeProtocolProtos.Rename2RequestProto req = ((ClientNamenodeProtocolProtos.Rename2RequestProto
				)ClientNamenodeProtocolProtos.Rename2RequestProto.NewBuilder().SetSrc(src).SetDst
				(dst).SetOverwriteDest(overwrite).Build());
			try
			{
				rpcProxy.Rename2(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual void Concat(string trg, string[] srcs)
		{
			ClientNamenodeProtocolProtos.ConcatRequestProto req = ((ClientNamenodeProtocolProtos.ConcatRequestProto
				)ClientNamenodeProtocolProtos.ConcatRequestProto.NewBuilder().SetTrg(trg).AddAllSrcs
				(Arrays.AsList(srcs)).Build());
			try
			{
				rpcProxy.Concat(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Delete(string src, bool recursive)
		{
			ClientNamenodeProtocolProtos.DeleteRequestProto req = ((ClientNamenodeProtocolProtos.DeleteRequestProto
				)ClientNamenodeProtocolProtos.DeleteRequestProto.NewBuilder().SetSrc(src).SetRecursive
				(recursive).Build());
			try
			{
				return rpcProxy.Delete(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Mkdirs(string src, FsPermission masked, bool createParent)
		{
			ClientNamenodeProtocolProtos.MkdirsRequestProto req = ((ClientNamenodeProtocolProtos.MkdirsRequestProto
				)ClientNamenodeProtocolProtos.MkdirsRequestProto.NewBuilder().SetSrc(src).SetMasked
				(PBHelper.Convert(masked)).SetCreateParent(createParent).Build());
			try
			{
				return rpcProxy.Mkdirs(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual DirectoryListing GetListing(string src, byte[] startAfter, bool needLocation
			)
		{
			ClientNamenodeProtocolProtos.GetListingRequestProto req = ((ClientNamenodeProtocolProtos.GetListingRequestProto
				)ClientNamenodeProtocolProtos.GetListingRequestProto.NewBuilder().SetSrc(src).SetStartAfter
				(ByteString.CopyFrom(startAfter)).SetNeedLocation(needLocation).Build());
			try
			{
				ClientNamenodeProtocolProtos.GetListingResponseProto result = rpcProxy.GetListing
					(null, req);
				if (result.HasDirList())
				{
					return PBHelper.Convert(result.GetDirList());
				}
				return null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RenewLease(string clientName)
		{
			ClientNamenodeProtocolProtos.RenewLeaseRequestProto req = ((ClientNamenodeProtocolProtos.RenewLeaseRequestProto
				)ClientNamenodeProtocolProtos.RenewLeaseRequestProto.NewBuilder().SetClientName(
				clientName).Build());
			try
			{
				rpcProxy.RenewLease(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool RecoverLease(string src, string clientName)
		{
			ClientNamenodeProtocolProtos.RecoverLeaseRequestProto req = ((ClientNamenodeProtocolProtos.RecoverLeaseRequestProto
				)ClientNamenodeProtocolProtos.RecoverLeaseRequestProto.NewBuilder().SetSrc(src).
				SetClientName(clientName).Build());
			try
			{
				return rpcProxy.RecoverLease(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long[] GetStats()
		{
			try
			{
				return PBHelper.Convert(rpcProxy.GetFsStats(null, VoidGetFsstatusRequest));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeInfo[] GetDatanodeReport(HdfsConstants.DatanodeReportType 
			type)
		{
			ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto req = ((ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto
				)ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto.NewBuilder().SetType
				(PBHelper.Convert(type)).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetDatanodeReport(null, req).GetDiList());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeStorageReport[] GetDatanodeStorageReport(HdfsConstants.DatanodeReportType
			 type)
		{
			ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto req = ((ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto
				)ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto.NewBuilder().
				SetType(PBHelper.Convert(type)).Build());
			try
			{
				return PBHelper.ConvertDatanodeStorageReports(rpcProxy.GetDatanodeStorageReport(null
					, req).GetDatanodeStorageReportsList());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual long GetPreferredBlockSize(string filename)
		{
			ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto req = ((ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto
				)ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto.NewBuilder().SetFilename
				(filename).Build());
			try
			{
				return rpcProxy.GetPreferredBlockSize(null, req).GetBsize();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action, bool isChecked
			)
		{
			ClientNamenodeProtocolProtos.SetSafeModeRequestProto req = ((ClientNamenodeProtocolProtos.SetSafeModeRequestProto
				)ClientNamenodeProtocolProtos.SetSafeModeRequestProto.NewBuilder().SetAction(PBHelper
				.Convert(action)).SetChecked(isChecked).Build());
			try
			{
				return rpcProxy.SetSafeMode(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveNamespace()
		{
			try
			{
				rpcProxy.SaveNamespace(null, VoidSaveNamespaceRequest);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual long RollEdits()
		{
			try
			{
				ClientNamenodeProtocolProtos.RollEditsResponseProto resp = rpcProxy.RollEdits(null
					, VoidRolleditsRequest);
				return resp.GetNewSegmentTxId();
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestoreFailedStorage(string arg)
		{
			ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto req = ((ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto
				)ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto.NewBuilder().SetArg
				(arg).Build());
			try
			{
				return rpcProxy.RestoreFailedStorage(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNodes()
		{
			try
			{
				rpcProxy.RefreshNodes(null, VoidRefreshNodesRequest);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void FinalizeUpgrade()
		{
			try
			{
				rpcProxy.FinalizeUpgrade(null, VoidFinalizeUpgradeRequest);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RollingUpgradeInfo RollingUpgrade(HdfsConstants.RollingUpgradeAction
			 action)
		{
			ClientNamenodeProtocolProtos.RollingUpgradeRequestProto r = ((ClientNamenodeProtocolProtos.RollingUpgradeRequestProto
				)ClientNamenodeProtocolProtos.RollingUpgradeRequestProto.NewBuilder().SetAction(
				PBHelper.Convert(action)).Build());
			try
			{
				ClientNamenodeProtocolProtos.RollingUpgradeResponseProto proto = rpcProxy.RollingUpgrade
					(null, r);
				if (proto.HasRollingUpgradeInfo())
				{
					return PBHelper.Convert(proto.GetRollingUpgradeInfo());
				}
				return null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CorruptFileBlocks ListCorruptFileBlocks(string path, string cookie
			)
		{
			ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto.Builder req = ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto
				.NewBuilder().SetPath(path);
			if (cookie != null)
			{
				req.SetCookie(cookie);
			}
			try
			{
				return PBHelper.Convert(rpcProxy.ListCorruptFileBlocks(null, ((ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto
					)req.Build())).GetCorrupt());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void MetaSave(string filename)
		{
			ClientNamenodeProtocolProtos.MetaSaveRequestProto req = ((ClientNamenodeProtocolProtos.MetaSaveRequestProto
				)ClientNamenodeProtocolProtos.MetaSaveRequestProto.NewBuilder().SetFilename(filename
				).Build());
			try
			{
				rpcProxy.MetaSave(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus GetFileInfo(string src)
		{
			ClientNamenodeProtocolProtos.GetFileInfoRequestProto req = ((ClientNamenodeProtocolProtos.GetFileInfoRequestProto
				)ClientNamenodeProtocolProtos.GetFileInfoRequestProto.NewBuilder().SetSrc(src).Build
				());
			try
			{
				ClientNamenodeProtocolProtos.GetFileInfoResponseProto res = rpcProxy.GetFileInfo(
					null, req);
				return res.HasFs() ? PBHelper.Convert(res.GetFs()) : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus GetFileLinkInfo(string src)
		{
			ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto req = ((ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto
				)ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto.NewBuilder().SetSrc(src
				).Build());
			try
			{
				ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto result = rpcProxy.GetFileLinkInfo
					(null, req);
				return result.HasFs() ? PBHelper.Convert(rpcProxy.GetFileLinkInfo(null, req).GetFs
					()) : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ContentSummary GetContentSummary(string path)
		{
			ClientNamenodeProtocolProtos.GetContentSummaryRequestProto req = ((ClientNamenodeProtocolProtos.GetContentSummaryRequestProto
				)ClientNamenodeProtocolProtos.GetContentSummaryRequestProto.NewBuilder().SetPath
				(path).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetContentSummary(null, req).GetSummary());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetQuota(string path, long namespaceQuota, long storagespaceQuota
			, StorageType type)
		{
			ClientNamenodeProtocolProtos.SetQuotaRequestProto.Builder builder = ClientNamenodeProtocolProtos.SetQuotaRequestProto
				.NewBuilder().SetPath(path).SetNamespaceQuota(namespaceQuota).SetStoragespaceQuota
				(storagespaceQuota);
			if (type != null)
			{
				builder.SetStorageType(PBHelper.ConvertStorageType(type));
			}
			ClientNamenodeProtocolProtos.SetQuotaRequestProto req = ((ClientNamenodeProtocolProtos.SetQuotaRequestProto
				)builder.Build());
			try
			{
				rpcProxy.SetQuota(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Fsync(string src, long fileId, string client, long lastBlockLength
			)
		{
			ClientNamenodeProtocolProtos.FsyncRequestProto req = ((ClientNamenodeProtocolProtos.FsyncRequestProto
				)ClientNamenodeProtocolProtos.FsyncRequestProto.NewBuilder().SetSrc(src).SetClient
				(client).SetLastBlockLength(lastBlockLength).SetFileId(fileId).Build());
			try
			{
				rpcProxy.Fsync(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetTimes(string src, long mtime, long atime)
		{
			ClientNamenodeProtocolProtos.SetTimesRequestProto req = ((ClientNamenodeProtocolProtos.SetTimesRequestProto
				)ClientNamenodeProtocolProtos.SetTimesRequestProto.NewBuilder().SetSrc(src).SetMtime
				(mtime).SetAtime(atime).Build());
			try
			{
				rpcProxy.SetTimes(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateSymlink(string target, string link, FsPermission dirPerm
			, bool createParent)
		{
			ClientNamenodeProtocolProtos.CreateSymlinkRequestProto req = ((ClientNamenodeProtocolProtos.CreateSymlinkRequestProto
				)ClientNamenodeProtocolProtos.CreateSymlinkRequestProto.NewBuilder().SetTarget(target
				).SetLink(link).SetDirPerm(PBHelper.Convert(dirPerm)).SetCreateParent(createParent
				).Build());
			try
			{
				rpcProxy.CreateSymlink(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual string GetLinkTarget(string path)
		{
			ClientNamenodeProtocolProtos.GetLinkTargetRequestProto req = ((ClientNamenodeProtocolProtos.GetLinkTargetRequestProto
				)ClientNamenodeProtocolProtos.GetLinkTargetRequestProto.NewBuilder().SetPath(path
				).Build());
			try
			{
				ClientNamenodeProtocolProtos.GetLinkTargetResponseProto rsp = rpcProxy.GetLinkTarget
					(null, req);
				return rsp.HasTargetPath() ? rsp.GetTargetPath() : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock UpdateBlockForPipeline(ExtendedBlock block, string clientName
			)
		{
			ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto req = ((ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto
				)ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto.NewBuilder().SetBlock
				(PBHelper.Convert(block)).SetClientName(clientName).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.UpdateBlockForPipeline(null, req).GetBlock());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdatePipeline(string clientName, ExtendedBlock oldBlock, ExtendedBlock
			 newBlock, DatanodeID[] newNodes, string[] storageIDs)
		{
			ClientNamenodeProtocolProtos.UpdatePipelineRequestProto req = ((ClientNamenodeProtocolProtos.UpdatePipelineRequestProto
				)ClientNamenodeProtocolProtos.UpdatePipelineRequestProto.NewBuilder().SetClientName
				(clientName).SetOldBlock(PBHelper.Convert(oldBlock)).SetNewBlock(PBHelper.Convert
				(newBlock)).AddAllNewNodes(Arrays.AsList(PBHelper.Convert(newNodes))).AddAllStorageIDs
				(storageIDs == null ? null : Arrays.AsList(storageIDs)).Build());
			try
			{
				rpcProxy.UpdatePipeline(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			GetDelegationToken(Text renewer)
		{
			SecurityProtos.GetDelegationTokenRequestProto req = ((SecurityProtos.GetDelegationTokenRequestProto
				)SecurityProtos.GetDelegationTokenRequestProto.NewBuilder().SetRenewer(renewer.ToString
				()).Build());
			try
			{
				SecurityProtos.GetDelegationTokenResponseProto resp = rpcProxy.GetDelegationToken
					(null, req);
				return resp.HasToken() ? PBHelper.ConvertDelegationToken(resp.GetToken()) : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			SecurityProtos.RenewDelegationTokenRequestProto req = ((SecurityProtos.RenewDelegationTokenRequestProto
				)SecurityProtos.RenewDelegationTokenRequestProto.NewBuilder().SetToken(PBHelper.
				Convert(token)).Build());
			try
			{
				return rpcProxy.RenewDelegationToken(null, req).GetNewExpiryTime();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token)
		{
			SecurityProtos.CancelDelegationTokenRequestProto req = ((SecurityProtos.CancelDelegationTokenRequestProto
				)SecurityProtos.CancelDelegationTokenRequestProto.NewBuilder().SetToken(PBHelper
				.Convert(token)).Build());
			try
			{
				rpcProxy.CancelDelegationToken(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetBalancerBandwidth(long bandwidth)
		{
			ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto req = ((ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto
				)ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto.NewBuilder().SetBandwidth
				(bandwidth).Build());
			try
			{
				rpcProxy.SetBalancerBandwidth(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(ClientNamenodeProtocolPB)
				, RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(ClientNamenodeProtocolPB
				)), methodName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DataEncryptionKey GetDataEncryptionKey()
		{
			try
			{
				ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto rsp = rpcProxy.GetDataEncryptionKey
					(null, VoidGetDataEncryptionkeyRequest);
				return rsp.HasDataEncryptionKey() ? PBHelper.Convert(rsp.GetDataEncryptionKey()) : 
					null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsFileClosed(string src)
		{
			ClientNamenodeProtocolProtos.IsFileClosedRequestProto req = ((ClientNamenodeProtocolProtos.IsFileClosedRequestProto
				)ClientNamenodeProtocolProtos.IsFileClosedRequestProto.NewBuilder().SetSrc(src).
				Build());
			try
			{
				return rpcProxy.IsFileClosed(null, req).GetResult();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		public virtual object GetUnderlyingProxyObject()
		{
			return rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string CreateSnapshot(string snapshotRoot, string snapshotName)
		{
			ClientNamenodeProtocolProtos.CreateSnapshotRequestProto.Builder builder = ClientNamenodeProtocolProtos.CreateSnapshotRequestProto
				.NewBuilder().SetSnapshotRoot(snapshotRoot);
			if (snapshotName != null)
			{
				builder.SetSnapshotName(snapshotName);
			}
			ClientNamenodeProtocolProtos.CreateSnapshotRequestProto req = ((ClientNamenodeProtocolProtos.CreateSnapshotRequestProto
				)builder.Build());
			try
			{
				return rpcProxy.CreateSnapshot(null, req).GetSnapshotPath();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteSnapshot(string snapshotRoot, string snapshotName)
		{
			ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto req = ((ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto
				)ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto.NewBuilder().SetSnapshotRoot
				(snapshotRoot).SetSnapshotName(snapshotName).Build());
			try
			{
				rpcProxy.DeleteSnapshot(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AllowSnapshot(string snapshotRoot)
		{
			ClientNamenodeProtocolProtos.AllowSnapshotRequestProto req = ((ClientNamenodeProtocolProtos.AllowSnapshotRequestProto
				)ClientNamenodeProtocolProtos.AllowSnapshotRequestProto.NewBuilder().SetSnapshotRoot
				(snapshotRoot).Build());
			try
			{
				rpcProxy.AllowSnapshot(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DisallowSnapshot(string snapshotRoot)
		{
			ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto req = ((ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto
				)ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto.NewBuilder().SetSnapshotRoot
				(snapshotRoot).Build());
			try
			{
				rpcProxy.DisallowSnapshot(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RenameSnapshot(string snapshotRoot, string snapshotOldName, string
			 snapshotNewName)
		{
			ClientNamenodeProtocolProtos.RenameSnapshotRequestProto req = ((ClientNamenodeProtocolProtos.RenameSnapshotRequestProto
				)ClientNamenodeProtocolProtos.RenameSnapshotRequestProto.NewBuilder().SetSnapshotRoot
				(snapshotRoot).SetSnapshotOldName(snapshotOldName).SetSnapshotNewName(snapshotNewName
				).Build());
			try
			{
				rpcProxy.RenameSnapshot(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshottableDirectoryStatus[] GetSnapshottableDirListing()
		{
			ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto req = ((ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto
				)ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto.NewBuilder(
				).Build());
			try
			{
				ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto result = rpcProxy
					.GetSnapshottableDirListing(null, req);
				if (result.HasSnapshottableDirList())
				{
					return PBHelper.Convert(result.GetSnapshottableDirList());
				}
				return null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshotDiffReport GetSnapshotDiffReport(string snapshotRoot, string
			 fromSnapshot, string toSnapshot)
		{
			ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto req = ((ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto
				)ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto.NewBuilder().SetSnapshotRoot
				(snapshotRoot).SetFromSnapshot(fromSnapshot).SetToSnapshot(toSnapshot).Build());
			try
			{
				ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto result = rpcProxy
					.GetSnapshotDiffReport(null, req);
				return PBHelper.Convert(result.GetDiffReport());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long AddCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag
			> flags)
		{
			try
			{
				ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto.Builder builder = ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto
					.NewBuilder().SetInfo(PBHelper.Convert(directive));
				if (!flags.IsEmpty())
				{
					builder.SetCacheFlags(PBHelper.ConvertCacheFlags(flags));
				}
				return rpcProxy.AddCacheDirective(null, ((ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto
					)builder.Build())).GetId();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag
			> flags)
		{
			try
			{
				ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto.Builder builder = ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto
					.NewBuilder().SetInfo(PBHelper.Convert(directive));
				if (!flags.IsEmpty())
				{
					builder.SetCacheFlags(PBHelper.ConvertCacheFlags(flags));
				}
				rpcProxy.ModifyCacheDirective(null, ((ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto
					)builder.Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveCacheDirective(long id)
		{
			try
			{
				rpcProxy.RemoveCacheDirective(null, ((ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto
					)ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto.NewBuilder().SetId
					(id).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		private class BatchedCacheEntries : BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry
			>
		{
			private readonly ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto response;

			internal BatchedCacheEntries(ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto
				 response)
			{
				this.response = response;
			}

			public virtual CacheDirectiveEntry Get(int i)
			{
				return PBHelper.Convert(response.GetElements(i));
			}

			public virtual int Size()
			{
				return response.GetElementsCount();
			}

			public virtual bool HasMore()
			{
				return response.GetHasMore();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> ListCacheDirectives
			(long prevId, CacheDirectiveInfo filter)
		{
			if (filter == null)
			{
				filter = new CacheDirectiveInfo.Builder().Build();
			}
			try
			{
				return new ClientNamenodeProtocolTranslatorPB.BatchedCacheEntries(rpcProxy.ListCacheDirectives
					(null, ((ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto)ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto
					.NewBuilder().SetPrevId(prevId).SetFilter(PBHelper.Convert(filter)).Build())));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AddCachePool(CachePoolInfo info)
		{
			ClientNamenodeProtocolProtos.AddCachePoolRequestProto.Builder builder = ClientNamenodeProtocolProtos.AddCachePoolRequestProto
				.NewBuilder();
			builder.SetInfo(PBHelper.Convert(info));
			try
			{
				rpcProxy.AddCachePool(null, ((ClientNamenodeProtocolProtos.AddCachePoolRequestProto
					)builder.Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCachePool(CachePoolInfo req)
		{
			ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto.Builder builder = ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto
				.NewBuilder();
			builder.SetInfo(PBHelper.Convert(req));
			try
			{
				rpcProxy.ModifyCachePool(null, ((ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto
					)builder.Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveCachePool(string cachePoolName)
		{
			try
			{
				rpcProxy.RemoveCachePool(null, ((ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto
					)ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto.NewBuilder().SetPoolName
					(cachePoolName).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		private class BatchedCachePoolEntries : BatchedRemoteIterator.BatchedEntries<CachePoolEntry
			>
		{
			private readonly ClientNamenodeProtocolProtos.ListCachePoolsResponseProto proto;

			public BatchedCachePoolEntries(ClientNamenodeProtocolProtos.ListCachePoolsResponseProto
				 proto)
			{
				this.proto = proto;
			}

			public virtual CachePoolEntry Get(int i)
			{
				ClientNamenodeProtocolProtos.CachePoolEntryProto elem = proto.GetEntries(i);
				return PBHelper.Convert(elem);
			}

			public virtual int Size()
			{
				return proto.GetEntriesCount();
			}

			public virtual bool HasMore()
			{
				return proto.GetHasMore();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BatchedRemoteIterator.BatchedEntries<CachePoolEntry> ListCachePools
			(string prevKey)
		{
			try
			{
				return new ClientNamenodeProtocolTranslatorPB.BatchedCachePoolEntries(rpcProxy.ListCachePools
					(null, ((ClientNamenodeProtocolProtos.ListCachePoolsRequestProto)ClientNamenodeProtocolProtos.ListCachePoolsRequestProto
					.NewBuilder().SetPrevPoolName(prevKey).Build())));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyAclEntries(string src, IList<AclEntry> aclSpec)
		{
			AclProtos.ModifyAclEntriesRequestProto req = ((AclProtos.ModifyAclEntriesRequestProto
				)AclProtos.ModifyAclEntriesRequestProto.NewBuilder().SetSrc(src).AddAllAclSpec(PBHelper
				.ConvertAclEntryProto(aclSpec)).Build());
			try
			{
				rpcProxy.ModifyAclEntries(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveAclEntries(string src, IList<AclEntry> aclSpec)
		{
			AclProtos.RemoveAclEntriesRequestProto req = ((AclProtos.RemoveAclEntriesRequestProto
				)AclProtos.RemoveAclEntriesRequestProto.NewBuilder().SetSrc(src).AddAllAclSpec(PBHelper
				.ConvertAclEntryProto(aclSpec)).Build());
			try
			{
				rpcProxy.RemoveAclEntries(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveDefaultAcl(string src)
		{
			AclProtos.RemoveDefaultAclRequestProto req = ((AclProtos.RemoveDefaultAclRequestProto
				)AclProtos.RemoveDefaultAclRequestProto.NewBuilder().SetSrc(src).Build());
			try
			{
				rpcProxy.RemoveDefaultAcl(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveAcl(string src)
		{
			AclProtos.RemoveAclRequestProto req = ((AclProtos.RemoveAclRequestProto)AclProtos.RemoveAclRequestProto
				.NewBuilder().SetSrc(src).Build());
			try
			{
				rpcProxy.RemoveAcl(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetAcl(string src, IList<AclEntry> aclSpec)
		{
			AclProtos.SetAclRequestProto req = ((AclProtos.SetAclRequestProto)AclProtos.SetAclRequestProto
				.NewBuilder().SetSrc(src).AddAllAclSpec(PBHelper.ConvertAclEntryProto(aclSpec)).
				Build());
			try
			{
				rpcProxy.SetAcl(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual AclStatus GetAclStatus(string src)
		{
			AclProtos.GetAclStatusRequestProto req = ((AclProtos.GetAclStatusRequestProto)AclProtos.GetAclStatusRequestProto
				.NewBuilder().SetSrc(src).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetAclStatus(null, req));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateEncryptionZone(string src, string keyName)
		{
			EncryptionZonesProtos.CreateEncryptionZoneRequestProto.Builder builder = EncryptionZonesProtos.CreateEncryptionZoneRequestProto
				.NewBuilder();
			builder.SetSrc(src);
			if (keyName != null && !keyName.IsEmpty())
			{
				builder.SetKeyName(keyName);
			}
			EncryptionZonesProtos.CreateEncryptionZoneRequestProto req = ((EncryptionZonesProtos.CreateEncryptionZoneRequestProto
				)builder.Build());
			try
			{
				rpcProxy.CreateEncryptionZone(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual EncryptionZone GetEZForPath(string src)
		{
			EncryptionZonesProtos.GetEZForPathRequestProto.Builder builder = EncryptionZonesProtos.GetEZForPathRequestProto
				.NewBuilder();
			builder.SetSrc(src);
			EncryptionZonesProtos.GetEZForPathRequestProto req = ((EncryptionZonesProtos.GetEZForPathRequestProto
				)builder.Build());
			try
			{
				EncryptionZonesProtos.GetEZForPathResponseProto response = rpcProxy.GetEZForPath(
					null, req);
				if (response.HasZone())
				{
					return PBHelper.Convert(response.GetZone());
				}
				else
				{
					return null;
				}
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BatchedRemoteIterator.BatchedEntries<EncryptionZone> ListEncryptionZones
			(long id)
		{
			EncryptionZonesProtos.ListEncryptionZonesRequestProto req = ((EncryptionZonesProtos.ListEncryptionZonesRequestProto
				)EncryptionZonesProtos.ListEncryptionZonesRequestProto.NewBuilder().SetId(id).Build
				());
			try
			{
				EncryptionZonesProtos.ListEncryptionZonesResponseProto response = rpcProxy.ListEncryptionZones
					(null, req);
				IList<EncryptionZone> elements = Lists.NewArrayListWithCapacity(response.GetZonesCount
					());
				foreach (EncryptionZonesProtos.EncryptionZoneProto p in response.GetZonesList())
				{
					elements.AddItem(PBHelper.Convert(p));
				}
				return new BatchedRemoteIterator.BatchedListEntries<EncryptionZone>(elements, response
					.GetHasMore());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetXAttr(string src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
		{
			XAttrProtos.SetXAttrRequestProto req = ((XAttrProtos.SetXAttrRequestProto)XAttrProtos.SetXAttrRequestProto
				.NewBuilder().SetSrc(src).SetXAttr(PBHelper.ConvertXAttrProto(xAttr)).SetFlag(PBHelper
				.Convert(flag)).Build());
			try
			{
				rpcProxy.SetXAttr(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<XAttr> GetXAttrs(string src, IList<XAttr> xAttrs)
		{
			XAttrProtos.GetXAttrsRequestProto.Builder builder = XAttrProtos.GetXAttrsRequestProto
				.NewBuilder();
			builder.SetSrc(src);
			if (xAttrs != null)
			{
				builder.AddAllXAttrs(PBHelper.ConvertXAttrProto(xAttrs));
			}
			XAttrProtos.GetXAttrsRequestProto req = ((XAttrProtos.GetXAttrsRequestProto)builder
				.Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetXAttrs(null, req));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<XAttr> ListXAttrs(string src)
		{
			XAttrProtos.ListXAttrsRequestProto.Builder builder = XAttrProtos.ListXAttrsRequestProto
				.NewBuilder();
			builder.SetSrc(src);
			XAttrProtos.ListXAttrsRequestProto req = ((XAttrProtos.ListXAttrsRequestProto)builder
				.Build());
			try
			{
				return PBHelper.Convert(rpcProxy.ListXAttrs(null, req));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveXAttr(string src, XAttr xAttr)
		{
			XAttrProtos.RemoveXAttrRequestProto req = ((XAttrProtos.RemoveXAttrRequestProto)XAttrProtos.RemoveXAttrRequestProto
				.NewBuilder().SetSrc(src).SetXAttr(PBHelper.ConvertXAttrProto(xAttr)).Build());
			try
			{
				rpcProxy.RemoveXAttr(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckAccess(string path, FsAction mode)
		{
			ClientNamenodeProtocolProtos.CheckAccessRequestProto req = ((ClientNamenodeProtocolProtos.CheckAccessRequestProto
				)ClientNamenodeProtocolProtos.CheckAccessRequestProto.NewBuilder().SetPath(path)
				.SetMode(PBHelper.Convert(mode)).Build());
			try
			{
				rpcProxy.CheckAccess(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetStoragePolicy(string src, string policyName)
		{
			ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto req = ((ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto
				)ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto.NewBuilder().SetSrc(src
				).SetPolicyName(policyName).Build());
			try
			{
				rpcProxy.SetStoragePolicy(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BlockStoragePolicy[] GetStoragePolicies()
		{
			try
			{
				ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto response = rpcProxy.
					GetStoragePolicies(null, VoidGetStoragePoliciesRequest);
				return PBHelper.ConvertStoragePolicies(response.GetPoliciesList());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetCurrentEditLogTxid()
		{
			ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto req = ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto
				.GetDefaultInstance();
			try
			{
				return rpcProxy.GetCurrentEditLogTxid(null, req).GetTxid();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual EventBatchList GetEditsFromTxid(long txid)
		{
			ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto req = ((ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto
				)ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto.NewBuilder().SetTxid(
				txid).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetEditsFromTxid(null, req));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}
	}
}
