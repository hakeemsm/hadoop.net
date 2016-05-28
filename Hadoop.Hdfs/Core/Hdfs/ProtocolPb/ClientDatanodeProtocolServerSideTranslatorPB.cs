using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// Implementation for protobuf service that forwards requests
	/// received on
	/// <see cref="ClientDatanodeProtocolPB"/>
	/// to the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientDatanodeProtocol"/>
	/// server implementation.
	/// </summary>
	public class ClientDatanodeProtocolServerSideTranslatorPB : ClientDatanodeProtocolPB
	{
		private static readonly ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto
			 RefreshNamenodeResp = ((ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto
			)ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto.NewBuilder().Build()
			);

		private static readonly ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto
			 DeleteBlockpoolResp = ((ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto
			)ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto.NewBuilder().Build());

		private static readonly ClientDatanodeProtocolProtos.ShutdownDatanodeResponseProto
			 ShutdownDatanodeResp = ((ClientDatanodeProtocolProtos.ShutdownDatanodeResponseProto
			)ClientDatanodeProtocolProtos.ShutdownDatanodeResponseProto.NewBuilder().Build()
			);

		private static readonly ClientDatanodeProtocolProtos.StartReconfigurationResponseProto
			 StartReconfigResp = ((ClientDatanodeProtocolProtos.StartReconfigurationResponseProto
			)ClientDatanodeProtocolProtos.StartReconfigurationResponseProto.NewBuilder().Build
			());

		private static readonly ClientDatanodeProtocolProtos.TriggerBlockReportResponseProto
			 TriggerBlockReportResp = ((ClientDatanodeProtocolProtos.TriggerBlockReportResponseProto
			)ClientDatanodeProtocolProtos.TriggerBlockReportResponseProto.NewBuilder().Build
			());

		private readonly ClientDatanodeProtocol impl;

		public ClientDatanodeProtocolServerSideTranslatorPB(ClientDatanodeProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto 
			GetReplicaVisibleLength(RpcController unused, ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto
			 request)
		{
			long len;
			try
			{
				len = impl.GetReplicaVisibleLength(PBHelper.Convert(request.GetBlock()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto)ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto
				.NewBuilder().SetLength(len).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto RefreshNamenodes
			(RpcController unused, ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto
			 request)
		{
			try
			{
				impl.RefreshNamenodes();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return RefreshNamenodeResp;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto DeleteBlockPool
			(RpcController unused, ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto 
			request)
		{
			try
			{
				impl.DeleteBlockPool(request.GetBlockPool(), request.GetForce());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return DeleteBlockpoolResp;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto GetBlockLocalPathInfo
			(RpcController unused, ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto
			 request)
		{
			BlockLocalPathInfo resp;
			try
			{
				resp = impl.GetBlockLocalPathInfo(PBHelper.Convert(request.GetBlock()), PBHelper.
					Convert(request.GetToken()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto)ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto
				.NewBuilder().SetBlock(PBHelper.Convert(resp.GetBlock())).SetLocalPath(resp.GetBlockPath
				()).SetLocalMetaPath(resp.GetMetaPath()).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto GetHdfsBlockLocations
			(RpcController controller, ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto
			 request)
		{
			HdfsBlocksMetadata resp;
			try
			{
				string poolId = request.GetBlockPoolId();
				IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>> tokens = new 
					AList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>>(request.GetTokensCount
					());
				foreach (SecurityProtos.TokenProto b in request.GetTokensList())
				{
					tokens.AddItem(PBHelper.Convert(b));
				}
				long[] blockIds = Longs.ToArray(request.GetBlockIdsList());
				// Call the real implementation
				resp = impl.GetHdfsBlocksMetadata(poolId, blockIds, tokens);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			IList<ByteString> volumeIdsByteStrings = new AList<ByteString>(resp.GetVolumeIds(
				).Count);
			foreach (byte[] b_1 in resp.GetVolumeIds())
			{
				volumeIdsByteStrings.AddItem(ByteString.CopyFrom(b_1));
			}
			// Build and return the response
			ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto.Builder builder = 
				ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto.NewBuilder();
			builder.AddAllVolumeIds(volumeIdsByteStrings);
			builder.AddAllVolumeIndexes(resp.GetVolumeIndexes());
			return ((ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto)builder.
				Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.ShutdownDatanodeResponseProto ShutdownDatanode
			(RpcController unused, ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto
			 request)
		{
			try
			{
				impl.ShutdownDatanode(request.GetForUpgrade());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ShutdownDatanodeResp;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto GetDatanodeInfo
			(RpcController unused, ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto 
			request)
		{
			ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto res;
			try
			{
				res = ((ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto)ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto
					.NewBuilder().SetLocalInfo(PBHelper.Convert(impl.GetDatanodeInfo())).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return res;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.StartReconfigurationResponseProto StartReconfiguration
			(RpcController unused, ClientDatanodeProtocolProtos.StartReconfigurationRequestProto
			 request)
		{
			try
			{
				impl.StartReconfiguration();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return StartReconfigResp;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.GetReconfigurationStatusResponseProto
			 GetReconfigurationStatus(RpcController unused, ClientDatanodeProtocolProtos.GetReconfigurationStatusRequestProto
			 request)
		{
			ClientDatanodeProtocolProtos.GetReconfigurationStatusResponseProto.Builder builder
				 = ClientDatanodeProtocolProtos.GetReconfigurationStatusResponseProto.NewBuilder
				();
			try
			{
				ReconfigurationTaskStatus status = impl.GetReconfigurationStatus();
				builder.SetStartTime(status.GetStartTime());
				if (status.Stopped())
				{
					builder.SetEndTime(status.GetEndTime());
					System.Diagnostics.Debug.Assert(status.GetStatus() != null);
					foreach (KeyValuePair<ReconfigurationUtil.PropertyChange, Optional<string>> result
						 in status.GetStatus())
					{
						ClientDatanodeProtocolProtos.GetReconfigurationStatusConfigChangeProto.Builder changeBuilder
							 = ClientDatanodeProtocolProtos.GetReconfigurationStatusConfigChangeProto.NewBuilder
							();
						ReconfigurationUtil.PropertyChange change = result.Key;
						changeBuilder.SetName(change.prop);
						changeBuilder.SetOldValue(change.oldVal != null ? change.oldVal : string.Empty);
						if (change.newVal != null)
						{
							changeBuilder.SetNewValue(change.newVal);
						}
						if (result.Value.IsPresent())
						{
							// Get full stack trace.
							changeBuilder.SetErrorMessage(result.Value.Get());
						}
						builder.AddChanges(changeBuilder);
					}
				}
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((ClientDatanodeProtocolProtos.GetReconfigurationStatusResponseProto)builder
				.Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ClientDatanodeProtocolProtos.TriggerBlockReportResponseProto TriggerBlockReport
			(RpcController unused, ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto
			 request)
		{
			try
			{
				impl.TriggerBlockReport(new BlockReportOptions.Factory().SetIncremental(request.GetIncremental
					()).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return TriggerBlockReportResp;
		}
	}
}
