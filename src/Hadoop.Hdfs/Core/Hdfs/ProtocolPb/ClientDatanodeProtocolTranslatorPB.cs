using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Primitives;
using Com.Google.Protobuf;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientDatanodeProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="ClientDatanodeProtocolPB"/>
	/// .
	/// </summary>
	public class ClientDatanodeProtocolTranslatorPB : ProtocolMetaInterface, ClientDatanodeProtocol
		, ProtocolTranslator, IDisposable
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.ProtocolPB.ClientDatanodeProtocolTranslatorPB
			));

		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly ClientDatanodeProtocolPB rpcProxy;

		private static readonly ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto
			 VoidRefreshNamenodes = ((ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto
			)ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto.NewBuilder().Build());

		private static readonly ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto 
			VoidGetDatanodeInfo = ((ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto
			)ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto.NewBuilder().Build());

		private static readonly ClientDatanodeProtocolProtos.GetReconfigurationStatusRequestProto
			 VoidGetReconfigStatus = ((ClientDatanodeProtocolProtos.GetReconfigurationStatusRequestProto
			)ClientDatanodeProtocolProtos.GetReconfigurationStatusRequestProto.NewBuilder().
			Build());

		private static readonly ClientDatanodeProtocolProtos.StartReconfigurationRequestProto
			 VoidStartReconfig = ((ClientDatanodeProtocolProtos.StartReconfigurationRequestProto
			)ClientDatanodeProtocolProtos.StartReconfigurationRequestProto.NewBuilder().Build
			());

		/// <exception cref="System.IO.IOException"/>
		public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid, Configuration conf
			, int socketTimeout, bool connectToDnViaHostname, LocatedBlock locatedBlock)
		{
			rpcProxy = CreateClientDatanodeProtocolProxy(datanodeid, conf, socketTimeout, connectToDnViaHostname
				, locatedBlock);
		}

		/// <exception cref="System.IO.IOException"/>
		public ClientDatanodeProtocolTranslatorPB(IPEndPoint addr, UserGroupInformation ticket
			, Configuration conf, SocketFactory factory)
		{
			rpcProxy = CreateClientDatanodeProtocolProxy(addr, ticket, conf, factory, 0);
		}

		/// <summary>Constructor.</summary>
		/// <param name="datanodeid">Datanode to connect to.</param>
		/// <param name="conf">Configuration.</param>
		/// <param name="socketTimeout">Socket timeout to use.</param>
		/// <param name="connectToDnViaHostname">connect to the Datanode using its hostname</param>
		/// <exception cref="System.IO.IOException"/>
		public ClientDatanodeProtocolTranslatorPB(DatanodeID datanodeid, Configuration conf
			, int socketTimeout, bool connectToDnViaHostname)
		{
			string dnAddr = datanodeid.GetIpcAddr(connectToDnViaHostname);
			IPEndPoint addr = NetUtils.CreateSocketAddr(dnAddr);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Connecting to datanode " + dnAddr + " addr=" + addr);
			}
			rpcProxy = CreateClientDatanodeProtocolProxy(addr, UserGroupInformation.GetCurrentUser
				(), conf, NetUtils.GetDefaultSocketFactory(conf), socketTimeout);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static ClientDatanodeProtocolPB CreateClientDatanodeProtocolProxy(DatanodeID
			 datanodeid, Configuration conf, int socketTimeout, bool connectToDnViaHostname, 
			LocatedBlock locatedBlock)
		{
			string dnAddr = datanodeid.GetIpcAddr(connectToDnViaHostname);
			IPEndPoint addr = NetUtils.CreateSocketAddr(dnAddr);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Connecting to datanode " + dnAddr + " addr=" + addr);
			}
			// Since we're creating a new UserGroupInformation here, we know that no
			// future RPC proxies will be able to re-use the same connection. And
			// usages of this proxy tend to be one-off calls.
			//
			// This is a temporary fix: callers should really achieve this by using
			// RPC.stopProxy() on the resulting object, but this is currently not
			// working in trunk. See the discussion on HDFS-1965.
			Configuration confWithNoIpcIdle = new Configuration(conf);
			confWithNoIpcIdle.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey
				, 0);
			UserGroupInformation ticket = UserGroupInformation.CreateRemoteUser(locatedBlock.
				GetBlock().GetLocalBlock().ToString());
			ticket.AddToken(locatedBlock.GetBlockToken());
			return CreateClientDatanodeProtocolProxy(addr, ticket, confWithNoIpcIdle, NetUtils
				.GetDefaultSocketFactory(conf), socketTimeout);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static ClientDatanodeProtocolPB CreateClientDatanodeProtocolProxy(IPEndPoint
			 addr, UserGroupInformation ticket, Configuration conf, SocketFactory factory, int
			 socketTimeout)
		{
			RPC.SetProtocolEngine(conf, typeof(ClientDatanodeProtocolPB), typeof(ProtobufRpcEngine
				));
			return RPC.GetProxy<ClientDatanodeProtocolPB>(RPC.GetProtocolVersion(typeof(ClientDatanodeProtocolPB
				)), addr, ticket, conf, factory, socketTimeout);
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetReplicaVisibleLength(ExtendedBlock b)
		{
			ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto req = ((ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto
				)ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto.NewBuilder().SetBlock
				(PBHelper.Convert(b)).Build());
			try
			{
				return rpcProxy.GetReplicaVisibleLength(NullController, req).GetLength();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNamenodes()
		{
			try
			{
				rpcProxy.RefreshNamenodes(NullController, VoidRefreshNamenodes);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteBlockPool(string bpid, bool force)
		{
			ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto req = ((ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto
				)ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto.NewBuilder().SetBlockPool
				(bpid).SetForce(force).Build());
			try
			{
				rpcProxy.DeleteBlockPool(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token)
		{
			ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto req = ((ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto
				)ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto.NewBuilder().SetBlock
				(PBHelper.Convert(block)).SetToken(PBHelper.Convert(token)).Build());
			ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto resp;
			try
			{
				resp = rpcProxy.GetBlockLocalPathInfo(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			return new BlockLocalPathInfo(PBHelper.Convert(resp.GetBlock()), resp.GetLocalPath
				(), resp.GetLocalMetaPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(ClientDatanodeProtocolPB)
				, RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(ClientDatanodeProtocolPB
				)), methodName);
		}

		public virtual object GetUnderlyingProxyObject()
		{
			return rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsBlocksMetadata GetHdfsBlocksMetadata(string blockPoolId, long[]
			 blockIds, IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>> tokens
			)
		{
			IList<SecurityProtos.TokenProto> tokensProtos = new AList<SecurityProtos.TokenProto
				>(tokens.Count);
			foreach (Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> t in tokens)
			{
				tokensProtos.AddItem(PBHelper.Convert(t));
			}
			// Build the request
			ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto request = ((ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto
				)ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto.NewBuilder().SetBlockPoolId
				(blockPoolId).AddAllBlockIds(Longs.AsList(blockIds)).AddAllTokens(tokensProtos).
				Build());
			// Send the RPC
			ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto response;
			try
			{
				response = rpcProxy.GetHdfsBlockLocations(NullController, request);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			// List of volumes in the response
			IList<ByteString> volumeIdsByteStrings = response.GetVolumeIdsList();
			IList<byte[]> volumeIds = new AList<byte[]>(volumeIdsByteStrings.Count);
			foreach (ByteString bs in volumeIdsByteStrings)
			{
				volumeIds.AddItem(bs.ToByteArray());
			}
			// Array of indexes into the list of volumes, one per block
			IList<int> volumeIndexes = response.GetVolumeIndexesList();
			// Parsed HdfsVolumeId values, one per block
			return new HdfsBlocksMetadata(blockPoolId, blockIds, volumeIds, volumeIndexes);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ShutdownDatanode(bool forUpgrade)
		{
			ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto request = ((ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto
				)ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto.NewBuilder().SetForUpgrade
				(forUpgrade).Build());
			try
			{
				rpcProxy.ShutdownDatanode(NullController, request);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeLocalInfo GetDatanodeInfo()
		{
			ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto response;
			try
			{
				response = rpcProxy.GetDatanodeInfo(NullController, VoidGetDatanodeInfo);
				return PBHelper.Convert(response.GetLocalInfo());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartReconfiguration()
		{
			try
			{
				rpcProxy.StartReconfiguration(NullController, VoidStartReconfig);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ReconfigurationTaskStatus GetReconfigurationStatus()
		{
			ClientDatanodeProtocolProtos.GetReconfigurationStatusResponseProto response;
			IDictionary<ReconfigurationUtil.PropertyChange, Optional<string>> statusMap = null;
			long startTime;
			long endTime = 0;
			try
			{
				response = rpcProxy.GetReconfigurationStatus(NullController, VoidGetReconfigStatus
					);
				startTime = response.GetStartTime();
				if (response.HasEndTime())
				{
					endTime = response.GetEndTime();
				}
				if (response.GetChangesCount() > 0)
				{
					statusMap = Maps.NewHashMap();
					foreach (ClientDatanodeProtocolProtos.GetReconfigurationStatusConfigChangeProto change
						 in response.GetChangesList())
					{
						ReconfigurationUtil.PropertyChange pc = new ReconfigurationUtil.PropertyChange(change
							.GetName(), change.GetNewValue(), change.GetOldValue());
						string errorMessage = null;
						if (change.HasErrorMessage())
						{
							errorMessage = change.GetErrorMessage();
						}
						statusMap[pc] = Optional.FromNullable(errorMessage);
					}
				}
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			return new ReconfigurationTaskStatus(startTime, endTime, statusMap);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TriggerBlockReport(BlockReportOptions options)
		{
			try
			{
				rpcProxy.TriggerBlockReport(NullController, ((ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto
					)ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto.NewBuilder().SetIncremental
					(options.IsIncremental()).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}
	}
}
