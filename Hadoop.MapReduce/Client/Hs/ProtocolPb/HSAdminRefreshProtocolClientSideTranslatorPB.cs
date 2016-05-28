using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.ProtocolPB
{
	public class HSAdminRefreshProtocolClientSideTranslatorPB : ProtocolMetaInterface
		, HSAdminRefreshProtocol, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly HSAdminRefreshProtocolPB rpcProxy;

		private static readonly HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto
			 VoidRefreshAdminAclsRequest = ((HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto
			)HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto.NewBuilder().Build());

		private static readonly HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto
			 VoidRefreshLoadedJobCacheRequest = ((HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto
			)HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto.NewBuilder().Build
			());

		private static readonly HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto
			 VoidRefreshJobRetentionSettingsRequest = ((HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto
			)HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto.NewBuilder
			().Build());

		private static readonly HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto
			 VoidRefreshLogRetentionSettingsRequest = ((HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto
			)HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto.NewBuilder
			().Build());

		public HSAdminRefreshProtocolClientSideTranslatorPB(HSAdminRefreshProtocolPB rpcProxy
			)
		{
			this.rpcProxy = rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshAdminAcls()
		{
			try
			{
				rpcProxy.RefreshAdminAcls(NullController, VoidRefreshAdminAclsRequest);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshLoadedJobCache()
		{
			try
			{
				rpcProxy.RefreshLoadedJobCache(NullController, VoidRefreshLoadedJobCacheRequest);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshJobRetentionSettings()
		{
			try
			{
				rpcProxy.RefreshJobRetentionSettings(NullController, VoidRefreshJobRetentionSettingsRequest
					);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshLogRetentionSettings()
		{
			try
			{
				rpcProxy.RefreshLogRetentionSettings(NullController, VoidRefreshLogRetentionSettingsRequest
					);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(HSAdminRefreshProtocolPB)
				, RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(HSAdminRefreshProtocolPB
				)), methodName);
		}
	}
}
