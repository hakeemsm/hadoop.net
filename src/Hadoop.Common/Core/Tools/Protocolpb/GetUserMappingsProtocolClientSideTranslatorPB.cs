using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tools.Proto;


namespace Org.Apache.Hadoop.Tools.ProtocolPB
{
	public class GetUserMappingsProtocolClientSideTranslatorPB : ProtocolMetaInterface
		, GetUserMappingsProtocol, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly GetUserMappingsProtocolPB rpcProxy;

		public GetUserMappingsProtocolClientSideTranslatorPB(GetUserMappingsProtocolPB rpcProxy
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
		public virtual string[] GetGroupsForUser(string user)
		{
			GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto request = ((GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
				)GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto.NewBuilder().SetUser
				(user).Build());
			GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto resp;
			try
			{
				resp = rpcProxy.GetGroupsForUser(NullController, request);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
			return Collections.ToArray(resp.GetGroupsList(), new string[resp.GetGroupsCount
				()]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(GetUserMappingsProtocolPB
				), RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(GetUserMappingsProtocolPB
				)), methodName);
		}
	}
}
