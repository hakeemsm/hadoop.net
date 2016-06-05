using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Proto;


namespace Org.Apache.Hadoop.Ipc.ProtocolPB
{
	public class GenericRefreshProtocolClientSideTranslatorPB : ProtocolMetaInterface
		, GenericRefreshProtocol, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null.</summary>
		private static readonly RpcController NullController = null;

		private readonly GenericRefreshProtocolPB rpcProxy;

		public GenericRefreshProtocolClientSideTranslatorPB(GenericRefreshProtocolPB rpcProxy
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
		public virtual ICollection<RefreshResponse> Refresh(string identifier, string[] args
			)
		{
			IList<string> argList = Arrays.AsList(args);
			try
			{
				GenericRefreshProtocolProtos.GenericRefreshRequestProto request = ((GenericRefreshProtocolProtos.GenericRefreshRequestProto
					)GenericRefreshProtocolProtos.GenericRefreshRequestProto.NewBuilder().SetIdentifier
					(identifier).AddAllArgs(argList).Build());
				GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto resp = rpcProxy
					.Refresh(NullController, request);
				return Unpack(resp);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		private ICollection<RefreshResponse> Unpack(GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
			 collection)
		{
			IList<GenericRefreshProtocolProtos.GenericRefreshResponseProto> responseProtos = 
				collection.GetResponsesList();
			IList<RefreshResponse> responses = new AList<RefreshResponse>();
			foreach (GenericRefreshProtocolProtos.GenericRefreshResponseProto rp in responseProtos)
			{
				RefreshResponse response = Unpack(rp);
				responses.AddItem(response);
			}
			return responses;
		}

		private RefreshResponse Unpack(GenericRefreshProtocolProtos.GenericRefreshResponseProto
			 proto)
		{
			// The default values
			string message = null;
			string sender = null;
			int returnCode = -1;
			// ... that can be overridden by data from the protobuf
			if (proto.HasUserMessage())
			{
				message = proto.GetUserMessage();
			}
			if (proto.HasExitStatus())
			{
				returnCode = proto.GetExitStatus();
			}
			if (proto.HasSenderName())
			{
				sender = proto.GetSenderName();
			}
			// ... and put into a RefreshResponse
			RefreshResponse response = new RefreshResponse(returnCode, message);
			response.SetSenderName(sender);
			return response;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(GenericRefreshProtocolPB)
				, RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(GenericRefreshProtocolPB
				)), methodName);
		}
	}
}
