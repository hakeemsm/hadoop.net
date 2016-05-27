using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.HA.ProtocolPB
{
	public class ZKFCProtocolServerSideTranslatorPB : ZKFCProtocolPB
	{
		private readonly ZKFCProtocol server;

		public ZKFCProtocolServerSideTranslatorPB(ZKFCProtocol server)
		{
			this.server = server;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ZKFCProtocolProtos.CedeActiveResponseProto CedeActive(RpcController
			 controller, ZKFCProtocolProtos.CedeActiveRequestProto request)
		{
			try
			{
				server.CedeActive(request.GetMillisToCede());
				return ZKFCProtocolProtos.CedeActiveResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ZKFCProtocolProtos.GracefulFailoverResponseProto GracefulFailover(
			RpcController controller, ZKFCProtocolProtos.GracefulFailoverRequestProto request
			)
		{
			try
			{
				server.GracefulFailover();
				return ZKFCProtocolProtos.GracefulFailoverResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string protocol, long clientVersion)
		{
			return RPC.GetProtocolVersion(typeof(ZKFCProtocolPB));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
			, int clientMethodsHash)
		{
			if (!protocol.Equals(RPC.GetProtocolName(typeof(ZKFCProtocolPB))))
			{
				throw new IOException("Serverside implements " + RPC.GetProtocolName(typeof(ZKFCProtocolPB
					)) + ". The following requested protocol is unknown: " + protocol);
			}
			return ProtocolSignature.GetProtocolSignature(clientMethodsHash, RPC.GetProtocolVersion
				(typeof(ZKFCProtocolPB)), typeof(HAServiceProtocolPB));
		}
	}
}
