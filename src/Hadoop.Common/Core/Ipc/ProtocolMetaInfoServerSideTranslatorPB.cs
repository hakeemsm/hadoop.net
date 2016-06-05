using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc.Protobuf;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// This class serves the requests for protocol versions and signatures by
	/// looking them up in the server registry.
	/// </summary>
	public class ProtocolMetaInfoServerSideTranslatorPB : ProtocolMetaInfoPB
	{
		internal RPC.Server server;

		public ProtocolMetaInfoServerSideTranslatorPB(RPC.Server server)
		{
			this.server = server;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ProtocolInfoProtos.GetProtocolVersionsResponseProto GetProtocolVersions
			(RpcController controller, ProtocolInfoProtos.GetProtocolVersionsRequestProto request
			)
		{
			string protocol = request.GetProtocol();
			ProtocolInfoProtos.GetProtocolVersionsResponseProto.Builder builder = ProtocolInfoProtos.GetProtocolVersionsResponseProto
				.NewBuilder();
			foreach (RPC.RpcKind r in RPC.RpcKind.Values())
			{
				long[] versions;
				try
				{
					versions = GetProtocolVersionForRpcKind(r, protocol);
				}
				catch (TypeLoadException e)
				{
					throw new ServiceException(e);
				}
				ProtocolInfoProtos.ProtocolVersionProto.Builder b = ProtocolInfoProtos.ProtocolVersionProto
					.NewBuilder();
				if (versions != null)
				{
					b.SetRpcKind(r.ToString());
					foreach (long v in versions)
					{
						b.AddVersions(v);
					}
				}
				builder.AddProtocolVersions(((ProtocolInfoProtos.ProtocolVersionProto)b.Build()));
			}
			return ((ProtocolInfoProtos.GetProtocolVersionsResponseProto)builder.Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual ProtocolInfoProtos.GetProtocolSignatureResponseProto GetProtocolSignature
			(RpcController controller, ProtocolInfoProtos.GetProtocolSignatureRequestProto request
			)
		{
			ProtocolInfoProtos.GetProtocolSignatureResponseProto.Builder builder = ProtocolInfoProtos.GetProtocolSignatureResponseProto
				.NewBuilder();
			string protocol = request.GetProtocol();
			string rpcKind = request.GetRpcKind();
			long[] versions;
			try
			{
				versions = GetProtocolVersionForRpcKind(RPC.RpcKind.ValueOf(rpcKind), protocol);
			}
			catch (TypeLoadException e1)
			{
				throw new ServiceException(e1);
			}
			if (versions == null)
			{
				return ((ProtocolInfoProtos.GetProtocolSignatureResponseProto)builder.Build());
			}
			foreach (long v in versions)
			{
				ProtocolInfoProtos.ProtocolSignatureProto.Builder sigBuilder = ProtocolInfoProtos.ProtocolSignatureProto
					.NewBuilder();
				sigBuilder.SetVersion(v);
				try
				{
					ProtocolSignature signature = ProtocolSignature.GetProtocolSignature(protocol, v);
					foreach (int m in signature.GetMethods())
					{
						sigBuilder.AddMethods(m);
					}
				}
				catch (TypeLoadException e)
				{
					throw new ServiceException(e);
				}
				builder.AddProtocolSignature(((ProtocolInfoProtos.ProtocolSignatureProto)sigBuilder
					.Build()));
			}
			return ((ProtocolInfoProtos.GetProtocolSignatureResponseProto)builder.Build());
		}

		/// <exception cref="System.TypeLoadException"/>
		private long[] GetProtocolVersionForRpcKind(RPC.RpcKind rpcKind, string protocol)
		{
			Type protocolClass = Runtime.GetType(protocol);
			string protocolName = RPC.GetProtocolName(protocolClass);
			RPC.Server.VerProtocolImpl[] vers = server.GetSupportedProtocolVersions(rpcKind, 
				protocolName);
			if (vers == null)
			{
				return null;
			}
			long[] versions = new long[vers.Length];
			for (int i = 0; i < versions.Length; i++)
			{
				versions[i] = vers[i].version;
			}
			return versions;
		}
	}
}
