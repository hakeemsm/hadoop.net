using System.Collections.Generic;
using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Proto;


namespace Org.Apache.Hadoop.Ipc.ProtocolPB
{
	public class GenericRefreshProtocolServerSideTranslatorPB : GenericRefreshProtocolPB
	{
		private readonly GenericRefreshProtocol impl;

		public GenericRefreshProtocolServerSideTranslatorPB(GenericRefreshProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
			 Refresh(RpcController controller, GenericRefreshProtocolProtos.GenericRefreshRequestProto
			 request)
		{
			try
			{
				IList<string> argList = request.GetArgsList();
				string[] args = Collections.ToArray(argList, new string[argList.Count]);
				if (!request.HasIdentifier())
				{
					throw new ServiceException("Request must contain identifier");
				}
				ICollection<RefreshResponse> results = impl.Refresh(request.GetIdentifier(), args
					);
				return Pack(results);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		// Convert a collection of RefreshResponse objects to a
		// RefreshResponseCollection proto
		private GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto Pack(ICollection
			<RefreshResponse> responses)
		{
			GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto.Builder b = GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
				.NewBuilder();
			foreach (RefreshResponse response in responses)
			{
				GenericRefreshProtocolProtos.GenericRefreshResponseProto.Builder respBuilder = GenericRefreshProtocolProtos.GenericRefreshResponseProto
					.NewBuilder();
				respBuilder.SetExitStatus(response.GetReturnCode());
				respBuilder.SetUserMessage(response.GetMessage());
				respBuilder.SetSenderName(response.GetSenderName());
				// Add to collection
				b.AddResponses(respBuilder);
			}
			return ((GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto)b.Build
				());
		}
	}
}
