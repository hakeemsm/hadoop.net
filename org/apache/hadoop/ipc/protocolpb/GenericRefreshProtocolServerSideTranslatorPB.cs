using Sharpen;

namespace org.apache.hadoop.ipc.protocolPB
{
	public class GenericRefreshProtocolServerSideTranslatorPB : org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB
	{
		private readonly org.apache.hadoop.ipc.GenericRefreshProtocol impl;

		public GenericRefreshProtocolServerSideTranslatorPB(org.apache.hadoop.ipc.GenericRefreshProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
			 refresh(com.google.protobuf.RpcController controller, org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshRequestProto
			 request)
		{
			try
			{
				System.Collections.Generic.IList<string> argList = request.getArgsList();
				string[] args = Sharpen.Collections.ToArray(argList, new string[argList.Count]);
				if (!request.hasIdentifier())
				{
					throw new com.google.protobuf.ServiceException("Request must contain identifier");
				}
				System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshResponse> results
					 = impl.refresh(request.getIdentifier(), args);
				return pack(results);
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		// Convert a collection of RefreshResponse objects to a
		// RefreshResponseCollection proto
		private org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
			 pack(System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshResponse
			> responses)
		{
			org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto.Builder
				 b = org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
				.newBuilder();
			foreach (org.apache.hadoop.ipc.RefreshResponse response in responses)
			{
				org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto.Builder
					 respBuilder = org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto
					.newBuilder();
				respBuilder.setExitStatus(response.getReturnCode());
				respBuilder.setUserMessage(response.getMessage());
				respBuilder.setSenderName(response.getSenderName());
				// Add to collection
				b.addResponses(respBuilder);
			}
			return ((org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
				)b.build());
		}
	}
}
