using System.Net;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public interface RemotePeerFactory
	{
		/// <param name="addr">The address to connect to.</param>
		/// <param name="blockToken">Token used during optional SASL negotiation</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>A new Peer connected to the address.</returns>
		/// <exception cref="System.IO.IOException">
		/// If there was an error connecting or creating
		/// the remote socket, encrypted stream, etc.
		/// </exception>
		Peer NewConnectedPeer(IPEndPoint addr, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> blockToken, DatanodeID datanodeId);
	}
}
