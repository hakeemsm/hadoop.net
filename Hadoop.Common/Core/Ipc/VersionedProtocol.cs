using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Superclass of all protocols that use Hadoop RPC.</summary>
	/// <remarks>
	/// Superclass of all protocols that use Hadoop RPC.
	/// Subclasses of this interface are also supposed to have
	/// a static final long versionID field.
	/// </remarks>
	public interface VersionedProtocol
	{
		/// <summary>Return protocol version corresponding to protocol interface.</summary>
		/// <param name="protocol">The classname of the protocol interface</param>
		/// <param name="clientVersion">The version of the protocol that the client speaks</param>
		/// <returns>the version that the server will speak</returns>
		/// <exception cref="System.IO.IOException">if any IO error occurs</exception>
		long GetProtocolVersion(string protocol, long clientVersion);

		/// <summary>Return protocol version corresponding to protocol interface.</summary>
		/// <param name="protocol">The classname of the protocol interface</param>
		/// <param name="clientVersion">The version of the protocol that the client speaks</param>
		/// <param name="clientMethodsHash">the hashcode of client protocol methods</param>
		/// <returns>
		/// the server protocol signature containing its version and
		/// a list of its supported methods
		/// </returns>
		/// <seealso cref="ProtocolSignature.GetProtocolSignature(VersionedProtocol, string, long, int)
		/// 	">for a default implementation</seealso>
		/// <exception cref="System.IO.IOException"/>
		ProtocolSignature GetProtocolSignature(string protocol, long clientVersion, int clientMethodsHash
			);
	}
}
