using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// This interface is implemented by the client side translators and can be used
	/// to obtain information about underlying protocol e.g.
	/// </summary>
	/// <remarks>
	/// This interface is implemented by the client side translators and can be used
	/// to obtain information about underlying protocol e.g. to check if a method is
	/// supported on the server side.
	/// </remarks>
	public interface ProtocolMetaInterface
	{
		/// <summary>Checks whether the given method name is supported by the server.</summary>
		/// <remarks>
		/// Checks whether the given method name is supported by the server.
		/// It is assumed that all method names are unique for a protocol.
		/// </remarks>
		/// <param name="methodName">The name of the method</param>
		/// <returns>true if method is supported, otherwise false.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool IsMethodSupported(string methodName);
	}
}
