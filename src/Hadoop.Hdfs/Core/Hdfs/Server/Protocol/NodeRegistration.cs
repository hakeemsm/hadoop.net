using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// Generic class specifying information, which need to be sent to the name-node
	/// during the registration process.
	/// </summary>
	public interface NodeRegistration
	{
		/// <summary>Get address of the server node.</summary>
		/// <returns>ipAddr:portNumber</returns>
		string GetAddress();

		/// <summary>Get registration ID of the server node.</summary>
		string GetRegistrationID();

		/// <summary>Get layout version of the server node.</summary>
		int GetVersion();

		string ToString();
	}
}
