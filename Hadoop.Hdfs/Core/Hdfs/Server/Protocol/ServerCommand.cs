using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Base class for a server command.</summary>
	/// <remarks>
	/// Base class for a server command.
	/// Issued by the name-node to notify other servers what should be done.
	/// Commands are defined by actions defined in respective protocols.
	/// </remarks>
	/// <seealso cref="DatanodeProtocol"/>
	/// <seealso cref="NamenodeProtocol"/>
	public abstract class ServerCommand
	{
		private readonly int action;

		/// <summary>Create a command for the specified action.</summary>
		/// <remarks>
		/// Create a command for the specified action.
		/// Actions are protocol specific.
		/// </remarks>
		/// <seealso cref="DatanodeProtocol"/>
		/// <seealso cref="NamenodeProtocol"/>
		/// <param name="action">protocol specific action</param>
		public ServerCommand(int action)
		{
			this.action = action;
		}

		/// <summary>Get server command action.</summary>
		/// <returns>action code.</returns>
		public virtual int GetAction()
		{
			return this.action;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetType().Name);
			sb.Append("/");
			sb.Append(action);
			return sb.ToString();
		}
	}
}
