using System;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	/// <summary>
	/// This exception is thrown by ResourceManager if it's loading an incompatible
	/// version of state from state store on recovery.
	/// </summary>
	[System.Serializable]
	public class RMStateVersionIncompatibleException : YarnException
	{
		private const long serialVersionUID = 1364408L;

		public RMStateVersionIncompatibleException(Exception cause)
			: base(cause)
		{
		}

		public RMStateVersionIncompatibleException(string message)
			: base(message)
		{
		}

		public RMStateVersionIncompatibleException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
