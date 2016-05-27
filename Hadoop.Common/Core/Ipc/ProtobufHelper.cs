using System;
using System.IO;
using Com.Google.Protobuf;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Helper methods for protobuf related RPC implementation</summary>
	public class ProtobufHelper
	{
		private ProtobufHelper()
		{
		}

		// Hidden constructor for class with only static helper methods
		/// <summary>
		/// Return the IOException thrown by the remote server wrapped in
		/// ServiceException as cause.
		/// </summary>
		/// <param name="se">ServiceException that wraps IO exception thrown by the server</param>
		/// <returns>
		/// Exception wrapped in ServiceException or
		/// a new IOException that wraps the unexpected ServiceException.
		/// </returns>
		public static IOException GetRemoteException(ServiceException se)
		{
			Exception e = se.InnerException;
			if (e == null)
			{
				return new IOException(se);
			}
			return e is IOException ? (IOException)e : new IOException(se);
		}
	}
}
