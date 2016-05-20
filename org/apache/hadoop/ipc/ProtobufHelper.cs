using Sharpen;

namespace org.apache.hadoop.ipc
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
		public static System.IO.IOException getRemoteException(com.google.protobuf.ServiceException
			 se)
		{
			System.Exception e = se.InnerException;
			if (e == null)
			{
				return new System.IO.IOException(se);
			}
			return e is System.IO.IOException ? (System.IO.IOException)e : new System.IO.IOException
				(se);
		}
	}
}
