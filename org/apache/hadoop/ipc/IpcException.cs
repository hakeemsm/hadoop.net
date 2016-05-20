using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// IPC exception is thrown by IPC layer when the IPC
	/// connection cannot be established.
	/// </summary>
	[System.Serializable]
	public class IpcException : System.IO.IOException
	{
		private const long serialVersionUID = 1L;

		internal readonly string errMsg;

		public IpcException(string err)
		{
			errMsg = err;
		}
	}
}
