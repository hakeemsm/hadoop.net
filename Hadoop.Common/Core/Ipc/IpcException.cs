using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// IPC exception is thrown by IPC layer when the IPC
	/// connection cannot be established.
	/// </summary>
	[System.Serializable]
	public class IpcException : IOException
	{
		private const long serialVersionUID = 1L;

		internal readonly string errMsg;

		public IpcException(string err)
		{
			errMsg = err;
		}
	}
}
