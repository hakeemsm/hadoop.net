using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Thrown by a remote server when it is up, but is not the active server in a
	/// set of servers in which only a subset may be active.
	/// </summary>
	[System.Serializable]
	public class StandbyException : System.IO.IOException
	{
		internal const long serialVersionUID = unchecked((long)(0x12308AD010L));

		public StandbyException(string msg)
			: base(msg)
		{
		}
	}
}
