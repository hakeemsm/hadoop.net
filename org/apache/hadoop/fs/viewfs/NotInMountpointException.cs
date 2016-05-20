using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>NotInMountpointException extends the UnsupportedOperationException.</summary>
	/// <remarks>
	/// NotInMountpointException extends the UnsupportedOperationException.
	/// Exception class used in cases where the given path is not mounted
	/// through viewfs.
	/// </remarks>
	[System.Serializable]
	public class NotInMountpointException : System.NotSupportedException
	{
		internal readonly string msg;

		public NotInMountpointException(org.apache.hadoop.fs.Path path, string operation)
		{
			/*Evolving for a release,to be changed to Stable */
			msg = operation + " on path `" + path + "' is not within a mount point";
		}

		public NotInMountpointException(string operation)
		{
			msg = operation + " on empty path is invalid";
		}

		public override string Message
		{
			get
			{
				return msg;
			}
		}
	}
}
