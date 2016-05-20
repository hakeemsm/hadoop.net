using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Exception corresponding to Operation Not Permitted - EPERM</summary>
	[System.Serializable]
	public class PathPermissionException : org.apache.hadoop.fs.PathIOException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathPermissionException(string path)
			: base(path, "Operation not permitted")
		{
		}
	}
}
