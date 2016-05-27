using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Exception corresponding to Operation Not Permitted - EPERM</summary>
	[System.Serializable]
	public class PathPermissionException : PathIOException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathPermissionException(string path)
			: base(path, "Operation not permitted")
		{
		}
	}
}
