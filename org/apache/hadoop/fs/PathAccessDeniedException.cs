using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>EACCES</summary>
	[System.Serializable]
	public class PathAccessDeniedException : org.apache.hadoop.fs.PathIOException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathAccessDeniedException(string path)
			: base(path, "Permission denied")
		{
		}
	}
}
