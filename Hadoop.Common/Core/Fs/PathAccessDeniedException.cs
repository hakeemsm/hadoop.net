using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>EACCES</summary>
	[System.Serializable]
	public class PathAccessDeniedException : PathIOException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathAccessDeniedException(string path)
			: base(path, "Permission denied")
		{
		}
	}
}
