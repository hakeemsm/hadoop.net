using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Exception corresponding to File Exists - EEXISTS</summary>
	[System.Serializable]
	public class PathExistsException : PathIOException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathExistsException(string path)
			: base(path, "File exists")
		{
		}

		protected internal PathExistsException(string path, string error)
			: base(path, error)
		{
		}
	}
}
