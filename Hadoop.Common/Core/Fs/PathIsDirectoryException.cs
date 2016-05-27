using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>EISDIR</summary>
	[System.Serializable]
	public class PathIsDirectoryException : PathExistsException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathIsDirectoryException(string path)
			: base(path, "Is a directory")
		{
		}
	}
}
