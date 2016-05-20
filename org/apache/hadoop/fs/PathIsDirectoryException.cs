using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>EISDIR</summary>
	[System.Serializable]
	public class PathIsDirectoryException : org.apache.hadoop.fs.PathExistsException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathIsDirectoryException(string path)
			: base(path, "Is a directory")
		{
		}
	}
}
