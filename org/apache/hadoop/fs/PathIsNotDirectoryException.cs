using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>ENOTDIR</summary>
	[System.Serializable]
	public class PathIsNotDirectoryException : org.apache.hadoop.fs.PathExistsException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathIsNotDirectoryException(string path)
			: base(path, "Is not a directory")
		{
		}
	}
}
