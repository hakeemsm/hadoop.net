

namespace Org.Apache.Hadoop.FS
{
	/// <summary>ENOTDIR</summary>
	[System.Serializable]
	public class PathIsNotDirectoryException : PathExistsException
	{
		internal const long serialVersionUID = 0L;

		/// <param name="path">for the exception</param>
		public PathIsNotDirectoryException(string path)
			: base(path, "Is not a directory")
		{
		}
	}
}
