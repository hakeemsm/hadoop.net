using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Indicates that the parent of specified Path is not a directory
	/// as expected.
	/// </summary>
	[System.Serializable]
	public class ParentNotDirectoryException : IOException
	{
		private const long serialVersionUID = 1L;

		public ParentNotDirectoryException()
			: base()
		{
		}

		public ParentNotDirectoryException(string msg)
			: base(msg)
		{
		}
	}
}
