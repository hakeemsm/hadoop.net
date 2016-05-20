using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Indicates that the parent of specified Path is not a directory
	/// as expected.
	/// </summary>
	[System.Serializable]
	public class ParentNotDirectoryException : System.IO.IOException
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
