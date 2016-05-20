using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Used when target file already exists for any operation and
	/// is not configured to be overwritten.
	/// </summary>
	[System.Serializable]
	public class FileAlreadyExistsException : System.IO.IOException
	{
		public FileAlreadyExistsException()
			: base()
		{
		}

		public FileAlreadyExistsException(string msg)
			: base(msg)
		{
		}
	}
}
