using System.IO;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Used when target file already exists for any operation and
	/// is not configured to be overwritten.
	/// </summary>
	[System.Serializable]
	public class FileAlreadyExistsException : IOException
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
