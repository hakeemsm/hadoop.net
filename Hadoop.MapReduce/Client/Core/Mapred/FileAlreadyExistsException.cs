using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Used when target file already exists for any operation and
	/// is not configured to be overwritten.
	/// </summary>
	[System.Serializable]
	public class FileAlreadyExistsException : IOException
	{
		private const long serialVersionUID = 1L;

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
