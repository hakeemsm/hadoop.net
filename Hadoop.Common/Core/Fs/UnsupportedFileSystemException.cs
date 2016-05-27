using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>File system for a given file system name/scheme is not supported</summary>
	[System.Serializable]
	public class UnsupportedFileSystemException : IOException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="message">exception message.</param>
		public UnsupportedFileSystemException(string message)
			: base(message)
		{
		}
	}
}
