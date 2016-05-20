using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>File system for a given file system name/scheme is not supported</summary>
	[System.Serializable]
	public class UnsupportedFileSystemException : System.IO.IOException
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
