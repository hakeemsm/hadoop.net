using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Used when file type differs from the desired file type.</summary>
	/// <remarks>
	/// Used when file type differs from the desired file type. like
	/// getting a file when a directory is expected. Or a wrong file type.
	/// </remarks>
	[System.Serializable]
	public class InvalidFileTypeException : IOException
	{
		private const long serialVersionUID = 1L;

		public InvalidFileTypeException()
			: base()
		{
		}

		public InvalidFileTypeException(string msg)
			: base(msg)
		{
		}
	}
}
