using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This exception is thrown when a read encounters a block that has no locations
	/// associated with it.
	/// </summary>
	[System.Serializable]
	public class BlockMissingException : IOException
	{
		private const long serialVersionUID = 1L;

		private readonly string filename;

		private readonly long offset;

		/// <summary>An exception that indicates that file was corrupted.</summary>
		/// <param name="filename">name of corrupted file</param>
		/// <param name="description">a description of the corruption details</param>
		public BlockMissingException(string filename, string description, long offset)
			: base(description)
		{
			this.filename = filename;
			this.offset = offset;
		}

		/// <summary>Returns the name of the corrupted file.</summary>
		/// <returns>name of corrupted file</returns>
		public virtual string GetFile()
		{
			return filename;
		}

		/// <summary>Returns the offset at which this file is corrupted</summary>
		/// <returns>offset of corrupted file</returns>
		public virtual long GetOffset()
		{
			return offset;
		}
	}
}
