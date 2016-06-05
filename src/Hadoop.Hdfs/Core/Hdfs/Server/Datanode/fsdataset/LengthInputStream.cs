using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>An input stream with length.</summary>
	public class LengthInputStream : FilterInputStream
	{
		private readonly long length;

		/// <summary>Create an stream.</summary>
		/// <param name="in">the underlying input stream.</param>
		/// <param name="length">the length of the stream.</param>
		public LengthInputStream(InputStream @in, long length)
			: base(@in)
		{
			this.length = length;
		}

		/// <returns>the length.</returns>
		public virtual long GetLength()
		{
			return length;
		}

		public virtual InputStream GetWrappedStream()
		{
			return @in;
		}
	}
}
