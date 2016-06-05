using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	/// <summary>
	/// Shuffle Header information that is sent by the TaskTracker and
	/// deciphered by the Fetcher thread of Reduce task
	/// </summary>
	public class ShuffleHeader : Writable
	{
		/// <summary>Header info of the shuffle http request/response</summary>
		public const string HttpHeaderName = "name";

		public const string DefaultHttpHeaderName = "mapreduce";

		public const string HttpHeaderVersion = "version";

		public const string DefaultHttpHeaderVersion = "1.0.0";

		/// <summary>The longest possible length of task attempt id that we will accept.</summary>
		private const int MaxIdLength = 1000;

		internal string mapId;

		internal long uncompressedLength;

		internal long compressedLength;

		internal int forReduce;

		public ShuffleHeader()
		{
		}

		public ShuffleHeader(string mapId, long compressedLength, long uncompressedLength
			, int forReduce)
		{
			this.mapId = mapId;
			this.compressedLength = compressedLength;
			this.uncompressedLength = uncompressedLength;
			this.forReduce = forReduce;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			mapId = WritableUtils.ReadStringSafely(@in, MaxIdLength);
			compressedLength = WritableUtils.ReadVLong(@in);
			uncompressedLength = WritableUtils.ReadVLong(@in);
			forReduce = WritableUtils.ReadVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, mapId);
			WritableUtils.WriteVLong(@out, compressedLength);
			WritableUtils.WriteVLong(@out, uncompressedLength);
			WritableUtils.WriteVInt(@out, forReduce);
		}
	}
}
