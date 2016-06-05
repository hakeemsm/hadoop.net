using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class IndexRecord
	{
		public long startOffset;

		public long rawLength;

		public long partLength;

		public IndexRecord()
		{
		}

		public IndexRecord(long startOffset, long rawLength, long partLength)
		{
			this.startOffset = startOffset;
			this.rawLength = rawLength;
			this.partLength = partLength;
		}
	}
}
