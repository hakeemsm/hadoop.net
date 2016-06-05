using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class SplitLineReader : LineReader
	{
		public SplitLineReader(InputStream @in, byte[] recordDelimiterBytes)
			: base(@in, recordDelimiterBytes)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public SplitLineReader(InputStream @in, Configuration conf, byte[] recordDelimiterBytes
			)
			: base(@in, conf, recordDelimiterBytes)
		{
		}

		public virtual bool NeedAdditionalRecordAfterSplit()
		{
			return false;
		}
	}
}
