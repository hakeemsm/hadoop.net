using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class CombineFileSplit : Org.Apache.Hadoop.Mapreduce.Lib.Input.CombineFileSplit
		, InputSplit
	{
		private JobConf job;

		public CombineFileSplit()
		{
		}

		public CombineFileSplit(JobConf job, Path[] files, long[] start, long[] lengths, 
			string[] locations)
			: base(files, start, lengths, locations)
		{
			this.job = job;
		}

		public CombineFileSplit(JobConf job, Path[] files, long[] lengths)
			: base(files, lengths)
		{
			this.job = job;
		}

		/// <summary>Copy constructor</summary>
		/// <exception cref="System.IO.IOException"/>
		public CombineFileSplit(Org.Apache.Hadoop.Mapred.Lib.CombineFileSplit old)
			: base(old)
		{
		}

		public virtual JobConf GetJob()
		{
			return job;
		}
	}
}
