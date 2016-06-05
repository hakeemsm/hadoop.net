using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Default
	/// <see cref="MapRunnable{K1, V1, K2, V2}"/>
	/// implementation.
	/// </summary>
	public class MapRunner<K1, V1, K2, V2> : MapRunnable<K1, V1, K2, V2>
	{
		private Mapper<K1, V1, K2, V2> mapper;

		private bool incrProcCount;

		public virtual void Configure(JobConf job)
		{
			this.mapper = ReflectionUtils.NewInstance(job.GetMapperClass(), job);
			//increment processed counter only if skipping feature is enabled
			this.incrProcCount = SkipBadRecords.GetMapperMaxSkipRecords(job) > 0 && SkipBadRecords
				.GetAutoIncrMapperProcCount(job);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output
			, Reporter reporter)
		{
			try
			{
				// allocate key & value instances that are re-used for all entries
				K1 key = input.CreateKey();
				V1 value = input.CreateValue();
				while (input.Next(key, value))
				{
					// map pair to output
					mapper.Map(key, value, output, reporter);
					if (incrProcCount)
					{
						reporter.IncrCounter(SkipBadRecords.CounterGroup, SkipBadRecords.CounterMapProcessedRecords
							, 1);
					}
				}
			}
			finally
			{
				mapper.Close();
			}
		}

		protected internal virtual Mapper<K1, V1, K2, V2> GetMapper()
		{
			return mapper;
		}
	}
}
