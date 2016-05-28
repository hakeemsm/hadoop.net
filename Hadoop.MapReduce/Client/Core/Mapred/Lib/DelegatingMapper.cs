using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
	/// that delegates behaviour of paths to multiple other
	/// mappers.
	/// </summary>
	/// <seealso cref="MultipleInputs.AddInputPath(Org.Apache.Hadoop.Mapred.JobConf, Org.Apache.Hadoop.FS.Path, System.Type{T}, System.Type{T})
	/// 	"/>
	public class DelegatingMapper<K1, V1, K2, V2> : Mapper<K1, V1, K2, V2>
	{
		private JobConf conf;

		private Mapper<K1, V1, K2, V2> mapper;

		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(K1 key, V1 value, OutputCollector<K2, V2> outputCollector
			, Reporter reporter)
		{
			if (mapper == null)
			{
				// Find the Mapper from the TaggedInputSplit.
				TaggedInputSplit inputSplit = (TaggedInputSplit)reporter.GetInputSplit();
				mapper = (Mapper<K1, V1, K2, V2>)ReflectionUtils.NewInstance(inputSplit.GetMapperClass
					(), conf);
			}
			mapper.Map(key, value, outputCollector, reporter);
		}

		public virtual void Configure(JobConf conf)
		{
			this.conf = conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (mapper != null)
			{
				mapper.Close();
			}
		}
	}
}
