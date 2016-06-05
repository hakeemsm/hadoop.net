using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Expert: Generic interface for
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// s.
	/// <p>Custom implementations of <code>MapRunnable</code> can exert greater
	/// control on map processing e.g. multi-threaded, asynchronous mappers etc.</p>
	/// </summary>
	/// <seealso cref="Mapper{K1, V1, K2, V2}"/>
	public interface MapRunnable<K1, V1, K2, V2> : JobConfigurable
	{
		/// <summary>Start mapping input <tt>&lt;key, value&gt;</tt> pairs.</summary>
		/// <remarks>
		/// Start mapping input <tt>&lt;key, value&gt;</tt> pairs.
		/// <p>Mapping of input records to output records is complete when this method
		/// returns.</p>
		/// </remarks>
		/// <param name="input">
		/// the
		/// <see cref="RecordReader{K, V}"/>
		/// to read the input records.
		/// </param>
		/// <param name="output">
		/// the
		/// <see cref="OutputCollector{K, V}"/>
		/// to collect the outputrecords.
		/// </param>
		/// <param name="reporter">
		/// 
		/// <see cref="Reporter"/>
		/// to report progress, status-updates etc.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void Run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output, Reporter reporter
			);
	}
}
