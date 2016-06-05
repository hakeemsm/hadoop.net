using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Base class for
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// and
	/// <see cref="Reducer{K2, V2, K3, V3}"/>
	/// implementations.
	/// <p>Provides default no-op implementations for a few methods, most non-trivial
	/// applications need to override some of them.</p>
	/// </summary>
	public class MapReduceBase : Closeable, JobConfigurable
	{
		/// <summary>Default implementation that does nothing.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
		}

		/// <summary>Default implementation that does nothing.</summary>
		public virtual void Configure(JobConf job)
		{
		}
	}
}
