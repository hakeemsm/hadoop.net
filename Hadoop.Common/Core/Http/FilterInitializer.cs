using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>Initialize a javax.servlet.Filter.</summary>
	public abstract class FilterInitializer
	{
		/// <summary>Initialize a Filter to a FilterContainer.</summary>
		/// <param name="container">The filter container</param>
		/// <param name="conf">Configuration for run-time parameters</param>
		public abstract void InitFilter(FilterContainer container, Configuration conf);
	}
}
