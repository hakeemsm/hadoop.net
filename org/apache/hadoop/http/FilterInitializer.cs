using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>Initialize a javax.servlet.Filter.</summary>
	public abstract class FilterInitializer
	{
		/// <summary>Initialize a Filter to a FilterContainer.</summary>
		/// <param name="container">The filter container</param>
		/// <param name="conf">Configuration for run-time parameters</param>
		public abstract void initFilter(org.apache.hadoop.http.FilterContainer container, 
			org.apache.hadoop.conf.Configuration conf);
	}
}
