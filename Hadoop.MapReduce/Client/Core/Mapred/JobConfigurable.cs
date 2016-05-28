using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>That what may be configured.</summary>
	public interface JobConfigurable
	{
		/// <summary>
		/// Initializes a new instance from a
		/// <see cref="JobConf"/>
		/// .
		/// </summary>
		/// <param name="job">the configuration</param>
		void Configure(JobConf job);
	}
}
