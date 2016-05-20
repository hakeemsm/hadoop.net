using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>
	/// Something that may be configured with a
	/// <see cref="Configuration"/>
	/// .
	/// </summary>
	public interface Configurable
	{
		/// <summary>Set the configuration to be used by this object.</summary>
		void setConf(org.apache.hadoop.conf.Configuration conf);

		/// <summary>Return the configuration used by this object.</summary>
		org.apache.hadoop.conf.Configuration getConf();
	}
}
