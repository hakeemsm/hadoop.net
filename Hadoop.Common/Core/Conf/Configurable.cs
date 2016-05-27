using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>
	/// Something that may be configured with a
	/// <see cref="Configuration"/>
	/// .
	/// </summary>
	public interface Configurable
	{
		/// <summary>Set the configuration to be used by this object.</summary>
		void SetConf(Configuration conf);

		/// <summary>Return the configuration used by this object.</summary>
		Configuration GetConf();
	}
}
