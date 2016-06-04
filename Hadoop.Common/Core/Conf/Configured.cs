using Hadoop.Common.Core.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>
	/// Base class for things that may be configured with a
	/// <see cref="Configuration"/>
	/// .
	/// </summary>
	public class Configured : Configurable
	{
		private Configuration conf;

		/// <summary>Construct a Configured.</summary>
		public Configured()
			: this(null)
		{
		}

		/// <summary>Construct a Configured.</summary>
		public Configured(Configuration conf)
		{
			SetConf(conf);
		}

		// inherit javadoc
		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		// inherit javadoc
		public virtual Configuration GetConf()
		{
			return conf;
		}
	}
}
