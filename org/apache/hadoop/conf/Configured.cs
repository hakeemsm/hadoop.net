using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>
	/// Base class for things that may be configured with a
	/// <see cref="Configuration"/>
	/// .
	/// </summary>
	public class Configured : org.apache.hadoop.conf.Configurable
	{
		private org.apache.hadoop.conf.Configuration conf;

		/// <summary>Construct a Configured.</summary>
		public Configured()
			: this(null)
		{
		}

		/// <summary>Construct a Configured.</summary>
		public Configured(org.apache.hadoop.conf.Configuration conf)
		{
			setConf(conf);
		}

		// inherit javadoc
		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		// inherit javadoc
		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}
	}
}
