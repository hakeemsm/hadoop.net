using System.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	public abstract class ConfigurationProvider
	{
		/// <exception cref="System.Exception"/>
		public virtual void Init(Configuration bootstrapConf)
		{
			InitInternal(bootstrapConf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void Close()
		{
			CloseInternal();
		}

		/// <summary>Opens an InputStream at the indicated file</summary>
		/// <param name="bootstrapConf">Configuration</param>
		/// <param name="name">The configuration file name</param>
		/// <returns>configuration</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract InputStream GetConfigurationInputStream(Configuration bootstrapConf
			, string name);

		/// <summary>Derived classes initialize themselves using this method.</summary>
		/// <exception cref="System.Exception"/>
		public abstract void InitInternal(Configuration bootstrapConf);

		/// <summary>Derived classes close themselves using this method.</summary>
		/// <exception cref="System.Exception"/>
		public abstract void CloseInternal();
	}
}
