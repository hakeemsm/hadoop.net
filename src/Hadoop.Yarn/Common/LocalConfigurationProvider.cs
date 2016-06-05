using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class LocalConfigurationProvider : ConfigurationProvider
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override InputStream GetConfigurationInputStream(Configuration bootstrapConf
			, string name)
		{
			if (name == null || name.IsEmpty())
			{
				throw new YarnException("Illegal argument! The parameter should not be null or empty"
					);
			}
			else
			{
				if (YarnConfiguration.RmConfigurationFiles.Contains(name))
				{
					return bootstrapConf.GetConfResourceAsInputStream(name);
				}
			}
			return new FileInputStream(name);
		}

		/// <exception cref="System.Exception"/>
		public override void InitInternal(Configuration bootstrapConf)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		public override void CloseInternal()
		{
		}
		// Do nothing
	}
}
