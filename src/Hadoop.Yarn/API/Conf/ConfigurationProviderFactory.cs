using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	public class ConfigurationProviderFactory
	{
		/// <summary>
		/// Creates an instance of
		/// <see cref="ConfigurationProvider"/>
		/// using given
		/// configuration.
		/// </summary>
		/// <param name="bootstrapConf"/>
		/// <returns>configurationProvider</returns>
		public static ConfigurationProvider GetConfigurationProvider(Configuration bootstrapConf
			)
		{
			Type defaultProviderClass;
			try
			{
				defaultProviderClass = (Type)Sharpen.Runtime.GetType(YarnConfiguration.DefaultRmConfigurationProviderClass
					);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException("Invalid default configuration provider class" + YarnConfiguration
					.DefaultRmConfigurationProviderClass, e);
			}
			ConfigurationProvider configurationProvider = ReflectionUtils.NewInstance(bootstrapConf
				.GetClass<ConfigurationProvider>(YarnConfiguration.RmConfigurationProviderClass, 
				defaultProviderClass), bootstrapConf);
			return configurationProvider;
		}
	}
}
