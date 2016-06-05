using System;
using System.Collections.Generic;
using System.Net;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>Provides SaslProperties to be used for a connection.</summary>
	/// <remarks>
	/// Provides SaslProperties to be used for a connection.
	/// The default implementation is to read the values from configuration.
	/// This class can be overridden to provide custom SaslProperties.
	/// The custom class can be specified via configuration.
	/// </remarks>
	public class SaslPropertiesResolver : Configurable
	{
		private IDictionary<string, string> properties;

		internal Configuration conf;

		/// <summary>Returns an instance of SaslPropertiesResolver.</summary>
		/// <remarks>
		/// Returns an instance of SaslPropertiesResolver.
		/// Looks up the configuration to see if there is custom class specified.
		/// Constructs the instance by passing the configuration directly to the
		/// constructor to achieve thread safety using final fields.
		/// </remarks>
		/// <param name="conf"/>
		/// <returns>SaslPropertiesResolver</returns>
		public static SaslPropertiesResolver GetInstance(Configuration conf)
		{
			Type clazz = conf.GetClass<SaslPropertiesResolver>(CommonConfigurationKeysPublic.
				HadoopSecuritySaslPropsResolverClass, typeof(SaslPropertiesResolver));
			return ReflectionUtils.NewInstance(clazz, conf);
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
			properties = new SortedDictionary<string, string>();
			string[] qop = conf.GetTrimmedStrings(CommonConfigurationKeysPublic.HadoopRpcProtection
				, SaslRpcServer.QualityOfProtection.Authentication.ToString());
			for (int i = 0; i < qop.Length; i++)
			{
				qop[i] = SaslRpcServer.QualityOfProtection.ValueOf(StringUtils.ToUpperCase(qop[i]
					)).GetSaslQop();
			}
			properties[Javax.Security.Sasl.Sasl.Qop] = StringUtils.Join(",", qop);
			properties[Javax.Security.Sasl.Sasl.ServerAuth] = "true";
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>The default Sasl Properties read from the configuration</summary>
		/// <returns>sasl Properties</returns>
		public virtual IDictionary<string, string> GetDefaultProperties()
		{
			return properties;
		}

		/// <summary>Identify the Sasl Properties to be used for a connection with a  client.
		/// 	</summary>
		/// <param name="clientAddress">client's address</param>
		/// <returns>the sasl properties to be used for the connection.</returns>
		public virtual IDictionary<string, string> GetServerProperties(IPAddress clientAddress
			)
		{
			return properties;
		}

		/// <summary>Identify the Sasl Properties to be used for a connection with a server.</summary>
		/// <param name="serverAddress">server's address</param>
		/// <returns>the sasl properties to be used for the connection.</returns>
		public virtual IDictionary<string, string> GetClientProperties(IPAddress serverAddress
			)
		{
			return properties;
		}
	}
}
