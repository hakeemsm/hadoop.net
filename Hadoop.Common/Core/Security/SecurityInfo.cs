using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public abstract class SecurityInfo
	{
		/// <summary>Get the KerberosInfo for a given protocol.</summary>
		/// <param name="protocol">interface class</param>
		/// <param name="conf">configuration</param>
		/// <returns>KerberosInfo</returns>
		public abstract KerberosInfo GetKerberosInfo(Type protocol, Configuration conf);

		/// <summary>Get the TokenInfo for a given protocol.</summary>
		/// <param name="protocol">interface class</param>
		/// <param name="conf">configuration object.</param>
		/// <returns>TokenInfo instance</returns>
		public abstract TokenInfo GetTokenInfo(Type protocol, Configuration conf);
	}
}
