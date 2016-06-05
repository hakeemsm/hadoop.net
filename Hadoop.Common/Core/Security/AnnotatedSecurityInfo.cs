using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>Constructs SecurityInfo from Annotations provided in protocol interface.
	/// 	</summary>
	public class AnnotatedSecurityInfo : SecurityInfo
	{
		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			return protocol.GetAnnotation<KerberosInfo>();
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			return protocol.GetAnnotation<TokenInfo>();
		}
	}
}
