using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Admin
{
	public class AdminSecurityInfo : SecurityInfo
	{
		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(ResourceManagerAdministrationProtocolPB)))
			{
				return null;
			}
			return new _KerberosInfo_41();
		}

		private sealed class _KerberosInfo_41 : KerberosInfo
		{
			public _KerberosInfo_41()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public string ServerPrincipal()
			{
				return YarnConfiguration.RmPrincipal;
			}

			public string ClientPrincipal()
			{
				return null;
			}
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			return null;
		}
	}
}
