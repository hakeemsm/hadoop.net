using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class SchedulerSecurityInfo : SecurityInfo
	{
		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			return null;
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(ApplicationMasterProtocolPB)))
			{
				return null;
			}
			return new _TokenInfo_47();
		}

		private sealed class _TokenInfo_47 : TokenInfo
		{
			public _TokenInfo_47()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public Type Value()
			{
				return typeof(AMRMTokenSelector);
			}
		}
	}
}
