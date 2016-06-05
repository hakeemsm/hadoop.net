using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class MRClientSecurityInfo : SecurityInfo
	{
		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			return null;
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(MRClientProtocolPB)))
			{
				return null;
			}
			return new _TokenInfo_44();
		}

		private sealed class _TokenInfo_44 : TokenInfo
		{
			public _TokenInfo_44()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public Type Value()
			{
				return typeof(ClientToAMTokenSelector);
			}
		}
	}
}
