using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Security.Client
{
	public class ClientHSTokenSelector : TokenSelector<MRDelegationTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ClientHSTokenSelector)
			);

		public virtual Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier
			> SelectToken(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			Log.Debug("Looking for a token with service " + service.ToString());
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Token kind is " + token.GetKind().ToString() + " and the token's service name is "
						 + token.GetService());
				}
				if (MRDelegationTokenIdentifier.KindName.Equals(token.GetKind()) && service.Equals
					(token.GetService()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier>)token;
				}
			}
			return null;
		}
	}
}
