using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	public class TimelineDelegationTokenSelector : TokenSelector<TimelineDelegationTokenIdentifier
		>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TimelineDelegationTokenSelector
			));

		public virtual Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
			> SelectToken(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Looking for a token with service " + service.ToString());
			}
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Token kind is " + token.GetKind().ToString() + " and the token's service name is "
						 + token.GetService());
				}
				if (TimelineDelegationTokenIdentifier.KindName.Equals(token.GetKind()) && service
					.Equals(token.GetService()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier>
						)token;
				}
			}
			return null;
		}
	}
}
