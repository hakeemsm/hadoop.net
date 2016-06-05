using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token
{
	/// <summary>
	/// Look through tokens to find the first job token that matches the service
	/// and return it.
	/// </summary>
	public class JobTokenSelector : TokenSelector<JobTokenIdentifier>
	{
		public virtual Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> SelectToken
			(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				if (JobTokenIdentifier.KindName.Equals(token.GetKind()) && service.Equals(token.GetService
					()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier>)token;
				}
			}
			return null;
		}
	}
}
