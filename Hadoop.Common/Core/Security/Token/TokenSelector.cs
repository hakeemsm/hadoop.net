using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token
{
	/// <summary>Select token of type T from tokens for use with named service</summary>
	/// <?/>
	public interface TokenSelector<T>
		where T : TokenIdentifier
	{
		Org.Apache.Hadoop.Security.Token.Token<T> SelectToken(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token
			<TokenIdentifier>> tokens);
	}
}
