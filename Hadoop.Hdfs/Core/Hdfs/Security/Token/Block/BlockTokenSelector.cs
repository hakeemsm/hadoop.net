using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>A block token selector for HDFS</summary>
	public class BlockTokenSelector : TokenSelector<BlockTokenIdentifier>
	{
		public virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> SelectToken
			(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				if (BlockTokenIdentifier.KindName.Equals(token.GetKind()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>)token;
				}
			}
			return null;
		}
	}
}
