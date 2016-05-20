using Sharpen;

namespace org.apache.hadoop.security.token
{
	/// <summary>Select token of type T from tokens for use with named service</summary>
	/// <?/>
	public interface TokenSelector<T>
		where T : org.apache.hadoop.security.token.TokenIdentifier
	{
		org.apache.hadoop.security.token.Token<T> selectToken(org.apache.hadoop.io.Text service
			, System.Collections.Generic.ICollection<org.apache.hadoop.security.token.Token<
			org.apache.hadoop.security.token.TokenIdentifier>> tokens);
	}
}
