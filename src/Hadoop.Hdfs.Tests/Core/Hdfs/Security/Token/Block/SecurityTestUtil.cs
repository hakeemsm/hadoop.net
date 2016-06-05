using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>Utilities for security tests</summary>
	public class SecurityTestUtil
	{
		/// <summary>check if an access token is expired.</summary>
		/// <remarks>
		/// check if an access token is expired. return true when token is expired,
		/// false otherwise
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static bool IsBlockTokenExpired(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> token)
		{
			return BlockTokenSecretManager.IsTokenExpired(token);
		}

		/// <summary>set access token lifetime.</summary>
		public static void SetBlockTokenLifetime(BlockTokenSecretManager handler, long tokenLifetime
			)
		{
			handler.SetTokenLifetime(tokenLifetime);
		}
	}
}
