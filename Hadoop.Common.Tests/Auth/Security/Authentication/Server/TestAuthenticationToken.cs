using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	public class TestAuthenticationToken
	{
		[Fact]
		public virtual void TestAnonymous()
		{
			NUnit.Framework.Assert.IsNotNull(AuthenticationToken.Anonymous);
			Assert.Equal(null, AuthenticationToken.Anonymous.GetUserName()
				);
			Assert.Equal(null, AuthenticationToken.Anonymous.GetName());
			Assert.Equal(null, AuthenticationToken.Anonymous.GetType());
			Assert.Equal(-1, AuthenticationToken.Anonymous.GetExpires());
			NUnit.Framework.Assert.IsFalse(AuthenticationToken.Anonymous.IsExpired());
		}
	}
}
