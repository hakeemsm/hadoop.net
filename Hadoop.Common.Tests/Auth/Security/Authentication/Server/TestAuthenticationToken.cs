using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	public class TestAuthenticationToken
	{
		[NUnit.Framework.Test]
		public virtual void TestAnonymous()
		{
			NUnit.Framework.Assert.IsNotNull(AuthenticationToken.Anonymous);
			NUnit.Framework.Assert.AreEqual(null, AuthenticationToken.Anonymous.GetUserName()
				);
			NUnit.Framework.Assert.AreEqual(null, AuthenticationToken.Anonymous.GetName());
			NUnit.Framework.Assert.AreEqual(null, AuthenticationToken.Anonymous.GetType());
			NUnit.Framework.Assert.AreEqual(-1, AuthenticationToken.Anonymous.GetExpires());
			NUnit.Framework.Assert.IsFalse(AuthenticationToken.Anonymous.IsExpired());
		}
	}
}
