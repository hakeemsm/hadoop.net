using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	public class TestAuthenticationToken
	{
		[NUnit.Framework.Test]
		public virtual void testAnonymous()
		{
			NUnit.Framework.Assert.IsNotNull(org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS);
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS.getUserName());
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS.getName());
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS.getType());
			NUnit.Framework.Assert.AreEqual(-1, org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS.getExpires());
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS.isExpired());
		}
	}
}
