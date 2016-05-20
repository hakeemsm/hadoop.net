using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestAuthToken
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConstructor()
		{
			try
			{
				new org.apache.hadoop.security.authentication.util.AuthToken(null, "p", "t");
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new org.apache.hadoop.security.authentication.util.AuthToken(string.Empty, "p", "t"
					);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new org.apache.hadoop.security.authentication.util.AuthToken("u", null, "t");
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new org.apache.hadoop.security.authentication.util.AuthToken("u", string.Empty, "t"
					);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new org.apache.hadoop.security.authentication.util.AuthToken("u", "p", null);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new org.apache.hadoop.security.authentication.util.AuthToken("u", "p", string.Empty
					);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			new org.apache.hadoop.security.authentication.util.AuthToken("u", "p", "t");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetters()
		{
			long expires = Sharpen.Runtime.currentTimeMillis() + 50;
			org.apache.hadoop.security.authentication.util.AuthToken token = new org.apache.hadoop.security.authentication.util.AuthToken
				("u", "p", "t");
			token.setExpires(expires);
			NUnit.Framework.Assert.AreEqual("u", token.getUserName());
			NUnit.Framework.Assert.AreEqual("p", token.getName());
			NUnit.Framework.Assert.AreEqual("t", token.getType());
			NUnit.Framework.Assert.AreEqual(expires, token.getExpires());
			NUnit.Framework.Assert.IsFalse(token.isExpired());
			java.lang.Thread.sleep(70);
			// +20 msec fuzz for timer granularity.
			NUnit.Framework.Assert.IsTrue(token.isExpired());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testToStringAndParse()
		{
			long expires = Sharpen.Runtime.currentTimeMillis() + 50;
			org.apache.hadoop.security.authentication.util.AuthToken token = new org.apache.hadoop.security.authentication.util.AuthToken
				("u", "p", "t");
			token.setExpires(expires);
			string str = token.ToString();
			token = org.apache.hadoop.security.authentication.util.AuthToken.parse(str);
			NUnit.Framework.Assert.AreEqual("p", token.getName());
			NUnit.Framework.Assert.AreEqual("t", token.getType());
			NUnit.Framework.Assert.AreEqual(expires, token.getExpires());
			NUnit.Framework.Assert.IsFalse(token.isExpired());
			java.lang.Thread.sleep(70);
			// +20 msec fuzz for timer granularity.
			NUnit.Framework.Assert.IsTrue(token.isExpired());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testParseValidAndInvalid()
		{
			long expires = Sharpen.Runtime.currentTimeMillis() + 50;
			org.apache.hadoop.security.authentication.util.AuthToken token = new org.apache.hadoop.security.authentication.util.AuthToken
				("u", "p", "t");
			token.setExpires(expires);
			string ostr = token.ToString();
			string str1 = "\"" + ostr + "\"";
			org.apache.hadoop.security.authentication.util.AuthToken.parse(str1);
			string str2 = ostr + "&s=1234";
			org.apache.hadoop.security.authentication.util.AuthToken.parse(str2);
			string str = Sharpen.Runtime.substring(ostr, 0, ostr.IndexOf("e="));
			try
			{
				org.apache.hadoop.security.authentication.util.AuthToken.parse(str);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException)
			{
			}
			catch (System.Exception)
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}
	}
}
