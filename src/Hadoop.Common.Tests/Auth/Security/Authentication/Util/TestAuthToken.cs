using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Client;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestAuthToken
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConstructor()
		{
			try
			{
				new AuthToken(null, "p", "t");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new AuthToken(string.Empty, "p", "t");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new AuthToken("u", null, "t");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new AuthToken("u", string.Empty, "t");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new AuthToken("u", "p", null);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				new AuthToken("u", "p", string.Empty);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			new AuthToken("u", "p", "t");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetters()
		{
			long expires = Runtime.CurrentTimeMillis() + 50;
			AuthToken token = new AuthToken("u", "p", "t");
			token.SetExpires(expires);
			Assert.Equal("u", token.GetUserName());
			Assert.Equal("p", token.GetName());
			Assert.Equal("t", token.GetType());
			Assert.Equal(expires, token.GetExpires());
			NUnit.Framework.Assert.IsFalse(token.IsExpired());
			Thread.Sleep(70);
			// +20 msec fuzz for timer granularity.
			Assert.True(token.IsExpired());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestToStringAndParse()
		{
			long expires = Runtime.CurrentTimeMillis() + 50;
			AuthToken token = new AuthToken("u", "p", "t");
			token.SetExpires(expires);
			string str = token.ToString();
			token = AuthToken.Parse(str);
			Assert.Equal("p", token.GetName());
			Assert.Equal("t", token.GetType());
			Assert.Equal(expires, token.GetExpires());
			NUnit.Framework.Assert.IsFalse(token.IsExpired());
			Thread.Sleep(70);
			// +20 msec fuzz for timer granularity.
			Assert.True(token.IsExpired());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestParseValidAndInvalid()
		{
			long expires = Runtime.CurrentTimeMillis() + 50;
			AuthToken token = new AuthToken("u", "p", "t");
			token.SetExpires(expires);
			string ostr = token.ToString();
			string str1 = "\"" + ostr + "\"";
			AuthToken.Parse(str1);
			string str2 = ostr + "&s=1234";
			AuthToken.Parse(str2);
			string str = Runtime.Substring(ostr, 0, ostr.IndexOf("e="));
			try
			{
				AuthToken.Parse(str);
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthenticationException)
			{
			}
			catch (Exception)
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}
	}
}
