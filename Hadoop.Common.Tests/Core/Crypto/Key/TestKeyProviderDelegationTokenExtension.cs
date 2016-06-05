using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestKeyProviderDelegationTokenExtension
	{
		public abstract class MockKeyProvider : KeyProvider, KeyProviderDelegationTokenExtension.DelegationTokenExtension
		{
			public MockKeyProvider()
				: base(new Configuration(false))
			{
			}

			public abstract Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
				(string arg1, Credentials arg2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreateExtension()
		{
			Configuration conf = new Configuration();
			Credentials credentials = new Credentials();
			KeyProvider kp = new UserProvider.Factory().CreateProvider(new URI("user:///"), conf
				);
			KeyProviderDelegationTokenExtension kpDTE1 = KeyProviderDelegationTokenExtension.
				CreateKeyProviderDelegationTokenExtension(kp);
			NUnit.Framework.Assert.IsNotNull(kpDTE1);
			// Default implementation should be a no-op and return null
			NUnit.Framework.Assert.IsNull(kpDTE1.AddDelegationTokens("user", credentials));
			TestKeyProviderDelegationTokenExtension.MockKeyProvider mock = Org.Mockito.Mockito.Mock
				<TestKeyProviderDelegationTokenExtension.MockKeyProvider>();
			Org.Mockito.Mockito.When(mock.GetConf()).ThenReturn(new Configuration());
			Org.Mockito.Mockito.When(mock.AddDelegationTokens("renewer", credentials)).ThenReturn
				(new Org.Apache.Hadoop.Security.Token.Token<object>[] { new Org.Apache.Hadoop.Security.Token.Token
				(null, null, new Text("kind"), new Text("service")) });
			KeyProviderDelegationTokenExtension kpDTE2 = KeyProviderDelegationTokenExtension.
				CreateKeyProviderDelegationTokenExtension(mock);
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = kpDTE2.AddDelegationTokens
				("renewer", credentials);
			NUnit.Framework.Assert.IsNotNull(tokens);
			Assert.Equal("kind", tokens[0].GetKind().ToString());
		}
	}
}
