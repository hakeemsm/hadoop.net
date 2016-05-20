using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class TestKeyProviderDelegationTokenExtension
	{
		public abstract class MockKeyProvider : org.apache.hadoop.crypto.key.KeyProvider, 
			org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
		{
			public MockKeyProvider()
				: base(new org.apache.hadoop.conf.Configuration(false))
			{
			}

			public abstract org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
				(string arg1, org.apache.hadoop.security.Credentials arg2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreateExtension()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.crypto.key.KeyProvider kp = new org.apache.hadoop.crypto.key.UserProvider.Factory
				().createProvider(new java.net.URI("user:///"), conf);
			org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension kpDTE1 = org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension
				.createKeyProviderDelegationTokenExtension(kp);
			NUnit.Framework.Assert.IsNotNull(kpDTE1);
			// Default implementation should be a no-op and return null
			NUnit.Framework.Assert.IsNull(kpDTE1.addDelegationTokens("user", credentials));
			org.apache.hadoop.crypto.key.TestKeyProviderDelegationTokenExtension.MockKeyProvider
				 mock = org.mockito.Mockito.mock<org.apache.hadoop.crypto.key.TestKeyProviderDelegationTokenExtension.MockKeyProvider
				>();
			org.mockito.Mockito.when(mock.getConf()).thenReturn(new org.apache.hadoop.conf.Configuration
				());
			org.mockito.Mockito.when(mock.addDelegationTokens("renewer", credentials)).thenReturn
				(new org.apache.hadoop.security.token.Token<object>[] { new org.apache.hadoop.security.token.Token
				(null, null, new org.apache.hadoop.io.Text("kind"), new org.apache.hadoop.io.Text
				("service")) });
			org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension kpDTE2 = org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension
				.createKeyProviderDelegationTokenExtension(mock);
			org.apache.hadoop.security.token.Token<object>[] tokens = kpDTE2.addDelegationTokens
				("renewer", credentials);
			NUnit.Framework.Assert.IsNotNull(tokens);
			NUnit.Framework.Assert.AreEqual("kind", tokens[0].getKind().ToString());
		}
	}
}
