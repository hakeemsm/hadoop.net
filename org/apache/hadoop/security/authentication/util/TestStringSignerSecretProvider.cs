using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestStringSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetSecrets()
		{
			string secretStr = "secret";
			org.apache.hadoop.security.authentication.util.StringSignerSecretProvider secretProvider
				 = new org.apache.hadoop.security.authentication.util.StringSignerSecretProvider
				();
			java.util.Properties secretProviderProps = new java.util.Properties();
			secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET, "secret");
			secretProvider.init(secretProviderProps, null, -1);
			byte[] secretBytes = Sharpen.Runtime.getBytesForString(secretStr);
			NUnit.Framework.Assert.assertArrayEquals(secretBytes, secretProvider.getCurrentSecret
				());
			byte[][] allSecrets = secretProvider.getAllSecrets();
			NUnit.Framework.Assert.AreEqual(1, allSecrets.Length);
			NUnit.Framework.Assert.assertArrayEquals(secretBytes, allSecrets[0]);
		}
	}
}
