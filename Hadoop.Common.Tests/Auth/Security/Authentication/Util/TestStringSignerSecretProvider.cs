using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestStringSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSecrets()
		{
			string secretStr = "secret";
			StringSignerSecretProvider secretProvider = new StringSignerSecretProvider();
			Properties secretProviderProps = new Properties();
			secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
			secretProvider.Init(secretProviderProps, null, -1);
			byte[] secretBytes = Sharpen.Runtime.GetBytesForString(secretStr);
			Assert.AssertArrayEquals(secretBytes, secretProvider.GetCurrentSecret());
			byte[][] allSecrets = secretProvider.GetAllSecrets();
			NUnit.Framework.Assert.AreEqual(1, allSecrets.Length);
			Assert.AssertArrayEquals(secretBytes, allSecrets[0]);
		}
	}
}
