using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestFileSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetSecrets()
		{
			java.io.File testDir = new java.io.File(Sharpen.Runtime.getProperty("test.build.data"
				, "target/test-dir"));
			testDir.mkdirs();
			string secretValue = "hadoop";
			java.io.File secretFile = new java.io.File(testDir, "http-secret.txt");
			System.IO.TextWriter writer = new java.io.FileWriter(secretFile);
			writer.write(secretValue);
			writer.close();
			org.apache.hadoop.security.authentication.util.FileSignerSecretProvider secretProvider
				 = new org.apache.hadoop.security.authentication.util.FileSignerSecretProvider();
			java.util.Properties secretProviderProps = new java.util.Properties();
			secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET_FILE, secretFile.getAbsolutePath());
			secretProvider.init(secretProviderProps, null, -1);
			NUnit.Framework.Assert.assertArrayEquals(Sharpen.Runtime.getBytesForString(secretValue
				), secretProvider.getCurrentSecret());
			byte[][] allSecrets = secretProvider.getAllSecrets();
			NUnit.Framework.Assert.AreEqual(1, allSecrets.Length);
			NUnit.Framework.Assert.assertArrayEquals(Sharpen.Runtime.getBytesForString(secretValue
				), allSecrets[0]);
		}
	}
}
