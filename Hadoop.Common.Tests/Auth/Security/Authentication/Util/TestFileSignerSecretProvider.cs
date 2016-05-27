using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestFileSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSecrets()
		{
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			testDir.Mkdirs();
			string secretValue = "hadoop";
			FilePath secretFile = new FilePath(testDir, "http-secret.txt");
			TextWriter writer = new FileWriter(secretFile);
			writer.Write(secretValue);
			writer.Close();
			FileSignerSecretProvider secretProvider = new FileSignerSecretProvider();
			Properties secretProviderProps = new Properties();
			secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecretFile, secretFile
				.GetAbsolutePath());
			secretProvider.Init(secretProviderProps, null, -1);
			Assert.AssertArrayEquals(Sharpen.Runtime.GetBytesForString(secretValue), secretProvider
				.GetCurrentSecret());
			byte[][] allSecrets = secretProvider.GetAllSecrets();
			NUnit.Framework.Assert.AreEqual(1, allSecrets.Length);
			Assert.AssertArrayEquals(Sharpen.Runtime.GetBytesForString(secretValue), allSecrets
				[0]);
		}
	}
}
