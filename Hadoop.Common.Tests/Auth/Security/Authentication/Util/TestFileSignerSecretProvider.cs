using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Server;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestFileSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[Fact]
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
			Assert.AssertArrayEquals(Runtime.GetBytesForString(secretValue), secretProvider
				.GetCurrentSecret());
			byte[][] allSecrets = secretProvider.GetAllSecrets();
			Assert.Equal(1, allSecrets.Length);
			Assert.AssertArrayEquals(Runtime.GetBytesForString(secretValue), allSecrets
				[0]);
		}
	}
}
