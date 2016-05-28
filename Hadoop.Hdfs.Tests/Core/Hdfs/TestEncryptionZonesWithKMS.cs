using NUnit.Framework;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Org.Apache.Hadoop.Crypto.Key.Kms.Server;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestEncryptionZonesWithKMS : TestEncryptionZones
	{
		private MiniKMS miniKMS;

		protected internal override string GetKeyProviderURI()
		{
			return KMSClientProvider.SchemeName + "://" + miniKMS.GetKMSUrl().ToExternalForm(
				).Replace("://", "@");
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void Setup()
		{
			FilePath kmsDir = new FilePath("target/test-classes/" + UUID.RandomUUID().ToString
				());
			NUnit.Framework.Assert.IsTrue(kmsDir.Mkdirs());
			MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
			miniKMS = miniKMSBuilder.SetKmsConfDir(kmsDir).Build();
			miniKMS.Start();
			base.Setup();
		}

		[TearDown]
		public override void Teardown()
		{
			base.Teardown();
			miniKMS.Stop();
		}

		protected internal override void SetProvider()
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateEZPopulatesEDEKCache()
		{
			Path zonePath = new Path("/TestEncryptionZone");
			fsWrapper.Mkdir(zonePath, FsPermission.GetDirDefault(), false);
			dfsAdmin.CreateEncryptionZone(zonePath, TestKey);
			NUnit.Framework.Assert.IsTrue(((KMSClientProvider)fs.GetClient().GetKeyProvider()
				).GetEncKeyQueueSize(TestKey) > 0);
		}

		/// <exception cref="System.Exception"/>
		public override void TestDelegationToken()
		{
			string renewer = "JobTracker";
			UserGroupInformation.CreateRemoteUser(renewer);
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
				renewer, creds);
			DistributedFileSystem.Log.Debug("Delegation tokens: " + Arrays.AsList(tokens));
			NUnit.Framework.Assert.AreEqual(2, tokens.Length);
			NUnit.Framework.Assert.AreEqual(2, creds.NumberOfTokens());
			// If the dt exists, will not get again
			tokens = fs.AddDelegationTokens(renewer, creds);
			NUnit.Framework.Assert.AreEqual(0, tokens.Length);
			NUnit.Framework.Assert.AreEqual(2, creds.NumberOfTokens());
		}
	}
}
