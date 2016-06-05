using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Ssl
{
	public class TestReloadingX509TrustManager
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.data", "target/test-dir"
			) + "/" + typeof(TestReloadingX509TrustManager).Name;

		private X509Certificate cert1;

		private X509Certificate cert2;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLoadMissingTrustStore()
		{
			string truststoreLocation = Basedir + "/testmissing.jks";
			ReloadingX509TrustManager tm = new ReloadingX509TrustManager("jks", truststoreLocation
				, "password", 10);
			try
			{
				tm.Init();
			}
			finally
			{
				tm.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLoadCorruptTrustStore()
		{
			string truststoreLocation = Basedir + "/testcorrupt.jks";
			OutputStream os = new FileOutputStream(truststoreLocation);
			os.Write(1);
			os.Close();
			ReloadingX509TrustManager tm = new ReloadingX509TrustManager("jks", truststoreLocation
				, "password", 10);
			try
			{
				tm.Init();
			}
			finally
			{
				tm.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestReload()
		{
			Sharpen.KeyPair kp = KeyStoreTestUtil.GenerateKeyPair("RSA");
			cert1 = KeyStoreTestUtil.GenerateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
			cert2 = KeyStoreTestUtil.GenerateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
			string truststoreLocation = Basedir + "/testreload.jks";
			KeyStoreTestUtil.CreateTrustStore(truststoreLocation, "password", "cert1", cert1);
			ReloadingX509TrustManager tm = new ReloadingX509TrustManager("jks", truststoreLocation
				, "password", 10);
			try
			{
				tm.Init();
				Assert.Equal(1, tm.GetAcceptedIssuers().Length);
				// Wait so that the file modification time is different
				Sharpen.Thread.Sleep((tm.GetReloadInterval() + 1000));
				// Add another cert
				IDictionary<string, X509Certificate> certs = new Dictionary<string, X509Certificate
					>();
				certs["cert1"] = cert1;
				certs["cert2"] = cert2;
				KeyStoreTestUtil.CreateTrustStore(truststoreLocation, "password", certs);
				// and wait to be sure reload has taken place
				Assert.Equal(10, tm.GetReloadInterval());
				// Wait so that the file modification time is different
				Sharpen.Thread.Sleep((tm.GetReloadInterval() + 200));
				Assert.Equal(2, tm.GetAcceptedIssuers().Length);
			}
			finally
			{
				tm.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestReloadMissingTrustStore()
		{
			Sharpen.KeyPair kp = KeyStoreTestUtil.GenerateKeyPair("RSA");
			cert1 = KeyStoreTestUtil.GenerateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
			cert2 = KeyStoreTestUtil.GenerateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
			string truststoreLocation = Basedir + "/testmissing.jks";
			KeyStoreTestUtil.CreateTrustStore(truststoreLocation, "password", "cert1", cert1);
			ReloadingX509TrustManager tm = new ReloadingX509TrustManager("jks", truststoreLocation
				, "password", 10);
			try
			{
				tm.Init();
				Assert.Equal(1, tm.GetAcceptedIssuers().Length);
				X509Certificate cert = tm.GetAcceptedIssuers()[0];
				new FilePath(truststoreLocation).Delete();
				// Wait so that the file modification time is different
				Sharpen.Thread.Sleep((tm.GetReloadInterval() + 200));
				Assert.Equal(1, tm.GetAcceptedIssuers().Length);
				Assert.Equal(cert, tm.GetAcceptedIssuers()[0]);
			}
			finally
			{
				tm.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestReloadCorruptTrustStore()
		{
			Sharpen.KeyPair kp = KeyStoreTestUtil.GenerateKeyPair("RSA");
			cert1 = KeyStoreTestUtil.GenerateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
			cert2 = KeyStoreTestUtil.GenerateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
			string truststoreLocation = Basedir + "/testcorrupt.jks";
			KeyStoreTestUtil.CreateTrustStore(truststoreLocation, "password", "cert1", cert1);
			ReloadingX509TrustManager tm = new ReloadingX509TrustManager("jks", truststoreLocation
				, "password", 10);
			try
			{
				tm.Init();
				Assert.Equal(1, tm.GetAcceptedIssuers().Length);
				X509Certificate cert = tm.GetAcceptedIssuers()[0];
				OutputStream os = new FileOutputStream(truststoreLocation);
				os.Write(1);
				os.Close();
				new FilePath(truststoreLocation).SetLastModified(Runtime.CurrentTimeMillis() - 1000
					);
				// Wait so that the file modification time is different
				Sharpen.Thread.Sleep((tm.GetReloadInterval() + 200));
				Assert.Equal(1, tm.GetAcceptedIssuers().Length);
				Assert.Equal(cert, tm.GetAcceptedIssuers()[0]);
			}
			finally
			{
				tm.Destroy();
			}
		}
	}
}
