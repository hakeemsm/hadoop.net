using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Javax.Security.Auth.X500;
using Mono.Math;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Alias;
using Org.Bouncycastle.X509;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Ssl
{
	public class KeyStoreTestUtil
	{
		/// <exception cref="System.Exception"/>
		public static string GetClasspathDir(Type klass)
		{
			string file = klass.FullName;
			file = file.Replace('.', '/') + ".class";
			Uri url = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource(file
				);
			string baseDir = url.ToURI().GetPath();
			baseDir = Sharpen.Runtime.Substring(baseDir, 0, baseDir.Length - file.Length - 1);
			return baseDir;
		}

		/// <exception cref="Sharpen.CertificateEncodingException"/>
		/// <exception cref="Sharpen.InvalidKeyException"/>
		/// <exception cref="System.InvalidOperationException"/>
		/// <exception cref="Sharpen.NoSuchProviderException"/>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="Sharpen.SignatureException"/>
		public static X509Certificate GenerateCertificate(string dn, Sharpen.KeyPair pair
			, int days, string algorithm)
		{
			DateTime from = new DateTime();
			DateTime to = Sharpen.Extensions.CreateDate(from.GetTime() + days * 86400000l);
			BigInteger sn = new BigInteger(64, new SecureRandom());
			Sharpen.KeyPair keyPair = pair;
			X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
			X500Principal dnName = new X500Principal(dn);
			certGen.SetSerialNumber(sn);
			certGen.SetIssuerDN(dnName);
			certGen.SetNotBefore(from);
			certGen.SetNotAfter(to);
			certGen.SetSubjectDN(dnName);
			certGen.SetPublicKey(keyPair.GetPublic());
			certGen.SetSignatureAlgorithm(algorithm);
			X509Certificate cert = certGen.Generate(pair.GetPrivate());
			return cert;
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		public static Sharpen.KeyPair GenerateKeyPair(string algorithm)
		{
			KeyPairGenerator keyGen = KeyPairGenerator.GetInstance(algorithm);
			keyGen.Initialize(1024);
			return keyGen.GenKeyPair();
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		private static KeyStore CreateEmptyKeyStore()
		{
			KeyStore ks = KeyStore.GetInstance("JKS");
			ks.Load(null, null);
			// initialize
			return ks;
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void SaveKeyStore(KeyStore ks, string filename, string password)
		{
			FileOutputStream @out = new FileOutputStream(filename);
			try
			{
				ks.Store(@out, password.ToCharArray());
			}
			finally
			{
				@out.Close();
			}
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateKeyStore(string filename, string password, string alias, 
			Key privateKey, Certificate cert)
		{
			KeyStore ks = CreateEmptyKeyStore();
			ks.SetKeyEntry(alias, privateKey, password.ToCharArray(), new Certificate[] { cert
				 });
			SaveKeyStore(ks, filename, password);
		}

		/// <summary>Creates a keystore with a single key and saves it to a file.</summary>
		/// <param name="filename">String file to save</param>
		/// <param name="password">String store password to set on keystore</param>
		/// <param name="keyPassword">String key password to set on key</param>
		/// <param name="alias">String alias to use for the key</param>
		/// <param name="privateKey">Key to save in keystore</param>
		/// <param name="cert">Certificate to use as certificate chain associated to key</param>
		/// <exception cref="Sharpen.GeneralSecurityException">for any error with the security APIs
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if there is an I/O error saving the file</exception>
		public static void CreateKeyStore(string filename, string password, string keyPassword
			, string alias, Key privateKey, Certificate cert)
		{
			KeyStore ks = CreateEmptyKeyStore();
			ks.SetKeyEntry(alias, privateKey, keyPassword.ToCharArray(), new Certificate[] { 
				cert });
			SaveKeyStore(ks, filename, password);
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateTrustStore(string filename, string password, string alias
			, Certificate cert)
		{
			KeyStore ks = CreateEmptyKeyStore();
			ks.SetCertificateEntry(alias, cert);
			SaveKeyStore(ks, filename, password);
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateTrustStore<T>(string filename, string password, IDictionary
			<string, T> certs)
			where T : Certificate
		{
			KeyStore ks = CreateEmptyKeyStore();
			foreach (KeyValuePair<string, T> cert in certs)
			{
				ks.SetCertificateEntry(cert.Key, cert.Value);
			}
			SaveKeyStore(ks, filename, password);
		}

		/// <exception cref="System.Exception"/>
		public static void CleanupSSLConfig(string keystoresDir, string sslConfDir)
		{
			FilePath f = new FilePath(keystoresDir + "/clientKS.jks");
			f.Delete();
			f = new FilePath(keystoresDir + "/serverKS.jks");
			f.Delete();
			f = new FilePath(keystoresDir + "/trustKS.jks");
			f.Delete();
			f = new FilePath(sslConfDir + "/ssl-client.xml");
			f.Delete();
			f = new FilePath(sslConfDir + "/ssl-server.xml");
			f.Delete();
		}

		/// <summary>
		/// Performs complete setup of SSL configuration in preparation for testing an
		/// SSLFactory.
		/// </summary>
		/// <remarks>
		/// Performs complete setup of SSL configuration in preparation for testing an
		/// SSLFactory.  This includes keys, certs, keystores, truststores, the server
		/// SSL configuration file, the client SSL configuration file, and the master
		/// configuration file read by the SSLFactory.
		/// </remarks>
		/// <param name="keystoresDir">String directory to save keystores</param>
		/// <param name="sslConfDir">String directory to save SSL configuration files</param>
		/// <param name="conf">
		/// Configuration master configuration to be used by an SSLFactory,
		/// which will be mutated by this method
		/// </param>
		/// <param name="useClientCert">
		/// boolean true to make the client present a cert in the
		/// SSL handshake
		/// </param>
		/// <exception cref="System.Exception"/>
		public static void SetupSSLConfig(string keystoresDir, string sslConfDir, Configuration
			 conf, bool useClientCert)
		{
			SetupSSLConfig(keystoresDir, sslConfDir, conf, useClientCert, true);
		}

		/// <summary>
		/// Performs complete setup of SSL configuration in preparation for testing an
		/// SSLFactory.
		/// </summary>
		/// <remarks>
		/// Performs complete setup of SSL configuration in preparation for testing an
		/// SSLFactory.  This includes keys, certs, keystores, truststores, the server
		/// SSL configuration file, the client SSL configuration file, and the master
		/// configuration file read by the SSLFactory.
		/// </remarks>
		/// <param name="keystoresDir">String directory to save keystores</param>
		/// <param name="sslConfDir">String directory to save SSL configuration files</param>
		/// <param name="conf">
		/// Configuration master configuration to be used by an SSLFactory,
		/// which will be mutated by this method
		/// </param>
		/// <param name="useClientCert">
		/// boolean true to make the client present a cert in the
		/// SSL handshake
		/// </param>
		/// <param name="trustStore">boolean true to create truststore, false not to create it
		/// 	</param>
		/// <exception cref="System.Exception"/>
		public static void SetupSSLConfig(string keystoresDir, string sslConfDir, Configuration
			 conf, bool useClientCert, bool trustStore)
		{
			string clientKS = keystoresDir + "/clientKS.jks";
			string clientPassword = "clientP";
			string serverKS = keystoresDir + "/serverKS.jks";
			string serverPassword = "serverP";
			string trustKS = null;
			string trustPassword = "trustP";
			FilePath sslClientConfFile = new FilePath(sslConfDir + "/ssl-client.xml");
			FilePath sslServerConfFile = new FilePath(sslConfDir + "/ssl-server.xml");
			IDictionary<string, X509Certificate> certs = new Dictionary<string, X509Certificate
				>();
			if (useClientCert)
			{
				Sharpen.KeyPair cKP = KeyStoreTestUtil.GenerateKeyPair("RSA");
				X509Certificate cCert = KeyStoreTestUtil.GenerateCertificate("CN=localhost, O=client"
					, cKP, 30, "SHA1withRSA");
				KeyStoreTestUtil.CreateKeyStore(clientKS, clientPassword, "client", cKP.GetPrivate
					(), cCert);
				certs["client"] = cCert;
			}
			Sharpen.KeyPair sKP = KeyStoreTestUtil.GenerateKeyPair("RSA");
			X509Certificate sCert = KeyStoreTestUtil.GenerateCertificate("CN=localhost, O=server"
				, sKP, 30, "SHA1withRSA");
			KeyStoreTestUtil.CreateKeyStore(serverKS, serverPassword, "server", sKP.GetPrivate
				(), sCert);
			certs["server"] = sCert;
			if (trustStore)
			{
				trustKS = keystoresDir + "/trustKS.jks";
				KeyStoreTestUtil.CreateTrustStore(trustKS, trustPassword, certs);
			}
			Configuration clientSSLConf = CreateClientSSLConfig(clientKS, clientPassword, clientPassword
				, trustKS);
			Configuration serverSSLConf = CreateServerSSLConfig(serverKS, serverPassword, serverPassword
				, trustKS);
			SaveConfig(sslClientConfFile, clientSSLConf);
			SaveConfig(sslServerConfFile, serverSSLConf);
			conf.Set(SSLFactory.SslHostnameVerifierKey, "ALLOW_ALL");
			conf.Set(SSLFactory.SslClientConfKey, sslClientConfFile.GetName());
			conf.Set(SSLFactory.SslServerConfKey, sslServerConfFile.GetName());
			conf.SetBoolean(SSLFactory.SslRequireClientCertKey, useClientCert);
		}

		/// <summary>Creates SSL configuration for a client.</summary>
		/// <param name="clientKS">String client keystore file</param>
		/// <param name="password">
		/// String store password, or null to avoid setting store
		/// password
		/// </param>
		/// <param name="keyPassword">
		/// String key password, or null to avoid setting key
		/// password
		/// </param>
		/// <param name="trustKS">String truststore file</param>
		/// <returns>Configuration for client SSL</returns>
		public static Configuration CreateClientSSLConfig(string clientKS, string password
			, string keyPassword, string trustKS)
		{
			Configuration clientSSLConf = CreateSSLConfig(SSLFactory.Mode.Client, clientKS, password
				, keyPassword, trustKS);
			return clientSSLConf;
		}

		/// <summary>Creates SSL configuration for a server.</summary>
		/// <param name="serverKS">String server keystore file</param>
		/// <param name="password">
		/// String store password, or null to avoid setting store
		/// password
		/// </param>
		/// <param name="keyPassword">
		/// String key password, or null to avoid setting key
		/// password
		/// </param>
		/// <param name="trustKS">String truststore file</param>
		/// <returns>Configuration for server SSL</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Configuration CreateServerSSLConfig(string serverKS, string password
			, string keyPassword, string trustKS)
		{
			Configuration serverSSLConf = CreateSSLConfig(SSLFactory.Mode.Server, serverKS, password
				, keyPassword, trustKS);
			return serverSSLConf;
		}

		/// <summary>Creates SSL configuration.</summary>
		/// <param name="mode">SSLFactory.Mode mode to configure</param>
		/// <param name="keystore">String keystore file</param>
		/// <param name="password">
		/// String store password, or null to avoid setting store
		/// password
		/// </param>
		/// <param name="keyPassword">
		/// String key password, or null to avoid setting key
		/// password
		/// </param>
		/// <param name="trustKS">String truststore file</param>
		/// <returns>Configuration for SSL</returns>
		private static Configuration CreateSSLConfig(SSLFactory.Mode mode, string keystore
			, string password, string keyPassword, string trustKS)
		{
			string trustPassword = "trustP";
			Configuration sslConf = new Configuration(false);
			if (keystore != null)
			{
				sslConf.Set(FileBasedKeyStoresFactory.ResolvePropertyName(mode, FileBasedKeyStoresFactory
					.SslKeystoreLocationTplKey), keystore);
			}
			if (password != null)
			{
				sslConf.Set(FileBasedKeyStoresFactory.ResolvePropertyName(mode, FileBasedKeyStoresFactory
					.SslKeystorePasswordTplKey), password);
			}
			if (keyPassword != null)
			{
				sslConf.Set(FileBasedKeyStoresFactory.ResolvePropertyName(mode, FileBasedKeyStoresFactory
					.SslKeystoreKeypasswordTplKey), keyPassword);
			}
			if (trustKS != null)
			{
				sslConf.Set(FileBasedKeyStoresFactory.ResolvePropertyName(mode, FileBasedKeyStoresFactory
					.SslTruststoreLocationTplKey), trustKS);
			}
			if (trustPassword != null)
			{
				sslConf.Set(FileBasedKeyStoresFactory.ResolvePropertyName(mode, FileBasedKeyStoresFactory
					.SslTruststorePasswordTplKey), trustPassword);
			}
			sslConf.Set(FileBasedKeyStoresFactory.ResolvePropertyName(mode, FileBasedKeyStoresFactory
				.SslTruststoreReloadIntervalTplKey), "1000");
			return sslConf;
		}

		/// <summary>Saves configuration to a file.</summary>
		/// <param name="file">File to save</param>
		/// <param name="conf">Configuration contents to write to file</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error saving the file</exception>
		public static void SaveConfig(FilePath file, Configuration conf)
		{
			TextWriter writer = new FileWriter(file);
			try
			{
				conf.WriteXml(writer);
			}
			finally
			{
				writer.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void ProvisionPasswordsToCredentialProvider()
		{
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			Configuration conf = new Configuration();
			Path jksPath = new Path(testDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(testDir, "test.jks");
			file.Delete();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CredentialProvider provider = CredentialProviderFactory.GetProviders(conf)[0];
			char[] keypass = new char[] { 'k', 'e', 'y', 'p', 'a', 's', 's' };
			char[] storepass = new char[] { 's', 't', 'o', 'r', 'e', 'p', 'a', 's', 's' };
			// create new aliases
			try
			{
				provider.CreateCredentialEntry(FileBasedKeyStoresFactory.ResolvePropertyName(SSLFactory.Mode
					.Server, FileBasedKeyStoresFactory.SslKeystorePasswordTplKey), storepass);
				provider.CreateCredentialEntry(FileBasedKeyStoresFactory.ResolvePropertyName(SSLFactory.Mode
					.Server, FileBasedKeyStoresFactory.SslKeystoreKeypasswordTplKey), keypass);
				// write out so that it can be found in checks
				provider.Flush();
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				throw;
			}
		}
	}
}
