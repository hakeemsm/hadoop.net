using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Base;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Security;
using Org.Mortbay.Jetty.Webapp;


namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class MiniKMS
	{
		private static Org.Mortbay.Jetty.Server CreateJettyServer(string keyStore, string
			 password, int inPort)
		{
			try
			{
				bool ssl = keyStore != null;
				IPAddress localhost = Extensions.GetAddressByName("localhost");
				string host = "localhost";
				Socket ss = Extensions.CreateServerSocket((inPort < 0) ? 0 : inPort, 50, 
					localhost);
				int port = ss.GetLocalPort();
				ss.Close();
				Org.Mortbay.Jetty.Server server = new Org.Mortbay.Jetty.Server(0);
				if (!ssl)
				{
					server.GetConnectors()[0].SetHost(host);
					server.GetConnectors()[0].SetPort(port);
				}
				else
				{
					SslSocketConnector c = new SslSocketConnectorSecure();
					c.SetHost(host);
					c.SetPort(port);
					c.SetNeedClientAuth(false);
					c.SetKeystore(keyStore);
					c.SetKeystoreType("jks");
					c.SetKeyPassword(password);
					server.SetConnectors(new Connector[] { c });
				}
				return server;
			}
			catch (Exception ex)
			{
				throw new RuntimeException("Could not start embedded servlet container, " + ex.Message
					, ex);
			}
		}

		private static Uri GetJettyURL(Org.Mortbay.Jetty.Server server)
		{
			bool ssl = server.GetConnectors()[0].GetType() == typeof(SslSocketConnectorSecure
				);
			try
			{
				string scheme = (ssl) ? "https" : "http";
				return new Uri(scheme + "://" + server.GetConnectors()[0].GetHost() + ":" + server
					.GetConnectors()[0].GetPort());
			}
			catch (UriFormatException ex)
			{
				throw new RuntimeException("It should never happen, " + ex.Message, ex);
			}
		}

		public class Builder
		{
			private FilePath kmsConfDir;

			private string log4jConfFile;

			private FilePath keyStoreFile;

			private string keyStorePassword;

			private int inPort = -1;

			public Builder()
			{
				kmsConfDir = new FilePath("target/test-classes").GetAbsoluteFile();
				log4jConfFile = "kms-log4j.properties";
			}

			public virtual MiniKMS.Builder SetKmsConfDir(FilePath confDir)
			{
				Preconditions.CheckNotNull(confDir, "KMS conf dir is NULL");
				Preconditions.CheckArgument(confDir.Exists(), "KMS conf dir does not exist");
				kmsConfDir = confDir;
				return this;
			}

			public virtual MiniKMS.Builder SetLog4jConfFile(string log4jConfFile)
			{
				Preconditions.CheckNotNull(log4jConfFile, "log4jconf file is NULL");
				this.log4jConfFile = log4jConfFile;
				return this;
			}

			public virtual MiniKMS.Builder SetPort(int port)
			{
				Preconditions.CheckArgument(port > 0, "input port must be greater than 0");
				this.inPort = port;
				return this;
			}

			public virtual MiniKMS.Builder SetSslConf(FilePath keyStoreFile, string keyStorePassword
				)
			{
				Preconditions.CheckNotNull(keyStoreFile, "keystore file is NULL");
				Preconditions.CheckNotNull(keyStorePassword, "keystore password is NULL");
				Preconditions.CheckArgument(keyStoreFile.Exists(), "keystore file does not exist"
					);
				this.keyStoreFile = keyStoreFile;
				this.keyStorePassword = keyStorePassword;
				return this;
			}

			public virtual MiniKMS Build()
			{
				Preconditions.CheckArgument(kmsConfDir.Exists(), "KMS conf dir does not exist");
				return new MiniKMS(kmsConfDir.GetAbsolutePath(), log4jConfFile, (keyStoreFile != 
					null) ? keyStoreFile.GetAbsolutePath() : null, keyStorePassword, inPort);
			}
		}

		private string kmsConfDir;

		private string log4jConfFile;

		private string keyStore;

		private string keyStorePassword;

		private Org.Mortbay.Jetty.Server jetty;

		private int inPort;

		private Uri kmsURL;

		public MiniKMS(string kmsConfDir, string log4ConfFile, string keyStore, string password
			, int inPort)
		{
			this.kmsConfDir = kmsConfDir;
			this.log4jConfFile = log4ConfFile;
			this.keyStore = keyStore;
			this.keyStorePassword = password;
			this.inPort = inPort;
		}

		/// <exception cref="System.Exception"/>
		public virtual void Start()
		{
			ClassLoader cl = Thread.CurrentThread().GetContextClassLoader();
			Runtime.SetProperty(KMSConfiguration.KmsConfigDir, kmsConfDir);
			FilePath aclsFile = new FilePath(kmsConfDir, "kms-acls.xml");
			if (!aclsFile.Exists())
			{
				InputStream @is = cl.GetResourceAsStream("mini-kms-acls-default.xml");
				OutputStream os = new FileOutputStream(aclsFile);
				IOUtils.Copy(@is, os);
				@is.Close();
				os.Close();
			}
			FilePath coreFile = new FilePath(kmsConfDir, "core-site.xml");
			if (!coreFile.Exists())
			{
				Configuration core = new Configuration();
				TextWriter writer = new FileWriter(coreFile);
				core.WriteXml(writer);
				writer.Close();
			}
			FilePath kmsFile = new FilePath(kmsConfDir, "kms-site.xml");
			if (!kmsFile.Exists())
			{
				Configuration kms = new Configuration(false);
				kms.Set(KMSConfiguration.KeyProviderUri, "jceks://file@" + new Path(kmsConfDir, "kms.keystore"
					).ToUri());
				kms.Set("hadoop.kms.authentication.type", "simple");
				TextWriter writer = new FileWriter(kmsFile);
				kms.WriteXml(writer);
				writer.Close();
			}
			Runtime.SetProperty("log4j.configuration", log4jConfFile);
			jetty = CreateJettyServer(keyStore, keyStorePassword, inPort);
			// we need to do a special handling for MiniKMS to work when in a dir and
			// when in a JAR in the classpath thanks to Jetty way of handling of webapps
			// when they are in the a DIR, WAR or JAR.
			Uri webXmlUrl = cl.GetResource("kms-webapp/WEB-INF/web.xml");
			if (webXmlUrl == null)
			{
				throw new RuntimeException("Could not find kms-webapp/ dir in test classpath");
			}
			bool webXmlInJar = webXmlUrl.AbsolutePath.Contains(".jar!/");
			string webappPath;
			if (webXmlInJar)
			{
				FilePath webInf = new FilePath("target/" + UUID.RandomUUID().ToString() + "/kms-webapp/WEB-INF"
					);
				webInf.Mkdirs();
				new FilePath(webInf, "web.xml").Delete();
				InputStream @is = cl.GetResourceAsStream("kms-webapp/WEB-INF/web.xml");
				OutputStream os = new FileOutputStream(new FilePath(webInf, "web.xml"));
				IOUtils.Copy(@is, os);
				@is.Close();
				os.Close();
				webappPath = webInf.GetParentFile().GetAbsolutePath();
			}
			else
			{
				webappPath = cl.GetResource("kms-webapp").AbsolutePath;
			}
			WebAppContext context = new WebAppContext(webappPath, "/kms");
			if (webXmlInJar)
			{
				context.SetClassLoader(cl);
			}
			jetty.AddHandler(context);
			jetty.Start();
			kmsURL = new Uri(GetJettyURL(jetty), "kms");
		}

		public virtual Uri GetKMSUrl()
		{
			return kmsURL;
		}

		public virtual void Stop()
		{
			if (jetty != null && jetty.IsRunning())
			{
				try
				{
					jetty.Stop();
					jetty = null;
				}
				catch (Exception ex)
				{
					throw new RuntimeException("Could not stop MiniKMS embedded Jetty, " + ex.Message
						, ex);
				}
			}
		}
	}
}
