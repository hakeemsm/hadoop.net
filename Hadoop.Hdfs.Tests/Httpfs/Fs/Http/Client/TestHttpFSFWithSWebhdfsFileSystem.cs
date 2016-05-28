using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	public class TestHttpFSFWithSWebhdfsFileSystem : TestHttpFSWithHttpFSFileSystem
	{
		private static string classpathDir;

		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + UUID.RandomUUID();

		private static Configuration sslConf;

		[AfterClass]
		public static void CleanUp()
		{
			new FilePath(classpathDir, "ssl-client.xml").Delete();
			new FilePath(classpathDir, "ssl-server.xml").Delete();
		}

		public TestHttpFSFWithSWebhdfsFileSystem(BaseTestHttpFSWith.Operation operation)
			: base(operation)
		{
			{
				Uri url = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource("classutils.txt"
					);
				classpathDir = url.ToExternalForm();
				if (classpathDir.StartsWith("file:"))
				{
					classpathDir = Sharpen.Runtime.Substring(classpathDir, "file:".Length);
					classpathDir = Sharpen.Runtime.Substring(classpathDir, 0, classpathDir.Length - "/classutils.txt"
						.Length);
				}
				else
				{
					throw new RuntimeException("Cannot find test classes dir");
				}
				FilePath @base = new FilePath(Basedir);
				FileUtil.FullyDelete(@base);
				@base.Mkdirs();
				string keyStoreDir = new FilePath(Basedir).GetAbsolutePath();
				try
				{
					sslConf = new Configuration();
					KeyStoreTestUtil.SetupSSLConfig(keyStoreDir, classpathDir, sslConf, false);
				}
				catch (Exception ex)
				{
					throw new RuntimeException(ex);
				}
				jettyTestHelper = new TestJettyHelper("jks", keyStoreDir + "/serverKS.jks", "serverP"
					);
			}
		}

		protected internal override Type GetFileSystemClass()
		{
			return typeof(SWebHdfsFileSystem);
		}

		protected internal override string GetScheme()
		{
			return "swebhdfs";
		}

		/// <exception cref="System.Exception"/>
		protected internal override FileSystem GetHttpFSFileSystem()
		{
			Configuration conf = new Configuration(sslConf);
			conf.Set("fs.swebhdfs.impl", GetFileSystemClass().FullName);
			URI uri = new URI("swebhdfs://" + TestJettyHelper.GetJettyURL().ToURI().GetAuthority
				());
			return FileSystem.Get(uri, conf);
		}
	}
}
