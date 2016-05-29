using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestSharedCacheClientImpl
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSharedCacheClientImpl
			));

		public static SharedCacheClientImpl client;

		public static ClientSCMProtocol cProtocol;

		private static Path TestRootDir;

		private static FileSystem localFs;

		private static string input = "This is a test file.";

		private static string inputChecksumSHA256 = "f29bc64a9d3732b4b9035125fdb3285f5b6455778edca72414671e0ca3b2e0de";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.BeforeClass]
		public static void BeforeClass()
		{
			localFs = FileSystem.GetLocal(new Configuration());
			TestRootDir = new Path("target", typeof(TestSharedCacheClientImpl).FullName + "-tmpDir"
				).MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory());
		}

		[NUnit.Framework.AfterClass]
		public static void AfterClass()
		{
			try
			{
				if (localFs != null)
				{
					localFs.Close();
				}
			}
			catch (IOException ioe)
			{
				Log.Info("IO exception in closing file system)");
				Sharpen.Runtime.PrintStackTrace(ioe);
			}
		}

		[SetUp]
		public virtual void Setup()
		{
			cProtocol = Org.Mockito.Mockito.Mock<ClientSCMProtocol>();
			client = new _SharedCacheClientImpl_85();
			// do nothing because it is mocked
			client.Init(new Configuration());
			client.Start();
		}

		private sealed class _SharedCacheClientImpl_85 : SharedCacheClientImpl
		{
			public _SharedCacheClientImpl_85()
			{
			}

			protected internal override ClientSCMProtocol CreateClientProxy()
			{
				return TestSharedCacheClientImpl.cProtocol;
			}

			protected internal override void StopClientProxy()
			{
			}
		}

		[TearDown]
		public virtual void Cleanup()
		{
			if (client != null)
			{
				client.Stop();
				client = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUse()
		{
			Path file = new Path("viewfs://test/path");
			UseSharedCacheResourceResponse response = new UseSharedCacheResourceResponsePBImpl
				();
			response.SetPath(file.ToString());
			Org.Mockito.Mockito.When(cProtocol.Use(Matchers.IsA<UseSharedCacheResourceRequest
				>())).ThenReturn(response);
			Path newPath = client.Use(Org.Mockito.Mockito.Mock<ApplicationId>(), "key");
			NUnit.Framework.Assert.AreEqual(file, newPath);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUseError()
		{
			string message = "Mock IOExcepiton!";
			Org.Mockito.Mockito.When(cProtocol.Use(Matchers.IsA<UseSharedCacheResourceRequest
				>())).ThenThrow(new IOException(message));
			client.Use(Org.Mockito.Mockito.Mock<ApplicationId>(), "key");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelease()
		{
			// Release does not care about the return value because it is empty
			Org.Mockito.Mockito.When(cProtocol.Release(Matchers.IsA<ReleaseSharedCacheResourceRequest
				>())).ThenReturn(null);
			client.Release(Org.Mockito.Mockito.Mock<ApplicationId>(), "key");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReleaseError()
		{
			string message = "Mock IOExcepiton!";
			Org.Mockito.Mockito.When(cProtocol.Release(Matchers.IsA<ReleaseSharedCacheResourceRequest
				>())).ThenThrow(new IOException(message));
			client.Release(Org.Mockito.Mockito.Mock<ApplicationId>(), "key");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChecksum()
		{
			string filename = "test1.txt";
			Path file = MakeFile(filename);
			NUnit.Framework.Assert.AreEqual(inputChecksumSHA256, client.GetFileChecksum(file)
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNonexistantFileChecksum()
		{
			Path file = new Path(TestRootDir, "non-existant-file");
			client.GetFileChecksum(file);
		}

		/// <exception cref="System.Exception"/>
		private Path MakeFile(string filename)
		{
			Path file = new Path(TestRootDir, filename);
			DataOutputStream @out = null;
			try
			{
				@out = localFs.Create(file);
				@out.Write(Sharpen.Runtime.GetBytesForString(input, "UTF-8"));
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
			return file;
		}
	}
}
