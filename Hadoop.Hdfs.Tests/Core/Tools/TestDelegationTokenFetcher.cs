using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Tools
{
	public class TestDelegationTokenFetcher
	{
		private DistributedFileSystem dfs;

		private Configuration conf;

		private URI uri;

		private const string ServiceValue = "localhost:2005";

		private const string tokenFile = "file.dta";

		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Init()
		{
			dfs = Org.Mockito.Mockito.Mock<DistributedFileSystem>();
			conf = new Configuration();
			uri = new URI("hdfs://" + ServiceValue);
			FileSystemTestHelper.AddFileSystemForTesting(uri, conf, dfs);
		}

		/// <summary>
		/// Verify that when the DelegationTokenFetcher runs, it talks to the Namenode,
		/// pulls out the correct user's token and successfully serializes it to disk.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void ExpectedTokenIsRetrievedFromDFS()
		{
			byte[] ident = new DelegationTokenIdentifier(new Text("owner"), new Text("renewer"
				), new Text("realuser")).GetBytes();
			byte[] pw = new byte[] { 42 };
			Text service = new Text(uri.ToString());
			// Create a token for the fetcher to fetch, wire NN to return it when asked
			// for this particular user.
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> t = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(ident, pw, FakeRenewer.Kind, service);
			Org.Mockito.Mockito.When(dfs.AddDelegationTokens(Matchers.Eq((string)null), Matchers.Any
				<Credentials>())).ThenAnswer(new _Answer_77(service, t));
			Org.Mockito.Mockito.When(dfs.GetUri()).ThenReturn(uri);
			FakeRenewer.Reset();
			FileSystem fileSys = FileSystem.GetLocal(conf);
			try
			{
				DelegationTokenFetcher.Main(new string[] { "-fs", uri.ToString(), tokenFile });
				Path p = new Path(fileSys.GetWorkingDirectory(), tokenFile);
				Credentials creds = Credentials.ReadTokenStorageFile(p, conf);
				IEnumerator<Org.Apache.Hadoop.Security.Token.Token<object>> itr = creds.GetAllTokens
					().GetEnumerator();
				// make sure we got back exactly the 1 token we expected
				NUnit.Framework.Assert.IsTrue(itr.HasNext());
				NUnit.Framework.Assert.AreEqual(t, itr.Next());
				NUnit.Framework.Assert.IsTrue(!itr.HasNext());
				DelegationTokenFetcher.Main(new string[] { "--print", tokenFile });
				DelegationTokenFetcher.Main(new string[] { "--renew", tokenFile });
				NUnit.Framework.Assert.AreEqual(t, FakeRenewer.lastRenewed);
				FakeRenewer.Reset();
				DelegationTokenFetcher.Main(new string[] { "--cancel", tokenFile });
				NUnit.Framework.Assert.AreEqual(t, FakeRenewer.lastCanceled);
			}
			finally
			{
				fileSys.Delete(new Path(tokenFile), true);
			}
		}

		private sealed class _Answer_77 : Answer<Org.Apache.Hadoop.Security.Token.Token<object
			>[]>
		{
			public _Answer_77(Text service, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> t)
			{
				this.service = service;
				this.t = t;
			}

			public Org.Apache.Hadoop.Security.Token.Token<object>[] Answer(InvocationOnMock invocation
				)
			{
				Credentials creds = (Credentials)invocation.GetArguments()[1];
				creds.AddToken(service, t);
				return new Org.Apache.Hadoop.Security.Token.Token<object>[] { t };
			}

			private readonly Text service;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> t;
		}
	}
}
