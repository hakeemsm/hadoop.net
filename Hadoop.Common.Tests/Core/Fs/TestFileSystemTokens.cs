using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFileSystemTokens
	{
		private static string renewer = "renewer!";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithNoToken()
		{
			FileSystemTestHelper.MockFileSystem fs = CreateFileSystemForServiceName(null);
			Credentials credentials = new Credentials();
			fs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(fs, false);
			NUnit.Framework.Assert.AreEqual(0, credentials.NumberOfTokens());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithToken()
		{
			Text service = new Text("singleTokenFs");
			FileSystemTestHelper.MockFileSystem fs = CreateFileSystemForServiceName(service);
			Credentials credentials = new Credentials();
			fs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(fs, true);
			NUnit.Framework.Assert.AreEqual(1, credentials.NumberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithTokenExists()
		{
			Credentials credentials = new Credentials();
			Text service = new Text("singleTokenFs");
			FileSystemTestHelper.MockFileSystem fs = CreateFileSystemForServiceName(service);
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			credentials.AddToken(service, token);
			fs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(fs, false);
			NUnit.Framework.Assert.AreEqual(1, credentials.NumberOfTokens());
			NUnit.Framework.Assert.AreSame(token, credentials.GetToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithChildTokens()
		{
			Credentials credentials = new Credentials();
			Text service1 = new Text("singleTokenFs1");
			Text service2 = new Text("singleTokenFs2");
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName(service1
				);
			FileSystemTestHelper.MockFileSystem fs2 = CreateFileSystemForServiceName(service2
				);
			FileSystemTestHelper.MockFileSystem fs3 = CreateFileSystemForServiceName(null);
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(null
				, fs1, fs2, fs3);
			multiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(multiFs, false);
			// has no tokens of own, only child tokens
			VerifyTokenFetch(fs1, true);
			VerifyTokenFetch(fs2, true);
			VerifyTokenFetch(fs3, false);
			NUnit.Framework.Assert.AreEqual(2, credentials.NumberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithDuplicateChildren()
		{
			Credentials credentials = new Credentials();
			Text service = new Text("singleTokenFs1");
			FileSystemTestHelper.MockFileSystem fs = CreateFileSystemForServiceName(service);
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(null
				, fs, new FilterFileSystem(fs));
			multiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(multiFs, false);
			VerifyTokenFetch(fs, true);
			NUnit.Framework.Assert.AreEqual(1, credentials.NumberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithDuplicateChildrenTokenExists()
		{
			Credentials credentials = new Credentials();
			Text service = new Text("singleTokenFs1");
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			credentials.AddToken(service, token);
			FileSystemTestHelper.MockFileSystem fs = CreateFileSystemForServiceName(service);
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(null
				, fs, new FilterFileSystem(fs));
			multiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(multiFs, false);
			VerifyTokenFetch(fs, false);
			NUnit.Framework.Assert.AreEqual(1, credentials.NumberOfTokens());
			NUnit.Framework.Assert.AreSame(token, credentials.GetToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithChildTokensOneExists()
		{
			Credentials credentials = new Credentials();
			Text service1 = new Text("singleTokenFs1");
			Text service2 = new Text("singleTokenFs2");
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			credentials.AddToken(service2, token);
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName(service1
				);
			FileSystemTestHelper.MockFileSystem fs2 = CreateFileSystemForServiceName(service2
				);
			FileSystemTestHelper.MockFileSystem fs3 = CreateFileSystemForServiceName(null);
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(null
				, fs1, fs2, fs3);
			multiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(multiFs, false);
			VerifyTokenFetch(fs1, true);
			VerifyTokenFetch(fs2, false);
			// we had added its token to credentials
			VerifyTokenFetch(fs3, false);
			NUnit.Framework.Assert.AreEqual(2, credentials.NumberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service1));
			NUnit.Framework.Assert.AreSame(token, credentials.GetToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithMyOwnAndChildTokens()
		{
			Credentials credentials = new Credentials();
			Text service1 = new Text("singleTokenFs1");
			Text service2 = new Text("singleTokenFs2");
			Text myService = new Text("multiTokenFs");
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			credentials.AddToken(service2, token);
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName(service1
				);
			FileSystemTestHelper.MockFileSystem fs2 = CreateFileSystemForServiceName(service2
				);
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(myService
				, fs1, fs2);
			multiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(multiFs, true);
			// its own token and also of its children
			VerifyTokenFetch(fs1, true);
			VerifyTokenFetch(fs2, false);
			// we had added its token to credentials 
			NUnit.Framework.Assert.AreEqual(3, credentials.NumberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(myService));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithMyOwnExistsAndChildTokens()
		{
			Credentials credentials = new Credentials();
			Text service1 = new Text("singleTokenFs1");
			Text service2 = new Text("singleTokenFs2");
			Text myService = new Text("multiTokenFs");
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			credentials.AddToken(myService, token);
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName(service1
				);
			FileSystemTestHelper.MockFileSystem fs2 = CreateFileSystemForServiceName(service2
				);
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(myService
				, fs1, fs2);
			multiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(multiFs, false);
			// we had added its token to credentials
			VerifyTokenFetch(fs1, true);
			VerifyTokenFetch(fs2, true);
			NUnit.Framework.Assert.AreEqual(3, credentials.NumberOfTokens());
			NUnit.Framework.Assert.AreSame(token, credentials.GetToken(myService));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsWithNestedDuplicatesChildren()
		{
			Credentials credentials = new Credentials();
			Text service1 = new Text("singleTokenFs1");
			Text service2 = new Text("singleTokenFs2");
			Text service4 = new Text("singleTokenFs4");
			Text multiService = new Text("multiTokenFs");
			Org.Apache.Hadoop.Security.Token.Token<object> token2 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			credentials.AddToken(service2, token2);
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName(service1
				);
			FileSystemTestHelper.MockFileSystem fs1B = CreateFileSystemForServiceName(service1
				);
			FileSystemTestHelper.MockFileSystem fs2 = CreateFileSystemForServiceName(service2
				);
			FileSystemTestHelper.MockFileSystem fs3 = CreateFileSystemForServiceName(null);
			FileSystemTestHelper.MockFileSystem fs4 = CreateFileSystemForServiceName(service4
				);
			// now let's get dirty!  ensure dup tokens aren't fetched even when
			// repeated and dupped in a nested fs.  fs4 is a real test of the drill
			// down: multi-filter-multi-filter-filter-fs4.
			FileSystemTestHelper.MockFileSystem multiFs = CreateFileSystemForServiceName(multiService
				, fs1, fs1B, fs2, fs2, new FilterFileSystem(fs3), new FilterFileSystem(new FilterFileSystem
				(fs4)));
			FileSystemTestHelper.MockFileSystem superMultiFs = CreateFileSystemForServiceName
				(null, fs1, fs1B, fs1, new FilterFileSystem(fs3), new FilterFileSystem(multiFs));
			superMultiFs.AddDelegationTokens(renewer, credentials);
			VerifyTokenFetch(superMultiFs, false);
			// does not have its own token
			VerifyTokenFetch(multiFs, true);
			// has its own token
			VerifyTokenFetch(fs1, true);
			VerifyTokenFetch(fs2, false);
			// we had added its token to credentials
			VerifyTokenFetch(fs3, false);
			// has no tokens
			VerifyTokenFetch(fs4, true);
			NUnit.Framework.Assert.AreEqual(4, credentials.NumberOfTokens());
			//fs1+fs2+fs4+multifs (fs3=0)
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service2));
			NUnit.Framework.Assert.AreSame(token2, credentials.GetToken(service2));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(multiService));
			NUnit.Framework.Assert.IsNotNull(credentials.GetToken(service4));
		}

		/// <exception cref="System.IO.IOException"/>
		public static FileSystemTestHelper.MockFileSystem CreateFileSystemForServiceName(
			Text service, params FileSystem[] children)
		{
			FileSystemTestHelper.MockFileSystem fs = new FileSystemTestHelper.MockFileSystem(
				);
			FileSystemTestHelper.MockFileSystem mockFs = ((FileSystemTestHelper.MockFileSystem
				)fs.GetRawFileSystem());
			if (service != null)
			{
				Org.Mockito.Mockito.When(mockFs.GetCanonicalServiceName()).ThenReturn(service.ToString
					());
				Org.Mockito.Mockito.When(mockFs.GetDelegationToken(Matchers.Any<string>())).ThenAnswer
					(new _Answer_255(service));
			}
			Org.Mockito.Mockito.When(mockFs.GetChildFileSystems()).ThenReturn(children);
			return fs;
		}

		private sealed class _Answer_255 : Answer<Org.Apache.Hadoop.Security.Token.Token<
			object>>
		{
			public _Answer_255(Text service)
			{
				this.service = service;
			}

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<object> Answer(InvocationOnMock invocation
				)
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
					<TokenIdentifier>();
				token.SetService(service);
				return token;
			}

			private readonly Text service;
		}

		// check that canonical name was requested, if renewer is not null that
		// a token was requested, and that child fs was invoked
		/// <exception cref="System.IO.IOException"/>
		private void VerifyTokenFetch(FileSystemTestHelper.MockFileSystem fs, bool expected
			)
		{
			Org.Mockito.Mockito.Verify(((FileSystemTestHelper.MockFileSystem)fs.GetRawFileSystem
				()), Org.Mockito.Mockito.AtLeast(1)).GetCanonicalServiceName();
			if (expected)
			{
				Org.Mockito.Mockito.Verify(((FileSystemTestHelper.MockFileSystem)fs.GetRawFileSystem
					())).GetDelegationToken(renewer);
			}
			else
			{
				Org.Mockito.Mockito.Verify(((FileSystemTestHelper.MockFileSystem)fs.GetRawFileSystem
					()), Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.Any<string>());
			}
			Org.Mockito.Mockito.Verify(((FileSystemTestHelper.MockFileSystem)fs.GetRawFileSystem
				()), Org.Mockito.Mockito.AtLeast(1)).GetChildFileSystems();
		}
	}
}
