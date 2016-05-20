using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileSystemTokens
	{
		private static string renewer = "renewer!";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithNoToken()
		{
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs = createFileSystemForServiceName
				(null);
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			fs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(fs, false);
			NUnit.Framework.Assert.AreEqual(0, credentials.numberOfTokens());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithToken()
		{
			org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text("singleTokenFs"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs = createFileSystemForServiceName
				(service);
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			fs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(fs, true);
			NUnit.Framework.Assert.AreEqual(1, credentials.numberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithTokenExists()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text("singleTokenFs"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs = createFileSystemForServiceName
				(service);
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			credentials.addToken(service, token);
			fs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(fs, false);
			NUnit.Framework.Assert.AreEqual(1, credentials.numberOfTokens());
			NUnit.Framework.Assert.AreSame(token, credentials.getToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithChildTokens()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service1 = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.io.Text service2 = new org.apache.hadoop.io.Text("singleTokenFs2"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs1 = createFileSystemForServiceName
				(service1);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs2 = createFileSystemForServiceName
				(service2);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs3 = createFileSystemForServiceName
				(null);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(null, fs1, fs2, fs3);
			multiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(multiFs, false);
			// has no tokens of own, only child tokens
			verifyTokenFetch(fs1, true);
			verifyTokenFetch(fs2, true);
			verifyTokenFetch(fs3, false);
			NUnit.Framework.Assert.AreEqual(2, credentials.numberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithDuplicateChildren()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs = createFileSystemForServiceName
				(service);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(null, fs, new org.apache.hadoop.fs.FilterFileSystem(fs));
			multiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(multiFs, false);
			verifyTokenFetch(fs, true);
			NUnit.Framework.Assert.AreEqual(1, credentials.numberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithDuplicateChildrenTokenExists()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			credentials.addToken(service, token);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs = createFileSystemForServiceName
				(service);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(null, fs, new org.apache.hadoop.fs.FilterFileSystem(fs));
			multiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(multiFs, false);
			verifyTokenFetch(fs, false);
			NUnit.Framework.Assert.AreEqual(1, credentials.numberOfTokens());
			NUnit.Framework.Assert.AreSame(token, credentials.getToken(service));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithChildTokensOneExists()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service1 = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.io.Text service2 = new org.apache.hadoop.io.Text("singleTokenFs2"
				);
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			credentials.addToken(service2, token);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs1 = createFileSystemForServiceName
				(service1);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs2 = createFileSystemForServiceName
				(service2);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs3 = createFileSystemForServiceName
				(null);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(null, fs1, fs2, fs3);
			multiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(multiFs, false);
			verifyTokenFetch(fs1, true);
			verifyTokenFetch(fs2, false);
			// we had added its token to credentials
			verifyTokenFetch(fs3, false);
			NUnit.Framework.Assert.AreEqual(2, credentials.numberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service1));
			NUnit.Framework.Assert.AreSame(token, credentials.getToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithMyOwnAndChildTokens()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service1 = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.io.Text service2 = new org.apache.hadoop.io.Text("singleTokenFs2"
				);
			org.apache.hadoop.io.Text myService = new org.apache.hadoop.io.Text("multiTokenFs"
				);
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			credentials.addToken(service2, token);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs1 = createFileSystemForServiceName
				(service1);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs2 = createFileSystemForServiceName
				(service2);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(myService, fs1, fs2);
			multiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(multiFs, true);
			// its own token and also of its children
			verifyTokenFetch(fs1, true);
			verifyTokenFetch(fs2, false);
			// we had added its token to credentials 
			NUnit.Framework.Assert.AreEqual(3, credentials.numberOfTokens());
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(myService));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithMyOwnExistsAndChildTokens()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service1 = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.io.Text service2 = new org.apache.hadoop.io.Text("singleTokenFs2"
				);
			org.apache.hadoop.io.Text myService = new org.apache.hadoop.io.Text("multiTokenFs"
				);
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			credentials.addToken(myService, token);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs1 = createFileSystemForServiceName
				(service1);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs2 = createFileSystemForServiceName
				(service2);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(myService, fs1, fs2);
			multiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(multiFs, false);
			// we had added its token to credentials
			verifyTokenFetch(fs1, true);
			verifyTokenFetch(fs2, true);
			NUnit.Framework.Assert.AreEqual(3, credentials.numberOfTokens());
			NUnit.Framework.Assert.AreSame(token, credentials.getToken(myService));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsWithNestedDuplicatesChildren()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.io.Text service1 = new org.apache.hadoop.io.Text("singleTokenFs1"
				);
			org.apache.hadoop.io.Text service2 = new org.apache.hadoop.io.Text("singleTokenFs2"
				);
			org.apache.hadoop.io.Text service4 = new org.apache.hadoop.io.Text("singleTokenFs4"
				);
			org.apache.hadoop.io.Text multiService = new org.apache.hadoop.io.Text("multiTokenFs"
				);
			org.apache.hadoop.security.token.Token<object> token2 = org.mockito.Mockito.mock<
				org.apache.hadoop.security.token.Token>();
			credentials.addToken(service2, token2);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs1 = createFileSystemForServiceName
				(service1);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs1B = createFileSystemForServiceName
				(service1);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs2 = createFileSystemForServiceName
				(service2);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs3 = createFileSystemForServiceName
				(null);
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs4 = createFileSystemForServiceName
				(service4);
			// now let's get dirty!  ensure dup tokens aren't fetched even when
			// repeated and dupped in a nested fs.  fs4 is a real test of the drill
			// down: multi-filter-multi-filter-filter-fs4.
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem multiFs = createFileSystemForServiceName
				(multiService, fs1, fs1B, fs2, fs2, new org.apache.hadoop.fs.FilterFileSystem(fs3
				), new org.apache.hadoop.fs.FilterFileSystem(new org.apache.hadoop.fs.FilterFileSystem
				(fs4)));
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem superMultiFs = createFileSystemForServiceName
				(null, fs1, fs1B, fs1, new org.apache.hadoop.fs.FilterFileSystem(fs3), new org.apache.hadoop.fs.FilterFileSystem
				(multiFs));
			superMultiFs.addDelegationTokens(renewer, credentials);
			verifyTokenFetch(superMultiFs, false);
			// does not have its own token
			verifyTokenFetch(multiFs, true);
			// has its own token
			verifyTokenFetch(fs1, true);
			verifyTokenFetch(fs2, false);
			// we had added its token to credentials
			verifyTokenFetch(fs3, false);
			// has no tokens
			verifyTokenFetch(fs4, true);
			NUnit.Framework.Assert.AreEqual(4, credentials.numberOfTokens());
			//fs1+fs2+fs4+multifs (fs3=0)
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service1));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service2));
			NUnit.Framework.Assert.AreSame(token2, credentials.getToken(service2));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(multiService));
			NUnit.Framework.Assert.IsNotNull(credentials.getToken(service4));
		}

		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem createFileSystemForServiceName
			(org.apache.hadoop.io.Text service, params org.apache.hadoop.fs.FileSystem[] children
			)
		{
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem fs = new org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
				();
			org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem mockFs = ((org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
				)fs.getRawFileSystem());
			if (service != null)
			{
				org.mockito.Mockito.when(mockFs.getCanonicalServiceName()).thenReturn(service.ToString
					());
				org.mockito.Mockito.when(mockFs.getDelegationToken(org.mockito.Matchers.any<string
					>())).thenAnswer(new _Answer_255(service));
			}
			org.mockito.Mockito.when(mockFs.getChildFileSystems()).thenReturn(children);
			return fs;
		}

		private sealed class _Answer_255 : org.mockito.stubbing.Answer<org.apache.hadoop.security.token.Token
			<object>>
		{
			public _Answer_255(org.apache.hadoop.io.Text service)
			{
				this.service = service;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.security.token.Token<object> answer(org.mockito.invocation.InvocationOnMock
				 invocation)
			{
				org.apache.hadoop.security.token.Token<object> token = new org.apache.hadoop.security.token.Token
					<org.apache.hadoop.security.token.TokenIdentifier>();
				token.setService(service);
				return token;
			}

			private readonly org.apache.hadoop.io.Text service;
		}

		// check that canonical name was requested, if renewer is not null that
		// a token was requested, and that child fs was invoked
		/// <exception cref="System.IO.IOException"/>
		private void verifyTokenFetch(org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
			 fs, bool expected)
		{
			org.mockito.Mockito.verify(((org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
				)fs.getRawFileSystem()), org.mockito.Mockito.atLeast(1)).getCanonicalServiceName
				();
			if (expected)
			{
				org.mockito.Mockito.verify(((org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
					)fs.getRawFileSystem())).getDelegationToken(renewer);
			}
			else
			{
				org.mockito.Mockito.verify(((org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
					)fs.getRawFileSystem()), org.mockito.Mockito.never()).getDelegationToken(org.mockito.Matchers.any
					<string>());
			}
			org.mockito.Mockito.verify(((org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
				)fs.getRawFileSystem()), org.mockito.Mockito.atLeast(1)).getChildFileSystems();
		}
	}
}
