using System;
using System.IO;
using System.Threading;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFileSystemCaching
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCacheEnabled()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.cachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			FileSystem fs1 = FileSystem.Get(new URI("cachedfile://a"), conf);
			FileSystem fs2 = FileSystem.Get(new URI("cachedfile://a"), conf);
			NUnit.Framework.Assert.AreSame(fs1, fs2);
		}

		internal class DefaultFs : LocalFileSystem
		{
			internal URI uri;

			public override void Initialize(URI uri, Configuration conf)
			{
				this.uri = uri;
			}

			public override URI GetUri()
			{
				return uri;
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDefaultFsUris()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.defaultfs.impl", typeof(TestFileSystemCaching.DefaultFs).FullName);
			URI defaultUri = URI.Create("defaultfs://host");
			FileSystem.SetDefaultUri(conf, defaultUri);
			FileSystem fs = null;
			// sanity check default fs
			FileSystem defaultFs = FileSystem.Get(conf);
			Assert.Equal(defaultUri, defaultFs.GetUri());
			// has scheme, no auth
			fs = FileSystem.Get(URI.Create("defaultfs:/"), conf);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			fs = FileSystem.Get(URI.Create("defaultfs:///"), conf);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			// has scheme, same auth
			fs = FileSystem.Get(URI.Create("defaultfs://host"), conf);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			// has scheme, different auth
			fs = FileSystem.Get(URI.Create("defaultfs://host2"), conf);
			NUnit.Framework.Assert.AreNotSame(defaultFs, fs);
			// no scheme, no auth
			fs = FileSystem.Get(URI.Create("/"), conf);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			// no scheme, same auth
			try
			{
				fs = FileSystem.Get(URI.Create("//host"), conf);
				NUnit.Framework.Assert.Fail("got fs with auth but no scheme");
			}
			catch (Exception e)
			{
				Assert.Equal("No FileSystem for scheme: null", e.Message);
			}
			// no scheme, different auth
			try
			{
				fs = FileSystem.Get(URI.Create("//host2"), conf);
				NUnit.Framework.Assert.Fail("got fs with auth but no scheme");
			}
			catch (Exception e)
			{
				Assert.Equal("No FileSystem for scheme: null", e.Message);
			}
		}

		public class InitializeForeverFileSystem : LocalFileSystem
		{
			internal static readonly Semaphore sem = Sharpen.Extensions.CreateSemaphore(0);

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI uri, Configuration conf)
			{
				// notify that InitializeForeverFileSystem started initialization
				sem.Release();
				try
				{
					while (true)
					{
						Sharpen.Thread.Sleep(1000);
					}
				}
				catch (Exception)
				{
					return;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCacheEnabledWithInitializeForeverFS()
		{
			Configuration conf = new Configuration();
			Sharpen.Thread t = new _Thread_130(conf);
			t.Start();
			// wait for InitializeForeverFileSystem to start initialization
			TestFileSystemCaching.InitializeForeverFileSystem.sem.WaitOne();
			conf.Set("fs.cachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			FileSystem.Get(new URI("cachedfile://a"), conf);
			t.Interrupt();
			t.Join();
		}

		private sealed class _Thread_130 : Sharpen.Thread
		{
			public _Thread_130(Configuration conf)
			{
				this.conf = conf;
			}

			public override void Run()
			{
				conf.Set("fs.localfs1.impl", "org.apache.hadoop.fs." + "TestFileSystemCaching$InitializeForeverFileSystem"
					);
				try
				{
					FileSystem.Get(new URI("localfs1://a"), conf);
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				catch (URISyntaxException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCacheDisabled()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.uncachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			conf.SetBoolean("fs.uncachedfile.impl.disable.cache", true);
			FileSystem fs1 = FileSystem.Get(new URI("uncachedfile://a"), conf);
			FileSystem fs2 = FileSystem.Get(new URI("uncachedfile://a"), conf);
			NUnit.Framework.Assert.AreNotSame(fs1, fs2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCacheForUgi<T>()
			where T : TokenIdentifier
		{
			Configuration conf = new Configuration();
			conf.Set("fs.cachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			UserGroupInformation ugiA = UserGroupInformation.CreateRemoteUser("foo");
			UserGroupInformation ugiB = UserGroupInformation.CreateRemoteUser("bar");
			FileSystem fsA = ugiA.DoAs(new _PrivilegedExceptionAction_171(conf));
			FileSystem fsA1 = ugiA.DoAs(new _PrivilegedExceptionAction_177(conf));
			//Since the UGIs are the same, we should have the same filesystem for both
			NUnit.Framework.Assert.AreSame(fsA, fsA1);
			FileSystem fsB = ugiB.DoAs(new _PrivilegedExceptionAction_186(conf));
			//Since the UGIs are different, we should end up with different filesystems
			//corresponding to the two UGIs
			NUnit.Framework.Assert.AreNotSame(fsA, fsB);
			Org.Apache.Hadoop.Security.Token.Token<T> t1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			UserGroupInformation ugiA2 = UserGroupInformation.CreateRemoteUser("foo");
			fsA = ugiA2.DoAs(new _PrivilegedExceptionAction_199(conf));
			// Although the users in the UGI are same, they have different subjects
			// and so are different.
			NUnit.Framework.Assert.AreNotSame(fsA, fsA1);
			ugiA.AddToken(t1);
			fsA = ugiA.DoAs(new _PrivilegedExceptionAction_211(conf));
			// Make sure that different UGI's with the same subject lead to the same
			// file system.
			NUnit.Framework.Assert.AreSame(fsA, fsA1);
		}

		private sealed class _PrivilegedExceptionAction_171 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_171(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_177 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_177(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_186 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_186(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_199 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_199(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_211 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_211(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUserFS()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.cachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			FileSystem fsU1 = FileSystem.Get(new URI("cachedfile://a"), conf, "bar");
			FileSystem fsU2 = FileSystem.Get(new URI("cachedfile://a"), conf, "foo");
			NUnit.Framework.Assert.AreNotSame(fsU1, fsU2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFsUniqueness()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.cachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			// multiple invocations of FileSystem.get return the same object.
			FileSystem fs1 = FileSystem.Get(conf);
			FileSystem fs2 = FileSystem.Get(conf);
			Assert.True(fs1 == fs2);
			// multiple invocations of FileSystem.newInstance return different objects
			fs1 = FileSystem.NewInstance(new URI("cachedfile://a"), conf, "bar");
			fs2 = FileSystem.NewInstance(new URI("cachedfile://a"), conf, "bar");
			Assert.True(fs1 != fs2 && !fs1.Equals(fs2));
			fs1.Close();
			fs2.Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCloseAllForUGI()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.cachedfile.impl", FileSystem.GetFileSystemClass("file", null).FullName
				);
			UserGroupInformation ugiA = UserGroupInformation.CreateRemoteUser("foo");
			FileSystem fsA = ugiA.DoAs(new _PrivilegedExceptionAction_254(conf));
			//Now we should get the cached filesystem
			FileSystem fsA1 = ugiA.DoAs(new _PrivilegedExceptionAction_261(conf));
			NUnit.Framework.Assert.AreSame(fsA, fsA1);
			FileSystem.CloseAllForUGI(ugiA);
			//Now we should get a different (newly created) filesystem
			fsA1 = ugiA.DoAs(new _PrivilegedExceptionAction_272(conf));
			NUnit.Framework.Assert.AreNotSame(fsA, fsA1);
		}

		private sealed class _PrivilegedExceptionAction_254 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_254(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_261 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_261(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_272 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_272(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return FileSystem.Get(new URI("cachedfile://a"), conf);
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDelete()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			Path path = new Path("/a");
			fs.Delete(path, false);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(path), Eq(false));
			Org.Mockito.Mockito.Reset(mockFs);
			fs.Delete(path, true);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(path), Eq(true));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteOnExit()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			Path path = new Path("/a");
			// delete on close if path does exist
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(new FileStatus
				());
			Assert.True(fs.DeleteOnExit(path));
			Org.Mockito.Mockito.Verify(mockFs).GetFileStatus(Eq(path));
			Org.Mockito.Mockito.Reset(mockFs);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(new FileStatus
				());
			fs.Close();
			Org.Mockito.Mockito.Verify(mockFs).GetFileStatus(Eq(path));
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(path), Eq(true));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteOnExitFNF()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			Path path = new Path("/a");
			// don't delete on close if path doesn't exist
			NUnit.Framework.Assert.IsFalse(fs.DeleteOnExit(path));
			Org.Mockito.Mockito.Verify(mockFs).GetFileStatus(Eq(path));
			Org.Mockito.Mockito.Reset(mockFs);
			fs.Close();
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).GetFileStatus(Eq(
				path));
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Any<Path>(
				), AnyBoolean());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteOnExitRemoved()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			Path path = new Path("/a");
			// don't delete on close if path existed, but later removed
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(new FileStatus
				());
			Assert.True(fs.DeleteOnExit(path));
			Org.Mockito.Mockito.Verify(mockFs).GetFileStatus(Eq(path));
			Org.Mockito.Mockito.Reset(mockFs);
			fs.Close();
			Org.Mockito.Mockito.Verify(mockFs).GetFileStatus(Eq(path));
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Any<Path>(
				), AnyBoolean());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCancelDeleteOnExit()
		{
			FileSystem mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem fs = new FilterFileSystem(mockFs);
			Path path = new Path("/a");
			// don't delete on close if path existed, but later cancelled
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(new FileStatus
				());
			Assert.True(fs.DeleteOnExit(path));
			Org.Mockito.Mockito.Verify(mockFs).GetFileStatus(Eq(path));
			Assert.True(fs.CancelDeleteOnExit(path));
			NUnit.Framework.Assert.IsFalse(fs.CancelDeleteOnExit(path));
			// false because not registered
			Org.Mockito.Mockito.Reset(mockFs);
			fs.Close();
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).GetFileStatus(Any
				<Path>());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Any<Path>(
				), AnyBoolean());
		}
	}
}
