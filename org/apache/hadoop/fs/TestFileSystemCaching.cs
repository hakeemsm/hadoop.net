using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileSystemCaching
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCacheEnabled()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.cachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			org.apache.hadoop.fs.FileSystem fs1 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI
				("cachedfile://a"), conf);
			org.apache.hadoop.fs.FileSystem fs2 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI
				("cachedfile://a"), conf);
			NUnit.Framework.Assert.AreSame(fs1, fs2);
		}

		internal class DefaultFs : org.apache.hadoop.fs.LocalFileSystem
		{
			internal java.net.URI uri;

			public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.uri = uri;
			}

			public override java.net.URI getUri()
			{
				return uri;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDefaultFsUris()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.defaultfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFileSystemCaching.DefaultFs
				)).getName());
			java.net.URI defaultUri = java.net.URI.create("defaultfs://host");
			org.apache.hadoop.fs.FileSystem.setDefaultUri(conf, defaultUri);
			org.apache.hadoop.fs.FileSystem fs = null;
			// sanity check default fs
			org.apache.hadoop.fs.FileSystem defaultFs = org.apache.hadoop.fs.FileSystem.get(conf
				);
			NUnit.Framework.Assert.AreEqual(defaultUri, defaultFs.getUri());
			// has scheme, no auth
			fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("defaultfs:/"), conf
				);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("defaultfs:///"), conf
				);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			// has scheme, same auth
			fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("defaultfs://host"), 
				conf);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			// has scheme, different auth
			fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("defaultfs://host2")
				, conf);
			NUnit.Framework.Assert.AreNotSame(defaultFs, fs);
			// no scheme, no auth
			fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("/"), conf);
			NUnit.Framework.Assert.AreSame(defaultFs, fs);
			// no scheme, same auth
			try
			{
				fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("//host"), conf);
				NUnit.Framework.Assert.Fail("got fs with auth but no scheme");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.AreEqual("No FileSystem for scheme: null", e.Message);
			}
			// no scheme, different auth
			try
			{
				fs = org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("//host2"), conf);
				NUnit.Framework.Assert.Fail("got fs with auth but no scheme");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.AreEqual("No FileSystem for scheme: null", e.Message);
			}
		}

		public class InitializeForeverFileSystem : org.apache.hadoop.fs.LocalFileSystem
		{
			internal static readonly java.util.concurrent.Semaphore sem = new java.util.concurrent.Semaphore
				(0);

			/// <exception cref="System.IO.IOException"/>
			public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				// notify that InitializeForeverFileSystem started initialization
				sem.release();
				try
				{
					while (true)
					{
						java.lang.Thread.sleep(1000);
					}
				}
				catch (System.Exception)
				{
					return;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCacheEnabledWithInitializeForeverFS()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			java.lang.Thread t = new _Thread_130(conf);
			t.start();
			// wait for InitializeForeverFileSystem to start initialization
			org.apache.hadoop.fs.TestFileSystemCaching.InitializeForeverFileSystem.sem.acquire
				();
			conf.set("fs.cachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf);
			t.interrupt();
			t.join();
		}

		private sealed class _Thread_130 : java.lang.Thread
		{
			public _Thread_130(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			public override void run()
			{
				conf.set("fs.localfs1.impl", "org.apache.hadoop.fs." + "TestFileSystemCaching$InitializeForeverFileSystem"
					);
				try
				{
					org.apache.hadoop.fs.FileSystem.get(new java.net.URI("localfs1://a"), conf);
				}
				catch (System.IO.IOException e)
				{
					Sharpen.Runtime.printStackTrace(e);
				}
				catch (java.net.URISyntaxException e)
				{
					Sharpen.Runtime.printStackTrace(e);
				}
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCacheDisabled()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.uncachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			conf.setBoolean("fs.uncachedfile.impl.disable.cache", true);
			org.apache.hadoop.fs.FileSystem fs1 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI
				("uncachedfile://a"), conf);
			org.apache.hadoop.fs.FileSystem fs2 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI
				("uncachedfile://a"), conf);
			NUnit.Framework.Assert.AreNotSame(fs1, fs2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCacheForUgi<T>()
			where T : org.apache.hadoop.security.token.TokenIdentifier
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.cachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			org.apache.hadoop.security.UserGroupInformation ugiA = org.apache.hadoop.security.UserGroupInformation
				.createRemoteUser("foo");
			org.apache.hadoop.security.UserGroupInformation ugiB = org.apache.hadoop.security.UserGroupInformation
				.createRemoteUser("bar");
			org.apache.hadoop.fs.FileSystem fsA = ugiA.doAs(new _PrivilegedExceptionAction_171
				(conf));
			org.apache.hadoop.fs.FileSystem fsA1 = ugiA.doAs(new _PrivilegedExceptionAction_177
				(conf));
			//Since the UGIs are the same, we should have the same filesystem for both
			NUnit.Framework.Assert.AreSame(fsA, fsA1);
			org.apache.hadoop.fs.FileSystem fsB = ugiB.doAs(new _PrivilegedExceptionAction_186
				(conf));
			//Since the UGIs are different, we should end up with different filesystems
			//corresponding to the two UGIs
			NUnit.Framework.Assert.AreNotSame(fsA, fsB);
			org.apache.hadoop.security.token.Token<T> t1 = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			org.apache.hadoop.security.UserGroupInformation ugiA2 = org.apache.hadoop.security.UserGroupInformation
				.createRemoteUser("foo");
			fsA = ugiA2.doAs(new _PrivilegedExceptionAction_199(conf));
			// Although the users in the UGI are same, they have different subjects
			// and so are different.
			NUnit.Framework.Assert.AreNotSame(fsA, fsA1);
			ugiA.addToken(t1);
			fsA = ugiA.doAs(new _PrivilegedExceptionAction_211(conf));
			// Make sure that different UGI's with the same subject lead to the same
			// file system.
			NUnit.Framework.Assert.AreSame(fsA, fsA1);
		}

		private sealed class _PrivilegedExceptionAction_171 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_171(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_177 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_177(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_186 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_186(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_199 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_199(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_211 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_211(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUserFS()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.cachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			org.apache.hadoop.fs.FileSystem fsU1 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI
				("cachedfile://a"), conf, "bar");
			org.apache.hadoop.fs.FileSystem fsU2 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI
				("cachedfile://a"), conf, "foo");
			NUnit.Framework.Assert.AreNotSame(fsU1, fsU2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsUniqueness()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.cachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			// multiple invocations of FileSystem.get return the same object.
			org.apache.hadoop.fs.FileSystem fs1 = org.apache.hadoop.fs.FileSystem.get(conf);
			org.apache.hadoop.fs.FileSystem fs2 = org.apache.hadoop.fs.FileSystem.get(conf);
			NUnit.Framework.Assert.IsTrue(fs1 == fs2);
			// multiple invocations of FileSystem.newInstance return different objects
			fs1 = org.apache.hadoop.fs.FileSystem.newInstance(new java.net.URI("cachedfile://a"
				), conf, "bar");
			fs2 = org.apache.hadoop.fs.FileSystem.newInstance(new java.net.URI("cachedfile://a"
				), conf, "bar");
			NUnit.Framework.Assert.IsTrue(fs1 != fs2 && !fs1.Equals(fs2));
			fs1.close();
			fs2.close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCloseAllForUGI()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.cachedfile.impl", org.apache.hadoop.fs.FileSystem.getFileSystemClass
				("file", null).getName());
			org.apache.hadoop.security.UserGroupInformation ugiA = org.apache.hadoop.security.UserGroupInformation
				.createRemoteUser("foo");
			org.apache.hadoop.fs.FileSystem fsA = ugiA.doAs(new _PrivilegedExceptionAction_254
				(conf));
			//Now we should get the cached filesystem
			org.apache.hadoop.fs.FileSystem fsA1 = ugiA.doAs(new _PrivilegedExceptionAction_261
				(conf));
			NUnit.Framework.Assert.AreSame(fsA, fsA1);
			org.apache.hadoop.fs.FileSystem.closeAllForUGI(ugiA);
			//Now we should get a different (newly created) filesystem
			fsA1 = ugiA.doAs(new _PrivilegedExceptionAction_272(conf));
			NUnit.Framework.Assert.AreNotSame(fsA, fsA1);
		}

		private sealed class _PrivilegedExceptionAction_254 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_254(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_261 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_261(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_272 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.fs.FileSystem>
		{
			public _PrivilegedExceptionAction_272(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.fs.FileSystem run()
			{
				return org.apache.hadoop.fs.FileSystem.get(new java.net.URI("cachedfile://a"), conf
					);
			}

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDelete()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/a");
			fs.delete(path, false);
			org.mockito.Mockito.verify(mockFs).delete(eq(path), eq(false));
			org.mockito.Mockito.reset(mockFs);
			fs.delete(path, true);
			org.mockito.Mockito.verify(mockFs).delete(eq(path), eq(true));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteOnExit()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/a");
			// delete on close if path does exist
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(new org.apache.hadoop.fs.FileStatus
				());
			NUnit.Framework.Assert.IsTrue(fs.deleteOnExit(path));
			org.mockito.Mockito.verify(mockFs).getFileStatus(eq(path));
			org.mockito.Mockito.reset(mockFs);
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(new org.apache.hadoop.fs.FileStatus
				());
			fs.close();
			org.mockito.Mockito.verify(mockFs).getFileStatus(eq(path));
			org.mockito.Mockito.verify(mockFs).delete(eq(path), eq(true));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteOnExitFNF()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/a");
			// don't delete on close if path doesn't exist
			NUnit.Framework.Assert.IsFalse(fs.deleteOnExit(path));
			org.mockito.Mockito.verify(mockFs).getFileStatus(eq(path));
			org.mockito.Mockito.reset(mockFs);
			fs.close();
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).getFileStatus(eq(
				path));
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(any<org.apache.hadoop.fs.Path
				>(), anyBoolean());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteOnExitRemoved()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/a");
			// don't delete on close if path existed, but later removed
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(new org.apache.hadoop.fs.FileStatus
				());
			NUnit.Framework.Assert.IsTrue(fs.deleteOnExit(path));
			org.mockito.Mockito.verify(mockFs).getFileStatus(eq(path));
			org.mockito.Mockito.reset(mockFs);
			fs.close();
			org.mockito.Mockito.verify(mockFs).getFileStatus(eq(path));
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(any<org.apache.hadoop.fs.Path
				>(), anyBoolean());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCancelDeleteOnExit()
		{
			org.apache.hadoop.fs.FileSystem mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.FilterFileSystem(mockFs
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/a");
			// don't delete on close if path existed, but later cancelled
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(new org.apache.hadoop.fs.FileStatus
				());
			NUnit.Framework.Assert.IsTrue(fs.deleteOnExit(path));
			org.mockito.Mockito.verify(mockFs).getFileStatus(eq(path));
			NUnit.Framework.Assert.IsTrue(fs.cancelDeleteOnExit(path));
			NUnit.Framework.Assert.IsFalse(fs.cancelDeleteOnExit(path));
			// false because not registered
			org.mockito.Mockito.reset(mockFs);
			fs.close();
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).getFileStatus(any
				<org.apache.hadoop.fs.Path>());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(any<org.apache.hadoop.fs.Path
				>(), anyBoolean());
		}
	}
}
