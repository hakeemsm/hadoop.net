using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Http.Server;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Mortbay.Jetty.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	public abstract class BaseTestHttpFSWith : HFSTestCase
	{
		protected internal abstract Path GetProxiedFSTestDir();

		protected internal abstract string GetProxiedFSURI();

		protected internal abstract Configuration GetProxiedFSConf();

		protected internal virtual bool IsLocalFS()
		{
			return GetProxiedFSURI().StartsWith("file://");
		}

		/// <exception cref="System.Exception"/>
		private void CreateHttpFSServer()
		{
			FilePath homeDir = TestDirHelper.GetTestDir();
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			HttpFSServerWebApp.SetHomeDirForCurrentThread(homeDir.GetAbsolutePath());
			FilePath secretFile = new FilePath(new FilePath(homeDir, "conf"), "secret");
			TextWriter w = new FileWriter(secretFile);
			w.Write("secret");
			w.Close();
			//FileSystem being served by HttpFS
			string fsDefaultName = GetProxiedFSURI();
			Configuration conf = new Configuration(false);
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, fsDefaultName);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			FilePath hdfsSite = new FilePath(new FilePath(homeDir, "conf"), "hdfs-site.xml");
			OutputStream os = new FileOutputStream(hdfsSite);
			conf.WriteXml(os);
			os.Close();
			//HTTPFS configuration
			conf = new Configuration(false);
			conf.Set("httpfs.proxyuser." + HadoopUsersConfTestHelper.GetHadoopProxyUser() + ".groups"
				, HadoopUsersConfTestHelper.GetHadoopProxyUserGroups());
			conf.Set("httpfs.proxyuser." + HadoopUsersConfTestHelper.GetHadoopProxyUser() + ".hosts"
				, HadoopUsersConfTestHelper.GetHadoopProxyUserHosts());
			conf.Set("httpfs.authentication.signature.secret.file", secretFile.GetAbsolutePath
				());
			FilePath httpfsSite = new FilePath(new FilePath(homeDir, "conf"), "httpfs-site.xml"
				);
			os = new FileOutputStream(httpfsSite);
			conf.WriteXml(os);
			os.Close();
			ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
			Uri url = cl.GetResource("webapp");
			WebAppContext context = new WebAppContext(url.AbsolutePath, "/webhdfs");
			Org.Mortbay.Jetty.Server server = TestJettyHelper.GetJettyServer();
			server.AddHandler(context);
			server.Start();
		}

		protected internal virtual Type GetFileSystemClass()
		{
			return typeof(HttpFSFileSystem);
		}

		protected internal virtual string GetScheme()
		{
			return "webhdfs";
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual FileSystem GetHttpFSFileSystem()
		{
			Configuration conf = new Configuration();
			conf.Set("fs.webhdfs.impl", GetFileSystemClass().FullName);
			URI uri = new URI(GetScheme() + "://" + TestJettyHelper.GetJettyURL().ToURI().GetAuthority
				());
			return FileSystem.Get(uri, conf);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void TestGet()
		{
			FileSystem fs = GetHttpFSFileSystem();
			NUnit.Framework.Assert.IsNotNull(fs);
			URI uri = new URI(GetScheme() + "://" + TestJettyHelper.GetJettyURL().ToURI().GetAuthority
				());
			NUnit.Framework.Assert.AreEqual(fs.GetUri(), uri);
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestOpen()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
			OutputStream os = fs.Create(path);
			os.Write(1);
			os.Close();
			fs.Close();
			fs = GetHttpFSFileSystem();
			InputStream @is = fs.Open(new Path(path.ToUri().GetPath()));
			NUnit.Framework.Assert.AreEqual(@is.Read(), 1);
			@is.Close();
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestCreate(Path path, bool @override)
		{
			FileSystem fs = GetHttpFSFileSystem();
			FsPermission permission = new FsPermission(FsAction.ReadWrite, FsAction.None, FsAction
				.None);
			OutputStream os = fs.Create(new Path(path.ToUri().GetPath()), permission, @override
				, 1024, (short)2, 100 * 1024 * 1024, null);
			os.Write(1);
			os.Close();
			fs.Close();
			fs = FileSystem.Get(GetProxiedFSConf());
			FileStatus status = fs.GetFileStatus(path);
			if (!IsLocalFS())
			{
				NUnit.Framework.Assert.AreEqual(status.GetReplication(), 2);
				NUnit.Framework.Assert.AreEqual(status.GetBlockSize(), 100 * 1024 * 1024);
			}
			NUnit.Framework.Assert.AreEqual(status.GetPermission(), permission);
			InputStream @is = fs.Open(path);
			NUnit.Framework.Assert.AreEqual(@is.Read(), 1);
			@is.Close();
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestCreate()
		{
			Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			fs.Delete(path, true);
			TestCreate(path, false);
			TestCreate(path, true);
			try
			{
				TestCreate(path, false);
				NUnit.Framework.Assert.Fail("the create should have failed because the file exists "
					 + "and override is FALSE");
			}
			catch (IOException)
			{
				System.Console.Out.WriteLine("#");
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail(ex.ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestAppend()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				fs.Close();
				fs = GetHttpFSFileSystem();
				os = fs.Append(new Path(path.ToUri().GetPath()));
				os.Write(2);
				os.Close();
				fs.Close();
				fs = FileSystem.Get(GetProxiedFSConf());
				InputStream @is = fs.Open(path);
				NUnit.Framework.Assert.AreEqual(@is.Read(), 1);
				NUnit.Framework.Assert.AreEqual(@is.Read(), 2);
				NUnit.Framework.Assert.AreEqual(@is.Read(), -1);
				@is.Close();
				fs.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestTruncate()
		{
			if (!IsLocalFS())
			{
				short repl = 3;
				int blockSize = 1024;
				int numOfBlocks = 2;
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path file = new Path(GetProxiedFSTestDir(), "foo.txt");
				byte[] data = FileSystemTestHelper.GetFileData(numOfBlocks, blockSize);
				FileSystemTestHelper.CreateFile(fs, file, data, blockSize, repl);
				int newLength = blockSize;
				bool isReady = fs.Truncate(file, newLength);
				NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
				FileStatus fileStatus = fs.GetFileStatus(file);
				NUnit.Framework.Assert.AreEqual(fileStatus.GetLen(), newLength);
				AppendTestUtil.CheckFullFile(fs, file, newLength, data, file.ToString());
				fs.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestConcat()
		{
			Configuration config = GetProxiedFSConf();
			config.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024);
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(config);
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path1 = new Path("/test/foo.txt");
				Path path2 = new Path("/test/bar.txt");
				Path path3 = new Path("/test/derp.txt");
				DFSTestUtil.CreateFile(fs, path1, 1024, (short)3, 0);
				DFSTestUtil.CreateFile(fs, path2, 1024, (short)3, 0);
				DFSTestUtil.CreateFile(fs, path3, 1024, (short)3, 0);
				fs.Close();
				fs = GetHttpFSFileSystem();
				fs.Concat(path1, new Path[] { path2, path3 });
				fs.Close();
				fs = FileSystem.Get(config);
				NUnit.Framework.Assert.IsTrue(fs.Exists(path1));
				NUnit.Framework.Assert.IsFalse(fs.Exists(path2));
				NUnit.Framework.Assert.IsFalse(fs.Exists(path3));
				fs.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestRename()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path path = new Path(GetProxiedFSTestDir(), "foo");
			fs.Mkdirs(path);
			fs.Close();
			fs = GetHttpFSFileSystem();
			Path oldPath = new Path(path.ToUri().GetPath());
			Path newPath = new Path(path.GetParent(), "bar");
			fs.Rename(oldPath, newPath);
			fs.Close();
			fs = FileSystem.Get(GetProxiedFSConf());
			NUnit.Framework.Assert.IsFalse(fs.Exists(oldPath));
			NUnit.Framework.Assert.IsTrue(fs.Exists(newPath));
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestDelete()
		{
			Path foo = new Path(GetProxiedFSTestDir(), "foo");
			Path bar = new Path(GetProxiedFSTestDir(), "bar");
			Path foe = new Path(GetProxiedFSTestDir(), "foe");
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			fs.Mkdirs(foo);
			fs.Mkdirs(new Path(bar, "a"));
			fs.Mkdirs(foe);
			FileSystem hoopFs = GetHttpFSFileSystem();
			NUnit.Framework.Assert.IsTrue(hoopFs.Delete(new Path(foo.ToUri().GetPath()), false
				));
			NUnit.Framework.Assert.IsFalse(fs.Exists(foo));
			try
			{
				hoopFs.Delete(new Path(bar.ToUri().GetPath()), false);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			NUnit.Framework.Assert.IsTrue(fs.Exists(bar));
			NUnit.Framework.Assert.IsTrue(hoopFs.Delete(new Path(bar.ToUri().GetPath()), true
				));
			NUnit.Framework.Assert.IsFalse(fs.Exists(bar));
			NUnit.Framework.Assert.IsTrue(fs.Exists(foe));
			NUnit.Framework.Assert.IsTrue(hoopFs.Delete(foe, true));
			NUnit.Framework.Assert.IsFalse(fs.Exists(foe));
			hoopFs.Close();
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestListStatus()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
			OutputStream os = fs.Create(path);
			os.Write(1);
			os.Close();
			FileStatus status1 = fs.GetFileStatus(path);
			fs.Close();
			fs = GetHttpFSFileSystem();
			FileStatus status2 = fs.GetFileStatus(new Path(path.ToUri().GetPath()));
			fs.Close();
			NUnit.Framework.Assert.AreEqual(status2.GetPermission(), status1.GetPermission());
			NUnit.Framework.Assert.AreEqual(status2.GetPath().ToUri().GetPath(), status1.GetPath
				().ToUri().GetPath());
			NUnit.Framework.Assert.AreEqual(status2.GetReplication(), status1.GetReplication(
				));
			NUnit.Framework.Assert.AreEqual(status2.GetBlockSize(), status1.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(status2.GetAccessTime(), status1.GetAccessTime());
			NUnit.Framework.Assert.AreEqual(status2.GetModificationTime(), status1.GetModificationTime
				());
			NUnit.Framework.Assert.AreEqual(status2.GetOwner(), status1.GetOwner());
			NUnit.Framework.Assert.AreEqual(status2.GetGroup(), status1.GetGroup());
			NUnit.Framework.Assert.AreEqual(status2.GetLen(), status1.GetLen());
			FileStatus[] stati = fs.ListStatus(path.GetParent());
			NUnit.Framework.Assert.AreEqual(stati.Length, 1);
			NUnit.Framework.Assert.AreEqual(stati[0].GetPath().GetName(), path.GetName());
		}

		/// <exception cref="System.Exception"/>
		private void TestWorkingdirectory()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path workingDir = fs.GetWorkingDirectory();
			fs.Close();
			fs = GetHttpFSFileSystem();
			if (IsLocalFS())
			{
				fs.SetWorkingDirectory(workingDir);
			}
			Path httpFSWorkingDir = fs.GetWorkingDirectory();
			fs.Close();
			NUnit.Framework.Assert.AreEqual(httpFSWorkingDir.ToUri().GetPath(), workingDir.ToUri
				().GetPath());
			fs = GetHttpFSFileSystem();
			fs.SetWorkingDirectory(new Path("/tmp"));
			workingDir = fs.GetWorkingDirectory();
			fs.Close();
			NUnit.Framework.Assert.AreEqual(workingDir.ToUri().GetPath(), new Path("/tmp").ToUri
				().GetPath());
		}

		/// <exception cref="System.Exception"/>
		private void TestMkdirs()
		{
			Path path = new Path(GetProxiedFSTestDir(), "foo");
			FileSystem fs = GetHttpFSFileSystem();
			fs.Mkdirs(path);
			fs.Close();
			fs = FileSystem.Get(GetProxiedFSConf());
			NUnit.Framework.Assert.IsTrue(fs.Exists(path));
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestSetTimes()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				FileStatus status1 = fs.GetFileStatus(path);
				fs.Close();
				long at = status1.GetAccessTime();
				long mt = status1.GetModificationTime();
				fs = GetHttpFSFileSystem();
				fs.SetTimes(path, mt - 10, at - 20);
				fs.Close();
				fs = FileSystem.Get(GetProxiedFSConf());
				status1 = fs.GetFileStatus(path);
				fs.Close();
				long atNew = status1.GetAccessTime();
				long mtNew = status1.GetModificationTime();
				NUnit.Framework.Assert.AreEqual(mtNew, mt - 10);
				NUnit.Framework.Assert.AreEqual(atNew, at - 20);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void TestSetPermission()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path path = new Path(GetProxiedFSTestDir(), "foodir");
			fs.Mkdirs(path);
			fs = GetHttpFSFileSystem();
			FsPermission permission1 = new FsPermission(FsAction.ReadWrite, FsAction.None, FsAction
				.None);
			fs.SetPermission(path, permission1);
			fs.Close();
			fs = FileSystem.Get(GetProxiedFSConf());
			FileStatus status1 = fs.GetFileStatus(path);
			fs.Close();
			FsPermission permission2 = status1.GetPermission();
			NUnit.Framework.Assert.AreEqual(permission2, permission1);
			//sticky bit
			fs = GetHttpFSFileSystem();
			permission1 = new FsPermission(FsAction.ReadWrite, FsAction.None, FsAction.None, 
				true);
			fs.SetPermission(path, permission1);
			fs.Close();
			fs = FileSystem.Get(GetProxiedFSConf());
			status1 = fs.GetFileStatus(path);
			fs.Close();
			permission2 = status1.GetPermission();
			NUnit.Framework.Assert.IsTrue(permission2.GetStickyBit());
			NUnit.Framework.Assert.AreEqual(permission2, permission1);
		}

		/// <exception cref="System.Exception"/>
		private void TestSetOwner()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				fs.Close();
				fs = GetHttpFSFileSystem();
				string user = HadoopUsersConfTestHelper.GetHadoopUsers()[1];
				string group = HadoopUsersConfTestHelper.GetHadoopUserGroups(user)[0];
				fs.SetOwner(path, user, group);
				fs.Close();
				fs = FileSystem.Get(GetProxiedFSConf());
				FileStatus status1 = fs.GetFileStatus(path);
				fs.Close();
				NUnit.Framework.Assert.AreEqual(status1.GetOwner(), user);
				NUnit.Framework.Assert.AreEqual(status1.GetGroup(), group);
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestSetReplication()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
			OutputStream os = fs.Create(path);
			os.Write(1);
			os.Close();
			fs.Close();
			fs.SetReplication(path, (short)2);
			fs = GetHttpFSFileSystem();
			fs.SetReplication(path, (short)1);
			fs.Close();
			fs = FileSystem.Get(GetProxiedFSConf());
			FileStatus status1 = fs.GetFileStatus(path);
			fs.Close();
			NUnit.Framework.Assert.AreEqual(status1.GetReplication(), (short)1);
		}

		/// <exception cref="System.Exception"/>
		private void TestChecksum()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				FileChecksum hdfsChecksum = fs.GetFileChecksum(path);
				fs.Close();
				fs = GetHttpFSFileSystem();
				FileChecksum httpChecksum = fs.GetFileChecksum(path);
				fs.Close();
				NUnit.Framework.Assert.AreEqual(httpChecksum.GetAlgorithmName(), hdfsChecksum.GetAlgorithmName
					());
				NUnit.Framework.Assert.AreEqual(httpChecksum.GetLength(), hdfsChecksum.GetLength(
					));
				Assert.AssertArrayEquals(httpChecksum.GetBytes(), hdfsChecksum.GetBytes());
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestContentSummary()
		{
			FileSystem fs = FileSystem.Get(GetProxiedFSConf());
			Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
			OutputStream os = fs.Create(path);
			os.Write(1);
			os.Close();
			ContentSummary hdfsContentSummary = fs.GetContentSummary(path);
			fs.Close();
			fs = GetHttpFSFileSystem();
			ContentSummary httpContentSummary = fs.GetContentSummary(path);
			fs.Close();
			NUnit.Framework.Assert.AreEqual(httpContentSummary.GetDirectoryCount(), hdfsContentSummary
				.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual(httpContentSummary.GetFileCount(), hdfsContentSummary
				.GetFileCount());
			NUnit.Framework.Assert.AreEqual(httpContentSummary.GetLength(), hdfsContentSummary
				.GetLength());
			NUnit.Framework.Assert.AreEqual(httpContentSummary.GetQuota(), hdfsContentSummary
				.GetQuota());
			NUnit.Framework.Assert.AreEqual(httpContentSummary.GetSpaceConsumed(), hdfsContentSummary
				.GetSpaceConsumed());
			NUnit.Framework.Assert.AreEqual(httpContentSummary.GetSpaceQuota(), hdfsContentSummary
				.GetSpaceQuota());
		}

		/// <summary>Set xattr</summary>
		/// <exception cref="System.Exception"/>
		private void TestSetXAttr()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				fs.Close();
				string name1 = "user.a1";
				byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				string name2 = "user.a2";
				byte[] value2 = new byte[] { unchecked((int)(0x41)), unchecked((int)(0x42)), unchecked(
					(int)(0x43)) };
				string name3 = "user.a3";
				byte[] value3 = null;
				string name4 = "trusted.a1";
				byte[] value4 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				string name5 = "a1";
				fs = GetHttpFSFileSystem();
				fs.SetXAttr(path, name1, value1);
				fs.SetXAttr(path, name2, value2);
				fs.SetXAttr(path, name3, value3);
				fs.SetXAttr(path, name4, value4);
				try
				{
					fs.SetXAttr(path, name5, value1);
					NUnit.Framework.Assert.Fail("Set xAttr with incorrect name format should fail.");
				}
				catch (IOException)
				{
				}
				catch (ArgumentException)
				{
				}
				fs.Close();
				fs = FileSystem.Get(GetProxiedFSConf());
				IDictionary<string, byte[]> xAttrs = fs.GetXAttrs(path);
				fs.Close();
				NUnit.Framework.Assert.AreEqual(4, xAttrs.Count);
				Assert.AssertArrayEquals(value1, xAttrs[name1]);
				Assert.AssertArrayEquals(value2, xAttrs[name2]);
				Assert.AssertArrayEquals(new byte[0], xAttrs[name3]);
				Assert.AssertArrayEquals(value4, xAttrs[name4]);
			}
		}

		/// <summary>Get xattrs</summary>
		/// <exception cref="System.Exception"/>
		private void TestGetXAttrs()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				fs.Close();
				string name1 = "user.a1";
				byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				string name2 = "user.a2";
				byte[] value2 = new byte[] { unchecked((int)(0x41)), unchecked((int)(0x42)), unchecked(
					(int)(0x43)) };
				string name3 = "user.a3";
				byte[] value3 = null;
				string name4 = "trusted.a1";
				byte[] value4 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				fs = FileSystem.Get(GetProxiedFSConf());
				fs.SetXAttr(path, name1, value1);
				fs.SetXAttr(path, name2, value2);
				fs.SetXAttr(path, name3, value3);
				fs.SetXAttr(path, name4, value4);
				fs.Close();
				// Get xattrs with names parameter
				fs = GetHttpFSFileSystem();
				IList<string> names = Lists.NewArrayList();
				names.AddItem(name1);
				names.AddItem(name2);
				names.AddItem(name3);
				names.AddItem(name4);
				IDictionary<string, byte[]> xAttrs = fs.GetXAttrs(path, names);
				fs.Close();
				NUnit.Framework.Assert.AreEqual(4, xAttrs.Count);
				Assert.AssertArrayEquals(value1, xAttrs[name1]);
				Assert.AssertArrayEquals(value2, xAttrs[name2]);
				Assert.AssertArrayEquals(new byte[0], xAttrs[name3]);
				Assert.AssertArrayEquals(value4, xAttrs[name4]);
				// Get specific xattr
				fs = GetHttpFSFileSystem();
				byte[] value = fs.GetXAttr(path, name1);
				Assert.AssertArrayEquals(value1, value);
				string name5 = "a1";
				try
				{
					value = fs.GetXAttr(path, name5);
					NUnit.Framework.Assert.Fail("Get xAttr with incorrect name format should fail.");
				}
				catch (IOException)
				{
				}
				catch (ArgumentException)
				{
				}
				fs.Close();
				// Get all xattrs
				fs = GetHttpFSFileSystem();
				xAttrs = fs.GetXAttrs(path);
				fs.Close();
				NUnit.Framework.Assert.AreEqual(4, xAttrs.Count);
				Assert.AssertArrayEquals(value1, xAttrs[name1]);
				Assert.AssertArrayEquals(value2, xAttrs[name2]);
				Assert.AssertArrayEquals(new byte[0], xAttrs[name3]);
				Assert.AssertArrayEquals(value4, xAttrs[name4]);
			}
		}

		/// <summary>Remove xattr</summary>
		/// <exception cref="System.Exception"/>
		private void TestRemoveXAttr()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				fs.Close();
				string name1 = "user.a1";
				byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				string name2 = "user.a2";
				byte[] value2 = new byte[] { unchecked((int)(0x41)), unchecked((int)(0x42)), unchecked(
					(int)(0x43)) };
				string name3 = "user.a3";
				byte[] value3 = null;
				string name4 = "trusted.a1";
				byte[] value4 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				string name5 = "a1";
				fs = FileSystem.Get(GetProxiedFSConf());
				fs.SetXAttr(path, name1, value1);
				fs.SetXAttr(path, name2, value2);
				fs.SetXAttr(path, name3, value3);
				fs.SetXAttr(path, name4, value4);
				fs.Close();
				fs = GetHttpFSFileSystem();
				fs.RemoveXAttr(path, name1);
				fs.RemoveXAttr(path, name3);
				fs.RemoveXAttr(path, name4);
				try
				{
					fs.RemoveXAttr(path, name5);
					NUnit.Framework.Assert.Fail("Remove xAttr with incorrect name format should fail."
						);
				}
				catch (IOException)
				{
				}
				catch (ArgumentException)
				{
				}
				fs = FileSystem.Get(GetProxiedFSConf());
				IDictionary<string, byte[]> xAttrs = fs.GetXAttrs(path);
				fs.Close();
				NUnit.Framework.Assert.AreEqual(1, xAttrs.Count);
				Assert.AssertArrayEquals(value2, xAttrs[name2]);
			}
		}

		/// <summary>List xattrs</summary>
		/// <exception cref="System.Exception"/>
		private void TestListXAttrs()
		{
			if (!IsLocalFS())
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				fs.Mkdirs(GetProxiedFSTestDir());
				Path path = new Path(GetProxiedFSTestDir(), "foo.txt");
				OutputStream os = fs.Create(path);
				os.Write(1);
				os.Close();
				fs.Close();
				string name1 = "user.a1";
				byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				string name2 = "user.a2";
				byte[] value2 = new byte[] { unchecked((int)(0x41)), unchecked((int)(0x42)), unchecked(
					(int)(0x43)) };
				string name3 = "user.a3";
				byte[] value3 = null;
				string name4 = "trusted.a1";
				byte[] value4 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
					(int)(0x33)) };
				fs = FileSystem.Get(GetProxiedFSConf());
				fs.SetXAttr(path, name1, value1);
				fs.SetXAttr(path, name2, value2);
				fs.SetXAttr(path, name3, value3);
				fs.SetXAttr(path, name4, value4);
				fs.Close();
				fs = GetHttpFSFileSystem();
				IList<string> names = fs.ListXAttrs(path);
				NUnit.Framework.Assert.AreEqual(4, names.Count);
				NUnit.Framework.Assert.IsTrue(names.Contains(name1));
				NUnit.Framework.Assert.IsTrue(names.Contains(name2));
				NUnit.Framework.Assert.IsTrue(names.Contains(name3));
				NUnit.Framework.Assert.IsTrue(names.Contains(name4));
			}
		}

		/// <summary>Runs assertions testing that two AclStatus objects contain the same info
		/// 	</summary>
		/// <param name="a">First AclStatus</param>
		/// <param name="b">Second AclStatus</param>
		/// <exception cref="System.Exception"/>
		private void AssertSameAcls(AclStatus a, AclStatus b)
		{
			NUnit.Framework.Assert.IsTrue(a.GetOwner().Equals(b.GetOwner()));
			NUnit.Framework.Assert.IsTrue(a.GetGroup().Equals(b.GetGroup()));
			NUnit.Framework.Assert.IsTrue(a.IsStickyBit() == b.IsStickyBit());
			NUnit.Framework.Assert.IsTrue(a.GetEntries().Count == b.GetEntries().Count);
			foreach (AclEntry e in a.GetEntries())
			{
				NUnit.Framework.Assert.IsTrue(b.GetEntries().Contains(e));
			}
			foreach (AclEntry e_1 in b.GetEntries())
			{
				NUnit.Framework.Assert.IsTrue(a.GetEntries().Contains(e_1));
			}
		}

		/// <summary>
		/// Simple ACL tests on a file:  Set an acl, add an acl, remove one acl,
		/// and remove all acls.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void TestFileAcls()
		{
			if (IsLocalFS())
			{
				return;
			}
			string aclUser1 = "user:foo:rw-";
			string aclUser2 = "user:bar:r--";
			string aclGroup1 = "group::r--";
			string aclSet = "user::rwx," + aclUser1 + "," + aclGroup1 + ",other::---";
			FileSystem proxyFs = FileSystem.Get(GetProxiedFSConf());
			FileSystem httpfs = GetHttpFSFileSystem();
			Path path = new Path(GetProxiedFSTestDir(), "testAclStatus.txt");
			OutputStream os = proxyFs.Create(path);
			os.Write(1);
			os.Close();
			AclStatus proxyAclStat = proxyFs.GetAclStatus(path);
			AclStatus httpfsAclStat = httpfs.GetAclStatus(path);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
			httpfs.SetAcl(path, AclEntry.ParseAclSpec(aclSet, true));
			proxyAclStat = proxyFs.GetAclStatus(path);
			httpfsAclStat = httpfs.GetAclStatus(path);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
			httpfs.ModifyAclEntries(path, AclEntry.ParseAclSpec(aclUser2, true));
			proxyAclStat = proxyFs.GetAclStatus(path);
			httpfsAclStat = httpfs.GetAclStatus(path);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
			httpfs.RemoveAclEntries(path, AclEntry.ParseAclSpec(aclUser1, true));
			proxyAclStat = proxyFs.GetAclStatus(path);
			httpfsAclStat = httpfs.GetAclStatus(path);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
			httpfs.RemoveAcl(path);
			proxyAclStat = proxyFs.GetAclStatus(path);
			httpfsAclStat = httpfs.GetAclStatus(path);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
		}

		/// <summary>Simple acl tests on a directory: set a default acl, remove default acls.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		private void TestDirAcls()
		{
			if (IsLocalFS())
			{
				return;
			}
			string defUser1 = "default:user:glarch:r-x";
			FileSystem proxyFs = FileSystem.Get(GetProxiedFSConf());
			FileSystem httpfs = GetHttpFSFileSystem();
			Path dir = GetProxiedFSTestDir();
			/* ACL Status on a directory */
			AclStatus proxyAclStat = proxyFs.GetAclStatus(dir);
			AclStatus httpfsAclStat = httpfs.GetAclStatus(dir);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
			/* Set a default ACL on the directory */
			httpfs.SetAcl(dir, (AclEntry.ParseAclSpec(defUser1, true)));
			proxyAclStat = proxyFs.GetAclStatus(dir);
			httpfsAclStat = httpfs.GetAclStatus(dir);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
			/* Remove the default ACL */
			httpfs.RemoveDefaultAcl(dir);
			proxyAclStat = proxyFs.GetAclStatus(dir);
			httpfsAclStat = httpfs.GetAclStatus(dir);
			AssertSameAcls(httpfsAclStat, proxyAclStat);
		}

		protected internal enum Operation
		{
			Get,
			Open,
			Create,
			Append,
			Truncate,
			Concat,
			Rename,
			Delete,
			ListStatus,
			WorkingDirectory,
			Mkdirs,
			SetTimes,
			SetPermission,
			SetOwner,
			SetReplication,
			Checksum,
			ContentSummary,
			Fileacls,
			Diracls,
			SetXattr,
			GetXattrs,
			RemoveXattr,
			ListXattrs
		}

		/// <exception cref="System.Exception"/>
		private void Operation(BaseTestHttpFSWith.Operation op)
		{
			switch (op)
			{
				case BaseTestHttpFSWith.Operation.Get:
				{
					TestGet();
					break;
				}

				case BaseTestHttpFSWith.Operation.Open:
				{
					TestOpen();
					break;
				}

				case BaseTestHttpFSWith.Operation.Create:
				{
					TestCreate();
					break;
				}

				case BaseTestHttpFSWith.Operation.Append:
				{
					TestAppend();
					break;
				}

				case BaseTestHttpFSWith.Operation.Truncate:
				{
					TestTruncate();
					break;
				}

				case BaseTestHttpFSWith.Operation.Concat:
				{
					TestConcat();
					break;
				}

				case BaseTestHttpFSWith.Operation.Rename:
				{
					TestRename();
					break;
				}

				case BaseTestHttpFSWith.Operation.Delete:
				{
					TestDelete();
					break;
				}

				case BaseTestHttpFSWith.Operation.ListStatus:
				{
					TestListStatus();
					break;
				}

				case BaseTestHttpFSWith.Operation.WorkingDirectory:
				{
					TestWorkingdirectory();
					break;
				}

				case BaseTestHttpFSWith.Operation.Mkdirs:
				{
					TestMkdirs();
					break;
				}

				case BaseTestHttpFSWith.Operation.SetTimes:
				{
					TestSetTimes();
					break;
				}

				case BaseTestHttpFSWith.Operation.SetPermission:
				{
					TestSetPermission();
					break;
				}

				case BaseTestHttpFSWith.Operation.SetOwner:
				{
					TestSetOwner();
					break;
				}

				case BaseTestHttpFSWith.Operation.SetReplication:
				{
					TestSetReplication();
					break;
				}

				case BaseTestHttpFSWith.Operation.Checksum:
				{
					TestChecksum();
					break;
				}

				case BaseTestHttpFSWith.Operation.ContentSummary:
				{
					TestContentSummary();
					break;
				}

				case BaseTestHttpFSWith.Operation.Fileacls:
				{
					TestFileAcls();
					break;
				}

				case BaseTestHttpFSWith.Operation.Diracls:
				{
					TestDirAcls();
					break;
				}

				case BaseTestHttpFSWith.Operation.SetXattr:
				{
					TestSetXAttr();
					break;
				}

				case BaseTestHttpFSWith.Operation.RemoveXattr:
				{
					TestRemoveXAttr();
					break;
				}

				case BaseTestHttpFSWith.Operation.GetXattrs:
				{
					TestGetXAttrs();
					break;
				}

				case BaseTestHttpFSWith.Operation.ListXattrs:
				{
					TestListXAttrs();
					break;
				}
			}
		}

		[Parameterized.Parameters]
		public static ICollection Operations()
		{
			object[][] ops = new object[BaseTestHttpFSWith.Operation.Values().Length][];
			for (int i = 0; i < BaseTestHttpFSWith.Operation.Values().Length; i++)
			{
				ops[i] = new object[] { BaseTestHttpFSWith.Operation.Values()[i] };
			}
			//To test one or a subset of operations do:
			//return Arrays.asList(new Object[][]{ new Object[]{Operation.APPEND}});
			return Arrays.AsList(ops);
		}

		private BaseTestHttpFSWith.Operation operation;

		public BaseTestHttpFSWith(BaseTestHttpFSWith.Operation operation)
		{
			this.operation = operation;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestOperation()
		{
			CreateHttpFSServer();
			Operation(operation);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestOperationDoAs()
		{
			CreateHttpFSServer();
			UserGroupInformation ugi = UserGroupInformation.CreateProxyUser(HadoopUsersConfTestHelper
				.GetHadoopUsers()[0], UserGroupInformation.GetCurrentUser());
			ugi.DoAs(new _PrivilegedExceptionAction_928(this));
		}

		private sealed class _PrivilegedExceptionAction_928 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_928(BaseTestHttpFSWith _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.Operation(this._enclosing.operation);
				return null;
			}

			private readonly BaseTestHttpFSWith _enclosing;
		}
	}
}
