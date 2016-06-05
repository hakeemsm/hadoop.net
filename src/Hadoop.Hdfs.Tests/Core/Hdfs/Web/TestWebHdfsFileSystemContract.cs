using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestWebHdfsFileSystemContract : FileSystemContractBaseTest
	{
		private static readonly Configuration conf = new Configuration();

		private static readonly MiniDFSCluster cluster;

		private string defaultWorkingDirectory;

		private UserGroupInformation ugi;

		static TestWebHdfsFileSystemContract()
		{
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.WaitActive();
				//change root permission to 777
				cluster.GetFileSystem().SetPermission(new Path("/"), new FsPermission((short)0x1ff
					));
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			//get file system as a non-superuser
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			ugi = UserGroupInformation.CreateUserForTesting(current.GetShortUserName() + "x", 
				new string[] { "user" });
			fs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(ugi, conf, WebHdfsFileSystem.Scheme);
			defaultWorkingDirectory = fs.GetWorkingDirectory().ToUri().GetPath();
		}

		protected override string GetDefaultWorkingDirectory()
		{
			return defaultWorkingDirectory;
		}

		/// <summary>
		/// HDFS throws AccessControlException
		/// when calling exist(..) on a path /foo/bar/file
		/// but /foo/bar is indeed a file in HDFS.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public override void TestMkdirsFailsForSubdirectoryOfExistingFile()
		{
			Path testDir = Path("/test/hadoop");
			NUnit.Framework.Assert.IsFalse(fs.Exists(testDir));
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(testDir));
			NUnit.Framework.Assert.IsTrue(fs.Exists(testDir));
			CreateFile(Path("/test/hadoop/file"));
			Path testSubDir = Path("/test/hadoop/file/subdir");
			try
			{
				fs.Mkdirs(testSubDir);
				Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			try
			{
				NUnit.Framework.Assert.IsFalse(fs.Exists(testSubDir));
			}
			catch (AccessControlException)
			{
			}
			// also okay for HDFS.
			Path testDeepSubDir = Path("/test/hadoop/file/deep/sub/dir");
			try
			{
				fs.Mkdirs(testDeepSubDir);
				Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			try
			{
				NUnit.Framework.Assert.IsFalse(fs.Exists(testDeepSubDir));
			}
			catch (AccessControlException)
			{
			}
		}

		// also okay for HDFS.
		//the following are new tests (i.e. not over-riding the super class methods)
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetFileBlockLocations()
		{
			string f = "/test/testGetFileBlockLocations";
			CreateFile(Path(f));
			BlockLocation[] computed = fs.GetFileBlockLocations(new Path(f), 0L, 1L);
			BlockLocation[] expected = cluster.GetFileSystem().GetFileBlockLocations(new Path
				(f), 0L, 1L);
			NUnit.Framework.Assert.AreEqual(expected.Length, computed.Length);
			for (int i = 0; i < computed.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(expected[i].ToString(), computed[i].ToString());
				// Check names
				string[] names1 = expected[i].GetNames();
				string[] names2 = computed[i].GetNames();
				Arrays.Sort(names1);
				Arrays.Sort(names2);
				Assert.AssertArrayEquals("Names differ", names1, names2);
				// Check topology
				string[] topos1 = expected[i].GetTopologyPaths();
				string[] topos2 = computed[i].GetTopologyPaths();
				Arrays.Sort(topos1);
				Arrays.Sort(topos2);
				Assert.AssertArrayEquals("Topology differs", topos1, topos2);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCaseInsensitive()
		{
			Path p = new Path("/test/testCaseInsensitive");
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
			PutOpParam.OP op = PutOpParam.OP.Mkdirs;
			//replace query with mix case letters
			Uri url = webhdfs.ToUrl(op, p);
			WebHdfsFileSystem.Log.Info("url      = " + url);
			Uri replaced = new Uri(url.ToString().Replace(op.ToQueryString(), "Op=mkDIrs"));
			WebHdfsFileSystem.Log.Info("replaced = " + replaced);
			//connect with the replaced URL.
			HttpURLConnection conn = (HttpURLConnection)replaced.OpenConnection();
			conn.SetRequestMethod(op.GetType().ToString());
			conn.Connect();
			BufferedReader @in = new BufferedReader(new InputStreamReader(conn.GetInputStream
				()));
			for (string line; (line = @in.ReadLine()) != null; )
			{
				WebHdfsFileSystem.Log.Info("> " + line);
			}
			//check if the command successes.
			NUnit.Framework.Assert.IsTrue(fs.GetFileStatus(p).IsDirectory());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOpenNonExistFile()
		{
			Path p = new Path("/test/testOpenNonExistFile");
			//open it as a file, should get FileNotFoundException 
			try
			{
				fs.Open(p);
				Fail("Expected FileNotFoundException was not thrown");
			}
			catch (FileNotFoundException fnfe)
			{
				WebHdfsFileSystem.Log.Info("This is expected.", fnfe);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSeek()
		{
			Path dir = new Path("/test/testSeek");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
			{
				//test zero file size
				Path zero = new Path(dir, "zero");
				fs.Create(zero).Close();
				int count = 0;
				FSDataInputStream @in = fs.Open(zero);
				for (; @in.Read() != -1; count++)
				{
				}
				@in.Close();
				NUnit.Framework.Assert.AreEqual(0, count);
			}
			byte[] mydata = new byte[1 << 20];
			new Random().NextBytes(mydata);
			Path p = new Path(dir, "file");
			FSDataOutputStream @out = fs.Create(p, false, 4096, (short)3, 1L << 17);
			@out.Write(mydata, 0, mydata.Length);
			@out.Close();
			int one_third = mydata.Length / 3;
			int two_third = one_third * 2;
			{
				//test seek
				int offset = one_third;
				int len = mydata.Length - offset;
				byte[] buf = new byte[len];
				FSDataInputStream @in = fs.Open(p);
				@in.Seek(offset);
				//read all remaining data
				@in.ReadFully(buf);
				@in.Close();
				for (int i = 0; i < buf.Length; i++)
				{
					NUnit.Framework.Assert.AreEqual("Position " + i + ", offset=" + offset + ", length="
						 + len, mydata[i + offset], buf[i]);
				}
			}
			{
				//test position read (read the data after the two_third location)
				int offset = two_third;
				int len = mydata.Length - offset;
				byte[] buf = new byte[len];
				FSDataInputStream @in = fs.Open(p);
				@in.ReadFully(offset, buf);
				@in.Close();
				for (int i = 0; i < buf.Length; i++)
				{
					NUnit.Framework.Assert.AreEqual("Position " + i + ", offset=" + offset + ", length="
						 + len, mydata[i + offset], buf[i]);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRootDir()
		{
			Path root = new Path("/");
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
			Uri url = webhdfs.ToUrl(GetOpParam.OP.Null, root);
			WebHdfsFileSystem.Log.Info("null url=" + url);
			NUnit.Framework.Assert.IsTrue(url.ToString().Contains("v1"));
			//test root permission
			FileStatus status = fs.GetFileStatus(root);
			NUnit.Framework.Assert.IsTrue(status != null);
			NUnit.Framework.Assert.AreEqual(0x1ff, status.GetPermission().ToShort());
			//delete root
			NUnit.Framework.Assert.IsFalse(fs.Delete(root, true));
			//create file using root path 
			try
			{
				FSDataOutputStream @out = fs.Create(root);
				@out.Write(1);
				@out.Close();
				Fail();
			}
			catch (IOException e)
			{
				WebHdfsFileSystem.Log.Info("This is expected.", e);
			}
			//open file using root path 
			try
			{
				FSDataInputStream @in = fs.Open(root);
				@in.Read();
				Fail();
			}
			catch (IOException e)
			{
				WebHdfsFileSystem.Log.Info("This is expected.", e);
			}
		}

		/// <summary>Test get with length parameter greater than actual file length.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLengthParamLongerThanFile()
		{
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
			Path dir = new Path("/test");
			NUnit.Framework.Assert.IsTrue(webhdfs.Mkdirs(dir));
			// Create a file with some content.
			Path testFile = new Path("/test/testLengthParamLongerThanFile");
			string content = "testLengthParamLongerThanFile";
			FSDataOutputStream testFileOut = webhdfs.Create(testFile);
			try
			{
				testFileOut.Write(Sharpen.Runtime.GetBytesForString(content, "US-ASCII"));
			}
			finally
			{
				IOUtils.CloseStream(testFileOut);
			}
			// Open the file, but request length longer than actual file length by 1.
			HttpOpParam.OP op = GetOpParam.OP.Open;
			Uri url = webhdfs.ToUrl(op, testFile, new LengthParam((long)(content.Length + 1))
				);
			HttpURLConnection conn = null;
			InputStream @is = null;
			try
			{
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetDoOutput(op.GetDoOutput());
				conn.SetInstanceFollowRedirects(true);
				// Expect OK response and Content-Length header equal to actual length.
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScOk, conn.GetResponseCode());
				NUnit.Framework.Assert.AreEqual(content.Length.ToString(), conn.GetHeaderField("Content-Length"
					));
				// Check content matches.
				byte[] respBody = new byte[content.Length];
				@is = conn.GetInputStream();
				IOUtils.ReadFully(@is, respBody, 0, content.Length);
				NUnit.Framework.Assert.AreEqual(content, Sharpen.Runtime.GetStringForBytes(respBody
					, "US-ASCII"));
			}
			finally
			{
				IOUtils.CloseStream(@is);
				if (conn != null)
				{
					conn.Disconnect();
				}
			}
		}

		/// <summary>
		/// Test get with offset and length parameters that combine to request a length
		/// greater than actual file length.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOffsetPlusLengthParamsLongerThanFile()
		{
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
			Path dir = new Path("/test");
			NUnit.Framework.Assert.IsTrue(webhdfs.Mkdirs(dir));
			// Create a file with some content.
			Path testFile = new Path("/test/testOffsetPlusLengthParamsLongerThanFile");
			string content = "testOffsetPlusLengthParamsLongerThanFile";
			FSDataOutputStream testFileOut = webhdfs.Create(testFile);
			try
			{
				testFileOut.Write(Sharpen.Runtime.GetBytesForString(content, "US-ASCII"));
			}
			finally
			{
				IOUtils.CloseStream(testFileOut);
			}
			// Open the file, but request offset starting at 1 and length equal to file
			// length.  Considering the offset, this is longer than the actual content.
			HttpOpParam.OP op = GetOpParam.OP.Open;
			Uri url = webhdfs.ToUrl(op, testFile, new LengthParam(Sharpen.Extensions.ValueOf(
				content.Length)), new OffsetParam(1L));
			HttpURLConnection conn = null;
			InputStream @is = null;
			try
			{
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetDoOutput(op.GetDoOutput());
				conn.SetInstanceFollowRedirects(true);
				// Expect OK response and Content-Length header equal to actual length.
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScOk, conn.GetResponseCode());
				NUnit.Framework.Assert.AreEqual((content.Length - 1).ToString(), conn.GetHeaderField
					("Content-Length"));
				// Check content matches.
				byte[] respBody = new byte[content.Length - 1];
				@is = conn.GetInputStream();
				IOUtils.ReadFully(@is, respBody, 0, content.Length - 1);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.Substring(content, 1), Sharpen.Runtime.GetStringForBytes
					(respBody, "US-ASCII"));
			}
			finally
			{
				IOUtils.CloseStream(@is);
				if (conn != null)
				{
					conn.Disconnect();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResponseCode()
		{
			WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
			Path root = new Path("/");
			Path dir = new Path("/test/testUrl");
			NUnit.Framework.Assert.IsTrue(webhdfs.Mkdirs(dir));
			Path file = new Path("/test/file");
			FSDataOutputStream @out = webhdfs.Create(file);
			@out.Write(1);
			@out.Close();
			{
				//test GETHOMEDIRECTORY
				Uri url = webhdfs.ToUrl(GetOpParam.OP.Gethomedirectory, root);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				IDictionary<object, object> m = WebHdfsTestUtil.ConnectAndGetJson(conn, HttpServletResponse
					.ScOk);
				NUnit.Framework.Assert.AreEqual(WebHdfsFileSystem.GetHomeDirectoryString(ugi), m[
					typeof(Path).Name]);
				conn.Disconnect();
			}
			{
				//test GETHOMEDIRECTORY with unauthorized doAs
				Uri url = webhdfs.ToUrl(GetOpParam.OP.Gethomedirectory, root, new DoAsParam(ugi.GetShortUserName
					() + "proxy"));
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScForbidden, conn.GetResponseCode
					());
				conn.Disconnect();
			}
			{
				//test set owner with empty parameters
				Uri url = webhdfs.ToUrl(PutOpParam.OP.Setowner, dir);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScBadRequest, conn.GetResponseCode
					());
				conn.Disconnect();
			}
			{
				//test set replication on a directory
				HttpOpParam.OP op = PutOpParam.OP.Setreplication;
				Uri url = webhdfs.ToUrl(op, dir);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScOk, conn.GetResponseCode());
				NUnit.Framework.Assert.IsFalse(webhdfs.SetReplication(dir, (short)1));
				conn.Disconnect();
			}
			{
				//test get file status for a non-exist file.
				Path p = new Path(dir, "non-exist");
				Uri url = webhdfs.ToUrl(GetOpParam.OP.Getfilestatus, p);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScNotFound, conn.GetResponseCode
					());
				conn.Disconnect();
			}
			{
				//test set permission with empty parameters
				HttpOpParam.OP op = PutOpParam.OP.Setpermission;
				Uri url = webhdfs.ToUrl(op, dir);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScOk, conn.GetResponseCode());
				NUnit.Framework.Assert.AreEqual(0, conn.GetContentLength());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationOctetStream, conn.GetContentType
					());
				NUnit.Framework.Assert.AreEqual((short)0x1ed, webhdfs.GetFileStatus(dir).GetPermission
					().ToShort());
				conn.Disconnect();
			}
			{
				//test append.
				AppendTestUtil.TestAppend(fs, new Path(dir, "append"));
			}
			{
				//test NamenodeAddressParam not set.
				HttpOpParam.OP op = PutOpParam.OP.Create;
				Uri url = webhdfs.ToUrl(op, dir);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetDoOutput(false);
				conn.SetInstanceFollowRedirects(false);
				conn.Connect();
				string redirect = conn.GetHeaderField("Location");
				conn.Disconnect();
				//remove NamenodeAddressParam
				WebHdfsFileSystem.Log.Info("redirect = " + redirect);
				int i = redirect.IndexOf(NamenodeAddressParam.Name);
				int j = redirect.IndexOf("&", i);
				string modified = Sharpen.Runtime.Substring(redirect, 0, i - 1) + Sharpen.Runtime.Substring
					(redirect, j);
				WebHdfsFileSystem.Log.Info("modified = " + modified);
				//connect to datanode
				conn = (HttpURLConnection)new Uri(modified).OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetDoOutput(op.GetDoOutput());
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScBadRequest, conn.GetResponseCode
					());
			}
			{
				//test jsonParse with non-json type.
				HttpOpParam.OP op = GetOpParam.OP.Open;
				Uri url = webhdfs.ToUrl(op, file);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.Connect();
				try
				{
					WebHdfsFileSystem.JsonParse(conn, false);
					Fail();
				}
				catch (IOException ioe)
				{
					WebHdfsFileSystem.Log.Info("GOOD", ioe);
				}
				conn.Disconnect();
			}
			{
				//test create with path containing spaces
				HttpOpParam.OP op = PutOpParam.OP.Create;
				Path path = new Path("/test/path with spaces");
				Uri url = webhdfs.ToUrl(op, path);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetDoOutput(false);
				conn.SetInstanceFollowRedirects(false);
				string redirect;
				try
				{
					conn.Connect();
					NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScTemporaryRedirect, conn.GetResponseCode
						());
					redirect = conn.GetHeaderField("Location");
				}
				finally
				{
					conn.Disconnect();
				}
				conn = (HttpURLConnection)new Uri(redirect).OpenConnection();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetDoOutput(op.GetDoOutput());
				try
				{
					conn.Connect();
					NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScCreated, conn.GetResponseCode
						());
				}
				finally
				{
					conn.Disconnect();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccess()
		{
			Path p1 = new Path("/pathX");
			try
			{
				UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("alpha", new 
					string[] { "beta" });
				WebHdfsFileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(ugi, conf, WebHdfsFileSystem
					.Scheme);
				fs.Mkdirs(p1);
				fs.SetPermission(p1, new FsPermission((short)0x124));
				fs.Access(p1, FsAction.Read);
				try
				{
					fs.Access(p1, FsAction.Write);
					Fail("The access call should have failed.");
				}
				catch (AccessControlException)
				{
				}
				// expected
				Path badPath = new Path("/bad");
				try
				{
					fs.Access(badPath, FsAction.Read);
					Fail("The access call should have failed");
				}
				catch (FileNotFoundException)
				{
				}
			}
			finally
			{
				// expected
				fs.Delete(p1, true);
			}
		}
	}
}
