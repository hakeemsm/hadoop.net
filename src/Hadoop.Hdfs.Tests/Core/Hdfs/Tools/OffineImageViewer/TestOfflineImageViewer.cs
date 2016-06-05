using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Javax.Xml.Parsers;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO.Output;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	public class TestOfflineImageViewer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(OfflineImageViewerPB));

		private const int NumDirs = 3;

		private const int FilesPerDir = 4;

		private const string TestRenewer = "JobTracker";

		private static FilePath originalFsimage = null;

		internal static readonly Dictionary<string, FileStatus> writtenFiles = Maps.NewHashMap
			();

		[Rule]
		public TemporaryFolder folder = new TemporaryFolder();

		// namespace as written to dfs, to be compared with viewer's output
		// Create a populated namespace for later testing. Save its contents to a
		// data structure and store its fsimage location.
		// We only want to generate the fsimage file once and use it for
		// multiple tests.
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void CreateOriginalFSImage()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.SetLong(DFSConfigKeys.DfsNamenodeDelegationTokenMaxLifetimeKey, 10000);
				conf.SetLong(DFSConfigKeys.DfsNamenodeDelegationTokenRenewIntervalKey, 5000);
				conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//"
					 + "DEFAULT");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				// Create a reasonable namespace
				for (int i = 0; i < NumDirs; i++)
				{
					Path dir = new Path("/dir" + i);
					hdfs.Mkdirs(dir);
					writtenFiles[dir.ToString()] = PathToFileEntry(hdfs, dir.ToString());
					for (int j = 0; j < FilesPerDir; j++)
					{
						Path file = new Path(dir, "file" + j);
						FSDataOutputStream o = hdfs.Create(file);
						o.Write(23);
						o.Close();
						writtenFiles[file.ToString()] = PathToFileEntry(hdfs, file.ToString());
					}
				}
				// Create an empty directory
				Path emptydir = new Path("/emptydir");
				hdfs.Mkdirs(emptydir);
				writtenFiles[emptydir.ToString()] = hdfs.GetFileStatus(emptydir);
				//Create a directory whose name should be escaped in XML
				Path invalidXMLDir = new Path("/dirContainingInvalidXMLChar\u0000here");
				hdfs.Mkdirs(invalidXMLDir);
				// Get delegation tokens so we log the delegation token op
				Org.Apache.Hadoop.Security.Token.Token<object>[] delegationTokens = hdfs.AddDelegationTokens
					(TestRenewer, null);
				foreach (Org.Apache.Hadoop.Security.Token.Token<object> t in delegationTokens)
				{
					Log.Debug("got token " + t);
				}
				Path snapshot = new Path("/snapshot");
				hdfs.Mkdirs(snapshot);
				hdfs.AllowSnapshot(snapshot);
				hdfs.Mkdirs(new Path("/snapshot/1"));
				hdfs.Delete(snapshot, true);
				// Set XAttrs so the fsimage contains XAttr ops
				Path xattr = new Path("/xattr");
				hdfs.Mkdirs(xattr);
				hdfs.SetXAttr(xattr, "user.a1", new byte[] { unchecked((int)(0x31)), unchecked((int
					)(0x32)), unchecked((int)(0x33)) });
				hdfs.SetXAttr(xattr, "user.a2", new byte[] { unchecked((int)(0x37)), unchecked((int
					)(0x38)), unchecked((int)(0x39)) });
				// OIV should be able to handle empty value XAttrs
				hdfs.SetXAttr(xattr, "user.a3", null);
				writtenFiles[xattr.ToString()] = hdfs.GetFileStatus(xattr);
				// Write results to the fsimage file
				hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
				hdfs.SaveNamespace();
				// Determine location of fsimage file
				originalFsimage = FSImageTestUtil.FindLatestImageFile(FSImageTestUtil.GetFSImage(
					cluster.GetNameNode()).GetStorage().GetStorageDir(0));
				if (originalFsimage == null)
				{
					throw new RuntimeException("Didn't generate or can't find fsimage");
				}
				Log.Debug("original FS image file is " + originalFsimage);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void DeleteOriginalFSImage()
		{
			if (originalFsimage != null && originalFsimage.Exists())
			{
				originalFsimage.Delete();
			}
		}

		// Convenience method to generate a file status from file system for
		// later comparison
		/// <exception cref="System.IO.IOException"/>
		private static FileStatus PathToFileEntry(FileSystem hdfs, string file)
		{
			return hdfs.GetFileStatus(new Path(file));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTruncatedFSImage()
		{
			FilePath truncatedFile = folder.NewFile();
			TextWriter output = new TextWriter(NullOutputStream.NullOutputStream);
			CopyPartOfFile(originalFsimage, truncatedFile);
			new FileDistributionCalculator(new Configuration(), 0, 0, output).Visit(new RandomAccessFile
				(truncatedFile, "r"));
		}

		/// <exception cref="System.IO.IOException"/>
		private void CopyPartOfFile(FilePath src, FilePath dest)
		{
			FileInputStream @in = null;
			FileOutputStream @out = null;
			int MaxBytes = 700;
			try
			{
				@in = new FileInputStream(src);
				@out = new FileOutputStream(dest);
				@in.GetChannel().TransferTo(0, MaxBytes, @out.GetChannel());
			}
			finally
			{
				IOUtils.Cleanup(null, @in);
				IOUtils.Cleanup(null, @out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileDistributionCalculator()
		{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			TextWriter o = new TextWriter(output);
			new FileDistributionCalculator(new Configuration(), 0, 0, o).Visit(new RandomAccessFile
				(originalFsimage, "r"));
			o.Close();
			string outputString = output.ToString();
			Sharpen.Pattern p = Sharpen.Pattern.Compile("totalFiles = (\\d+)\n");
			Matcher matcher = p.Matcher(outputString);
			NUnit.Framework.Assert.IsTrue(matcher.Find() && matcher.GroupCount() == 1);
			int totalFiles = System.Convert.ToInt32(matcher.Group(1));
			NUnit.Framework.Assert.AreEqual(NumDirs * FilesPerDir, totalFiles);
			p = Sharpen.Pattern.Compile("totalDirectories = (\\d+)\n");
			matcher = p.Matcher(outputString);
			NUnit.Framework.Assert.IsTrue(matcher.Find() && matcher.GroupCount() == 1);
			int totalDirs = System.Convert.ToInt32(matcher.Group(1));
			// totalDirs includes root directory, empty directory, and xattr directory
			NUnit.Framework.Assert.AreEqual(NumDirs + 4, totalDirs);
			FileStatus maxFile = Sharpen.Collections.Max(writtenFiles.Values, new _IComparer_238
				());
			p = Sharpen.Pattern.Compile("maxFileSize = (\\d+)\n");
			matcher = p.Matcher(output.ToString("UTF-8"));
			NUnit.Framework.Assert.IsTrue(matcher.Find() && matcher.GroupCount() == 1);
			NUnit.Framework.Assert.AreEqual(maxFile.GetLen(), long.Parse(matcher.Group(1)));
		}

		private sealed class _IComparer_238 : IComparer<FileStatus>
		{
			public _IComparer_238()
			{
			}

			public int Compare(FileStatus first, FileStatus second)
			{
				return first.GetLen() < second.GetLen() ? -1 : ((first.GetLen() == second.GetLen(
					)) ? 0 : 1);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileDistributionCalculatorWithOptions()
		{
			int status = OfflineImageViewerPB.Run(new string[] { "-i", originalFsimage.GetAbsolutePath
				(), "-o", "-", "-p", "FileDistribution", "-maxSize", "512", "-step", "8" });
			NUnit.Framework.Assert.AreEqual(0, status);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void TestPBImageXmlWriter()
		{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			TextWriter o = new TextWriter(output);
			PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), o);
			v.Visit(new RandomAccessFile(originalFsimage, "r"));
			SAXParserFactory spf = SAXParserFactory.NewInstance();
			SAXParser parser = spf.NewSAXParser();
			string xml = output.ToString();
			parser.Parse(new InputSource(new StringReader(xml)), new DefaultHandler());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebImageViewer()
		{
			WebImageViewer viewer = new WebImageViewer(NetUtils.CreateSocketAddr("localhost:0"
				));
			try
			{
				viewer.InitServer(originalFsimage.GetAbsolutePath());
				int port = viewer.GetPort();
				// create a WebHdfsFileSystem instance
				URI uri = new URI("webhdfs://localhost:" + port.ToString());
				Configuration conf = new Configuration();
				WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)FileSystem.Get(uri, conf);
				// verify the number of directories
				FileStatus[] statuses = webhdfs.ListStatus(new Path("/"));
				NUnit.Framework.Assert.AreEqual(NumDirs + 3, statuses.Length);
				// contains empty and xattr directory
				// verify the number of files in the directory
				statuses = webhdfs.ListStatus(new Path("/dir0"));
				NUnit.Framework.Assert.AreEqual(FilesPerDir, statuses.Length);
				// compare a file
				FileStatus status = webhdfs.ListStatus(new Path("/dir0/file0"))[0];
				FileStatus expected = writtenFiles["/dir0/file0"];
				CompareFile(expected, status);
				// LISTSTATUS operation to an empty directory
				statuses = webhdfs.ListStatus(new Path("/emptydir"));
				NUnit.Framework.Assert.AreEqual(0, statuses.Length);
				// LISTSTATUS operation to a invalid path
				Uri url = new Uri("http://localhost:" + port + "/webhdfs/v1/invalid/?op=LISTSTATUS"
					);
				VerifyHttpResponseCode(HttpURLConnection.HttpNotFound, url);
				// LISTSTATUS operation to a invalid prefix
				url = new Uri("http://localhost:" + port + "/foo");
				VerifyHttpResponseCode(HttpURLConnection.HttpNotFound, url);
				// GETFILESTATUS operation
				status = webhdfs.GetFileStatus(new Path("/dir0/file0"));
				CompareFile(expected, status);
				// GETFILESTATUS operation to a invalid path
				url = new Uri("http://localhost:" + port + "/webhdfs/v1/invalid/?op=GETFILESTATUS"
					);
				VerifyHttpResponseCode(HttpURLConnection.HttpNotFound, url);
				// invalid operation
				url = new Uri("http://localhost:" + port + "/webhdfs/v1/?op=INVALID");
				VerifyHttpResponseCode(HttpURLConnection.HttpBadRequest, url);
				// invalid method
				url = new Uri("http://localhost:" + port + "/webhdfs/v1/?op=LISTSTATUS");
				HttpURLConnection connection = (HttpURLConnection)url.OpenConnection();
				connection.SetRequestMethod("POST");
				connection.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpBadMethod, connection.GetResponseCode
					());
			}
			finally
			{
				// shutdown the viewer
				viewer.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPBDelimitedWriter()
		{
			TestPBDelimitedWriter(string.Empty);
			// Test in memory db.
			TestPBDelimitedWriter(new FileSystemTestHelper().GetTestRootDir() + "/delimited.db"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TestPBDelimitedWriter(string db)
		{
			string Delimiter = "\t";
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			using (TextWriter o = new TextWriter(output))
			{
				PBImageDelimitedTextWriter v = new PBImageDelimitedTextWriter(o, Delimiter, db);
				v.Visit(new RandomAccessFile(originalFsimage, "r"));
			}
			ICollection<string> fileNames = new HashSet<string>();
			using (ByteArrayInputStream input = new ByteArrayInputStream(output.ToByteArray()
				))
			{
				using (BufferedReader reader = new BufferedReader(new InputStreamReader(input)))
				{
					string line;
					while ((line = reader.ReadLine()) != null)
					{
						System.Console.Out.WriteLine(line);
						string[] fields = line.Split(Delimiter);
						NUnit.Framework.Assert.AreEqual(12, fields.Length);
						fileNames.AddItem(fields[0]);
					}
				}
			}
			// writtenFiles does not contain root directory and "invalid XML char" dir.
			for (IEnumerator<string> it = fileNames.GetEnumerator(); it.HasNext(); )
			{
				string filename = it.Next();
				if (filename.StartsWith("/dirContainingInvalidXMLChar"))
				{
					it.Remove();
				}
				else
				{
					if (filename.Equals("/"))
					{
						it.Remove();
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(writtenFiles.Keys, fileNames);
		}

		private static void CompareFile(FileStatus expected, FileStatus status)
		{
			NUnit.Framework.Assert.AreEqual(expected.GetAccessTime(), status.GetAccessTime());
			NUnit.Framework.Assert.AreEqual(expected.GetBlockSize(), status.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(expected.GetGroup(), status.GetGroup());
			NUnit.Framework.Assert.AreEqual(expected.GetLen(), status.GetLen());
			NUnit.Framework.Assert.AreEqual(expected.GetModificationTime(), status.GetModificationTime
				());
			NUnit.Framework.Assert.AreEqual(expected.GetOwner(), status.GetOwner());
			NUnit.Framework.Assert.AreEqual(expected.GetPermission(), status.GetPermission());
			NUnit.Framework.Assert.AreEqual(expected.GetReplication(), status.GetReplication(
				));
			NUnit.Framework.Assert.AreEqual(expected.IsDirectory(), status.IsDirectory());
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyHttpResponseCode(int expectedCode, Uri url)
		{
			HttpURLConnection connection = (HttpURLConnection)url.OpenConnection();
			connection.SetRequestMethod("GET");
			connection.Connect();
			NUnit.Framework.Assert.AreEqual(expectedCode, connection.GetResponseCode());
		}
	}
}
