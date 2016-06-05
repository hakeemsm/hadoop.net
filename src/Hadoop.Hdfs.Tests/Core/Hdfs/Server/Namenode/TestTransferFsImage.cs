using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestTransferFsImage
	{
		private static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestTransferFsImage
			));

		/// <summary>Regression test for HDFS-1997.</summary>
		/// <remarks>
		/// Regression test for HDFS-1997. Test that, if an exception
		/// occurs on the client side, it is properly reported as such,
		/// and reported to the associated NNStorage object.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientSideException()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			NNStorage mockStorage = Org.Mockito.Mockito.Mock<NNStorage>();
			IList<FilePath> localPath = Collections.SingletonList(new FilePath("/xxxxx-does-not-exist/blah"
				));
			try
			{
				Uri fsName = DFSUtil.GetInfoServer(cluster.GetNameNode().GetServiceRpcAddress(), 
					conf, DFSUtil.GetHttpClientScheme(conf)).ToURL();
				string id = "getimage=1&txid=0";
				TransferFsImage.GetFileClient(fsName, id, localPath, mockStorage, false);
				NUnit.Framework.Assert.Fail("Didn't get an exception!");
			}
			catch (IOException ioe)
			{
				Org.Mockito.Mockito.Verify(mockStorage).ReportErrorOnFile(localPath[0]);
				NUnit.Framework.Assert.IsTrue("Unexpected exception: " + StringUtils.StringifyException
					(ioe), ioe.Message.Contains("Unable to download to any storage"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Similar to the above test, except that there are multiple local files
		/// and one of them can be saved.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientSideExceptionOnJustOneDir()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			NNStorage mockStorage = Org.Mockito.Mockito.Mock<NNStorage>();
			IList<FilePath> localPaths = ImmutableList.Of(new FilePath("/xxxxx-does-not-exist/blah"
				), new FilePath(TestDir, "testfile"));
			try
			{
				Uri fsName = DFSUtil.GetInfoServer(cluster.GetNameNode().GetServiceRpcAddress(), 
					conf, DFSUtil.GetHttpClientScheme(conf)).ToURL();
				string id = "getimage=1&txid=0";
				TransferFsImage.GetFileClient(fsName, id, localPaths, mockStorage, false);
				Org.Mockito.Mockito.Verify(mockStorage).ReportErrorOnFile(localPaths[0]);
				NUnit.Framework.Assert.IsTrue("The valid local file should get saved properly", localPaths
					[1].Length() > 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test to verify the read timeout</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetImageTimeout()
		{
			HttpServer2 testServer = HttpServerFunctionalTest.CreateServer("hdfs");
			try
			{
				testServer.AddServlet("ImageTransfer", ImageServlet.PathSpec, typeof(TestTransferFsImage.TestImageTransferServlet
					));
				testServer.Start();
				Uri serverURL = HttpServerFunctionalTest.GetServerURL(testServer);
				TransferFsImage.timeout = 2000;
				try
				{
					TransferFsImage.GetFileClient(serverURL, "txid=1", null, null, false);
					NUnit.Framework.Assert.Fail("TransferImage Should fail with timeout");
				}
				catch (SocketTimeoutException e)
				{
					NUnit.Framework.Assert.AreEqual("Read should timeout", "Read timed out", e.Message
						);
				}
			}
			finally
			{
				if (testServer != null)
				{
					testServer.Stop();
				}
			}
		}

		/// <summary>Test to verify the timeout of Image upload</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestImageUploadTimeout()
		{
			Configuration conf = new HdfsConfiguration();
			NNStorage mockStorage = Org.Mockito.Mockito.Mock<NNStorage>();
			HttpServer2 testServer = HttpServerFunctionalTest.CreateServer("hdfs");
			try
			{
				testServer.AddServlet("ImageTransfer", ImageServlet.PathSpec, typeof(TestTransferFsImage.TestImageTransferServlet
					));
				testServer.Start();
				Uri serverURL = HttpServerFunctionalTest.GetServerURL(testServer);
				// set the timeout here, otherwise it will take default.
				TransferFsImage.timeout = 2000;
				FilePath tmpDir = new FilePath(new FileSystemTestHelper().GetTestRootDir());
				tmpDir.Mkdirs();
				FilePath mockImageFile = FilePath.CreateTempFile("image", string.Empty, tmpDir);
				FileOutputStream imageFile = new FileOutputStream(mockImageFile);
				imageFile.Write(Sharpen.Runtime.GetBytesForString("data"));
				imageFile.Close();
				Org.Mockito.Mockito.When(mockStorage.FindImageFile(Org.Mockito.Mockito.Any<NNStorage.NameNodeFile
					>(), Org.Mockito.Mockito.AnyLong())).ThenReturn(mockImageFile);
				Org.Mockito.Mockito.When(mockStorage.ToColonSeparatedString()).ThenReturn("storage:info:string"
					);
				try
				{
					TransferFsImage.UploadImageFromStorage(serverURL, conf, mockStorage, NNStorage.NameNodeFile
						.Image, 1L);
					NUnit.Framework.Assert.Fail("TransferImage Should fail with timeout");
				}
				catch (SocketTimeoutException e)
				{
					NUnit.Framework.Assert.AreEqual("Upload should timeout", "Read timed out", e.Message
						);
				}
			}
			finally
			{
				testServer.Stop();
			}
		}

		[System.Serializable]
		public class TestImageTransferServlet : HttpServlet
		{
			private const long serialVersionUID = 1L;

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				lock (this)
				{
					try
					{
						Sharpen.Runtime.Wait(this, 5000);
					}
					catch (Exception)
					{
					}
				}
			}

			// Ignore
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoPut(HttpServletRequest req, HttpServletResponse resp)
			{
				lock (this)
				{
					try
					{
						Sharpen.Runtime.Wait(this, 5000);
					}
					catch (Exception)
					{
					}
				}
			}
			// Ignore
		}
	}
}
