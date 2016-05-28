using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestWebHdfsContentLength
	{
		private static Socket listenSocket;

		private static string bindAddr;

		private static Path p;

		private static FileSystem fs;

		private static readonly Sharpen.Pattern contentLengthPattern = Sharpen.Pattern.Compile
			("^(Content-Length|Transfer-Encoding):\\s*(.*)", Sharpen.Pattern.Multiline);

		private static string errResponse = "HTTP/1.1 500 Boom\r\n" + "Content-Length: 0\r\n"
			 + "Connection: close\r\n\r\n";

		private static string redirectResponse;

		private static ExecutorService executor;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			listenSocket = new Socket();
			listenSocket.Bind(null);
			bindAddr = NetUtils.GetHostPortString((IPEndPoint)listenSocket.LocalEndPoint);
			redirectResponse = "HTTP/1.1 307 Redirect\r\n" + "Location: http://" + bindAddr +
				 "/path\r\n" + "Connection: close\r\n\r\n";
			p = new Path("webhdfs://" + bindAddr + "/path");
			fs = p.GetFileSystem(new Configuration());
			executor = Executors.NewSingleThreadExecutor();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Teardown()
		{
			if (listenSocket != null)
			{
				listenSocket.Close();
			}
			if (executor != null)
			{
				executor.ShutdownNow();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetOp()
		{
			Future<string> future = ContentLengthFuture(errResponse);
			try
			{
				fs.GetFileStatus(p);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(null, GetContentLength(future));
		}

		[NUnit.Framework.Test]
		public virtual void TestGetOpWithRedirect()
		{
			Future<string> future1 = ContentLengthFuture(redirectResponse);
			Future<string> future2 = ContentLengthFuture(errResponse);
			try
			{
				fs.Open(p).Read();
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(null, GetContentLength(future1));
			NUnit.Framework.Assert.AreEqual(null, GetContentLength(future2));
		}

		[NUnit.Framework.Test]
		public virtual void TestPutOp()
		{
			Future<string> future = ContentLengthFuture(errResponse);
			try
			{
				fs.Mkdirs(p);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual("0", GetContentLength(future));
		}

		[NUnit.Framework.Test]
		public virtual void TestPutOpWithRedirect()
		{
			Future<string> future1 = ContentLengthFuture(redirectResponse);
			Future<string> future2 = ContentLengthFuture(errResponse);
			try
			{
				FSDataOutputStream os = fs.Create(p);
				os.Write(new byte[] { 0 });
				os.Close();
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual("0", GetContentLength(future1));
			NUnit.Framework.Assert.AreEqual("chunked", GetContentLength(future2));
		}

		[NUnit.Framework.Test]
		public virtual void TestPostOp()
		{
			Future<string> future = ContentLengthFuture(errResponse);
			try
			{
				fs.Concat(p, new Path[] { p });
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual("0", GetContentLength(future));
		}

		[NUnit.Framework.Test]
		public virtual void TestPostOpWithRedirect()
		{
			// POST operation with redirect
			Future<string> future1 = ContentLengthFuture(redirectResponse);
			Future<string> future2 = ContentLengthFuture(errResponse);
			try
			{
				FSDataOutputStream os = fs.Append(p);
				os.Write(new byte[] { 0 });
				os.Close();
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual("0", GetContentLength(future1));
			NUnit.Framework.Assert.AreEqual("chunked", GetContentLength(future2));
		}

		[NUnit.Framework.Test]
		public virtual void TestDelete()
		{
			Future<string> future = ContentLengthFuture(errResponse);
			try
			{
				fs.Delete(p, false);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(null, GetContentLength(future));
		}

		private string GetContentLength(Future<string> future)
		{
			string request = null;
			try
			{
				request = future.Get(2, TimeUnit.Seconds);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail(e.ToString());
			}
			Matcher matcher = contentLengthPattern.Matcher(request);
			return matcher.Find() ? matcher.Group(2) : null;
		}

		private Future<string> ContentLengthFuture(string response)
		{
			return executor.Submit(new _Callable_180(response));
		}

		private sealed class _Callable_180 : Callable<string>
		{
			public _Callable_180(string response)
			{
				this.response = response;
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				Socket client = TestWebHdfsContentLength.listenSocket.Accept();
				client.ReceiveTimeout = 2000;
				try
				{
					client.GetOutputStream().Write(Sharpen.Runtime.GetBytesForString(response));
					client.ShutdownOutput();
					byte[] buf = new byte[4 * 1024];
					// much bigger than request
					int n = client.GetInputStream().Read(buf);
					return Sharpen.Runtime.GetStringForBytes(buf, 0, n);
				}
				finally
				{
					client.Close();
				}
			}

			private readonly string response;
		}
	}
}
