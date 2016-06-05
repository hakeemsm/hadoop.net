using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>
	/// This test suite checks that WebHdfsFileSystem sets connection timeouts and
	/// read timeouts on its sockets, thus preventing threads from hanging
	/// indefinitely on an undefined/infinite timeout.
	/// </summary>
	/// <remarks>
	/// This test suite checks that WebHdfsFileSystem sets connection timeouts and
	/// read timeouts on its sockets, thus preventing threads from hanging
	/// indefinitely on an undefined/infinite timeout.  The tests work by starting a
	/// bogus server on the namenode HTTP port, which is rigged to not accept new
	/// connections or to accept connections but not send responses.
	/// </remarks>
	public class TestWebHdfsTimeouts
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestWebHdfsTimeouts));

		private const int ClientsToConsumeBacklog = 100;

		private const int ConnectionBacklog = 1;

		private const int ShortSocketTimeout = 5;

		private const int TestTimeout = 10000;

		private IList<SocketChannel> clients;

		private WebHdfsFileSystem fs;

		private IPEndPoint nnHttpAddress;

		private Socket serverSocket;

		private Sharpen.Thread serverThread;

		private sealed class _ConnectionConfigurator_71 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_71()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				conn.SetReadTimeout(TestWebHdfsTimeouts.ShortSocketTimeout);
				conn.SetConnectTimeout(TestWebHdfsTimeouts.ShortSocketTimeout);
				return conn;
			}
		}

		private readonly URLConnectionFactory connectionFactory = new URLConnectionFactory
			(new _ConnectionConfigurator_71());

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = WebHdfsTestUtil.CreateConf();
			serverSocket = Sharpen.Extensions.CreateServerSocket(0, ConnectionBacklog);
			nnHttpAddress = new IPEndPoint("localhost", serverSocket.GetLocalPort());
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "localhost:" + serverSocket.GetLocalPort
				());
			fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme);
			fs.connectionFactory = connectionFactory;
			clients = new AList<SocketChannel>();
			serverThread = null;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(clients, new SocketChannel[clients
				.Count]));
			IOUtils.Cleanup(Log, fs);
			if (serverSocket != null)
			{
				try
				{
					serverSocket.Close();
				}
				catch (IOException e)
				{
					Log.Debug("Exception in closing " + serverSocket, e);
				}
			}
			if (serverThread != null)
			{
				serverThread.Join();
			}
		}

		/// <summary>Expect connect timeout, because the connection backlog is consumed.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConnectTimeout()
		{
			ConsumeConnectionBacklog();
			try
			{
				fs.ListFiles(new Path("/"), false);
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("connect timed out", e.Message);
			}
		}

		/// <summary>Expect read timeout, because the bogus server never sends a reply.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReadTimeout()
		{
			try
			{
				fs.ListFiles(new Path("/"), false);
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("Read timed out", e.Message);
			}
		}

		/// <summary>
		/// Expect connect timeout on a URL that requires auth, because the connection
		/// backlog is consumed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAuthUrlConnectTimeout()
		{
			ConsumeConnectionBacklog();
			try
			{
				fs.GetDelegationToken("renewer");
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("connect timed out", e.Message);
			}
		}

		/// <summary>
		/// Expect read timeout on a URL that requires auth, because the bogus server
		/// never sends a reply.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAuthUrlReadTimeout()
		{
			try
			{
				fs.GetDelegationToken("renewer");
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("Read timed out", e.Message);
			}
		}

		/// <summary>
		/// After a redirect, expect connect timeout accessing the redirect location,
		/// because the connection backlog is consumed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRedirectConnectTimeout()
		{
			StartSingleTemporaryRedirectResponseThread(true);
			try
			{
				fs.GetFileChecksum(new Path("/file"));
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("connect timed out", e.Message);
			}
		}

		/// <summary>
		/// After a redirect, expect read timeout accessing the redirect location,
		/// because the bogus server never sends a reply.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRedirectReadTimeout()
		{
			StartSingleTemporaryRedirectResponseThread(false);
			try
			{
				fs.GetFileChecksum(new Path("/file"));
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("Read timed out", e.Message);
			}
		}

		/// <summary>
		/// On the second step of two-step write, expect connect timeout accessing the
		/// redirect location, because the connection backlog is consumed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTwoStepWriteConnectTimeout()
		{
			StartSingleTemporaryRedirectResponseThread(true);
			OutputStream os = null;
			try
			{
				os = fs.Create(new Path("/file"));
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("connect timed out", e.Message);
			}
			finally
			{
				IOUtils.Cleanup(Log, os);
			}
		}

		/// <summary>
		/// On the second step of two-step write, expect read timeout accessing the
		/// redirect location, because the bogus server never sends a reply.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTwoStepWriteReadTimeout()
		{
			StartSingleTemporaryRedirectResponseThread(false);
			OutputStream os = null;
			try
			{
				os = fs.Create(new Path("/file"));
				os.Close();
				// must close stream to force reading the HTTP response
				os = null;
				NUnit.Framework.Assert.Fail("expected timeout");
			}
			catch (SocketTimeoutException e)
			{
				NUnit.Framework.Assert.AreEqual("Read timed out", e.Message);
			}
			finally
			{
				IOUtils.Cleanup(Log, os);
			}
		}

		/// <summary>
		/// Starts a background thread that accepts one and only one client connection
		/// on the server socket, sends an HTTP 307 Temporary Redirect response, and
		/// then exits.
		/// </summary>
		/// <remarks>
		/// Starts a background thread that accepts one and only one client connection
		/// on the server socket, sends an HTTP 307 Temporary Redirect response, and
		/// then exits.  This is useful for testing timeouts on the second step of
		/// methods that issue 2 HTTP requests (request 1, redirect, request 2).
		/// For handling the first request, this method sets socket timeout to use the
		/// initial values defined in URLUtils.  Afterwards, it guarantees that the
		/// second request will use a very short timeout.
		/// Optionally, the thread may consume the connection backlog immediately after
		/// receiving its one and only client connection.  This is useful for forcing a
		/// connection timeout on the second request.
		/// On tearDown, open client connections are closed, and the thread is joined.
		/// </remarks>
		/// <param name="consumeConnectionBacklog">
		/// boolean whether or not to consume connection
		/// backlog and thus force a connection timeout on the second request
		/// </param>
		private void StartSingleTemporaryRedirectResponseThread(bool consumeConnectionBacklog
			)
		{
			fs.connectionFactory = URLConnectionFactory.DefaultSystemConnectionFactory;
			serverThread = new _Thread_254(this, consumeConnectionBacklog);
			// Accept one and only one client connection.
			// Immediately setup conditions for subsequent connections.
			// Consume client's HTTP request by reading until EOF or empty line.
			// Write response.
			// Fail the test on any I/O error in the server thread.
			// Clean it all up.
			serverThread.Start();
		}

		private sealed class _Thread_254 : Sharpen.Thread
		{
			public _Thread_254(TestWebHdfsTimeouts _enclosing, bool consumeConnectionBacklog)
			{
				this._enclosing = _enclosing;
				this.consumeConnectionBacklog = consumeConnectionBacklog;
			}

			public override void Run()
			{
				Socket clientSocket = null;
				OutputStream @out = null;
				InputStream @in = null;
				InputStreamReader isr = null;
				BufferedReader br = null;
				try
				{
					clientSocket = this._enclosing.serverSocket.Accept();
					this._enclosing.fs.connectionFactory = this._enclosing.connectionFactory;
					if (consumeConnectionBacklog)
					{
						this._enclosing.ConsumeConnectionBacklog();
					}
					@in = clientSocket.GetInputStream();
					isr = new InputStreamReader(@in);
					br = new BufferedReader(isr);
					for (; ; )
					{
						string line = br.ReadLine();
						if (line == null || line.IsEmpty())
						{
							break;
						}
					}
					@out = clientSocket.GetOutputStream();
					@out.Write(Sharpen.Runtime.GetBytesForString(this._enclosing.TemporaryRedirect(), 
						"UTF-8"));
				}
				catch (IOException e)
				{
					TestWebHdfsTimeouts.Log.Error("unexpected IOException in server thread", e);
					NUnit.Framework.Assert.Fail("unexpected IOException in server thread: " + e);
				}
				finally
				{
					IOUtils.Cleanup(TestWebHdfsTimeouts.Log, br, isr, @in, @out);
					IOUtils.CloseSocket(clientSocket);
				}
			}

			private readonly TestWebHdfsTimeouts _enclosing;

			private readonly bool consumeConnectionBacklog;
		}

		/// <summary>
		/// Consumes the test server's connection backlog by spamming non-blocking
		/// SocketChannel client connections.
		/// </summary>
		/// <remarks>
		/// Consumes the test server's connection backlog by spamming non-blocking
		/// SocketChannel client connections.  We never do anything with these sockets
		/// beyond just initiaing the connections.  The method saves a reference to each
		/// new SocketChannel so that it can be closed during tearDown.  We define a
		/// very small connection backlog, but the OS may silently enforce a larger
		/// minimum backlog than requested.  To work around this, we create far more
		/// client connections than our defined backlog.
		/// </remarks>
		/// <exception cref="System.IO.IOException">thrown for any I/O error</exception>
		private void ConsumeConnectionBacklog()
		{
			for (int i = 0; i < ClientsToConsumeBacklog; ++i)
			{
				SocketChannel client = SocketChannel.Open();
				client.ConfigureBlocking(false);
				client.Connect(nnHttpAddress);
				clients.AddItem(client);
			}
		}

		/// <summary>
		/// Creates an HTTP 307 response with the redirect location set back to the
		/// test server's address.
		/// </summary>
		/// <remarks>
		/// Creates an HTTP 307 response with the redirect location set back to the
		/// test server's address.  HTTP is supposed to terminate newlines with CRLF, so
		/// we hard-code that instead of using the line separator property.
		/// </remarks>
		/// <returns>String HTTP 307 response</returns>
		private string TemporaryRedirect()
		{
			return "HTTP/1.1 307 Temporary Redirect\r\n" + "Location: http://" + NetUtils.GetHostPortString
				(nnHttpAddress) + "\r\n" + "\r\n";
		}
	}
}
