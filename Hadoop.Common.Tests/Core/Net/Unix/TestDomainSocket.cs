using System;
using System.IO;
using Com.Google.Common.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang.Exception;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Net.Unix
{
	public class TestDomainSocket
	{
		private static TemporarySocketDirectory sockDir;

		[BeforeClass]
		public static void Init()
		{
			sockDir = new TemporarySocketDirectory();
			DomainSocket.DisableBindPathValidation();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Shutdown()
		{
			sockDir.Close();
		}

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeTrue(DomainSocket.GetLoadingFailureReason() == null);
		}

		/// <summary>
		/// Test that we can create a socket and close it, even if it hasn't been
		/// opened.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSocketCreateAndClose()
		{
			DomainSocket serv = DomainSocket.BindAndListen(new FilePath(sockDir.GetDir(), "test_sock_create_and_close"
				).GetAbsolutePath());
			serv.Close();
		}

		/// <summary>Test DomainSocket path setting and getting.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSocketPathSetGet()
		{
			NUnit.Framework.Assert.AreEqual("/var/run/hdfs/sock.100", DomainSocket.GetEffectivePath
				("/var/run/hdfs/sock._PORT", 100));
		}

		/// <summary>Test that we get a read result of -1 on EOF.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestSocketReadEof()
		{
			string TestPath = new FilePath(sockDir.GetDir(), "testSocketReadEof").GetAbsolutePath
				();
			DomainSocket serv = DomainSocket.BindAndListen(TestPath);
			ExecutorService exeServ = Executors.NewSingleThreadExecutor();
			Callable<Void> callable = new _Callable_109(serv);
			Future<Void> future = exeServ.Submit(callable);
			DomainSocket conn = DomainSocket.Connect(serv.GetPath());
			Sharpen.Thread.Sleep(50);
			conn.Close();
			serv.Close();
			future.Get(2, TimeUnit.Minutes);
		}

		private sealed class _Callable_109 : Callable<Void>
		{
			public _Callable_109(DomainSocket serv)
			{
				this.serv = serv;
			}

			public Void Call()
			{
				DomainSocket conn;
				try
				{
					conn = serv.Accept();
				}
				catch (IOException e)
				{
					throw new RuntimeException("unexpected IOException", e);
				}
				byte[] buf = new byte[100];
				for (int i = 0; i < buf.Length; i++)
				{
					buf[i] = 0;
				}
				try
				{
					NUnit.Framework.Assert.AreEqual(-1, conn.GetInputStream().Read());
				}
				catch (IOException e)
				{
					throw new RuntimeException("unexpected IOException", e);
				}
				return null;
			}

			private readonly DomainSocket serv;
		}

		/// <summary>
		/// Test that if one thread is blocking in a read or write operation, another
		/// thread can close the socket and stop the accept.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestSocketAcceptAndClose()
		{
			string TestPath = new FilePath(sockDir.GetDir(), "test_sock_accept_and_close").GetAbsolutePath
				();
			DomainSocket serv = DomainSocket.BindAndListen(TestPath);
			ExecutorService exeServ = Executors.NewSingleThreadExecutor();
			Callable<Void> callable = new _Callable_149(serv);
			Future<Void> future = exeServ.Submit(callable);
			Sharpen.Thread.Sleep(500);
			serv.Close();
			future.Get(2, TimeUnit.Minutes);
		}

		private sealed class _Callable_149 : Callable<Void>
		{
			public _Callable_149(DomainSocket serv)
			{
				this.serv = serv;
			}

			public Void Call()
			{
				try
				{
					serv.Accept();
					throw new RuntimeException("expected the accept() to be " + "interrupted and fail"
						);
				}
				catch (AsynchronousCloseException)
				{
					return null;
				}
				catch (IOException e)
				{
					throw new RuntimeException("unexpected IOException", e);
				}
			}

			private readonly DomainSocket serv;
		}

		/// <summary>
		/// Test that we get an AsynchronousCloseException when the DomainSocket
		/// we're using is closed during a read or write operation.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TestAsyncCloseDuringIO(bool closeDuringWrite)
		{
			string TestPath = new FilePath(sockDir.GetDir(), "testAsyncCloseDuringIO(" + closeDuringWrite
				 + ")").GetAbsolutePath();
			DomainSocket serv = DomainSocket.BindAndListen(TestPath);
			ExecutorService exeServ = Executors.NewFixedThreadPool(2);
			Callable<Void> serverCallable = new _Callable_180(serv, closeDuringWrite);
			// The server just continues either writing or reading until someone
			// asynchronously closes the client's socket.  At that point, all our
			// reads return EOF, and writes get a socket error.
			Future<Void> serverFuture = exeServ.Submit(serverCallable);
			DomainSocket clientConn = DomainSocket.Connect(serv.GetPath());
			Callable<Void> clientCallable = new _Callable_213(closeDuringWrite, clientConn);
			// The client writes or reads until another thread
			// asynchronously closes the socket.  At that point, we should
			// get ClosedChannelException, or possibly its subclass
			// AsynchronousCloseException.
			Future<Void> clientFuture = exeServ.Submit(clientCallable);
			Sharpen.Thread.Sleep(500);
			clientConn.Close();
			serv.Close();
			clientFuture.Get(2, TimeUnit.Minutes);
			serverFuture.Get(2, TimeUnit.Minutes);
		}

		private sealed class _Callable_180 : Callable<Void>
		{
			public _Callable_180(DomainSocket serv, bool closeDuringWrite)
			{
				this.serv = serv;
				this.closeDuringWrite = closeDuringWrite;
			}

			public Void Call()
			{
				DomainSocket serverConn = null;
				try
				{
					serverConn = serv.Accept();
					byte[] buf = new byte[100];
					for (int i = 0; i < buf.Length; i++)
					{
						buf[i] = 0;
					}
					if (closeDuringWrite)
					{
						try
						{
							while (true)
							{
								serverConn.GetOutputStream().Write(buf);
							}
						}
						catch (IOException)
						{
						}
					}
					else
					{
						do
						{
						}
						while (serverConn.GetInputStream().Read(buf, 0, buf.Length) != -1);
					}
				}
				catch (IOException e)
				{
					throw new RuntimeException("unexpected IOException", e);
				}
				finally
				{
					IOUtils.Cleanup(DomainSocket.Log, serverConn);
				}
				return null;
			}

			private readonly DomainSocket serv;

			private readonly bool closeDuringWrite;
		}

		private sealed class _Callable_213 : Callable<Void>
		{
			public _Callable_213(bool closeDuringWrite, DomainSocket clientConn)
			{
				this.closeDuringWrite = closeDuringWrite;
				this.clientConn = clientConn;
			}

			public Void Call()
			{
				byte[] buf = new byte[100];
				for (int i = 0; i < buf.Length; i++)
				{
					buf[i] = 0;
				}
				try
				{
					if (closeDuringWrite)
					{
						while (true)
						{
							clientConn.GetOutputStream().Write(buf);
						}
					}
					else
					{
						while (true)
						{
							clientConn.GetInputStream().Read(buf, 0, buf.Length);
						}
					}
				}
				catch (ClosedChannelException)
				{
					return null;
				}
				catch (IOException e)
				{
					throw new RuntimeException("unexpected IOException", e);
				}
			}

			private readonly bool closeDuringWrite;

			private readonly DomainSocket clientConn;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAsyncCloseDuringWrite()
		{
			TestAsyncCloseDuringIO(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAsyncCloseDuringRead()
		{
			TestAsyncCloseDuringIO(false);
		}

		/// <summary>Test that attempting to connect to an invalid path doesn't work.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInvalidOperations()
		{
			try
			{
				DomainSocket.Connect(new FilePath(sockDir.GetDir(), "test_sock_invalid_operation"
					).GetAbsolutePath());
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("connect(2) error: ", e);
			}
		}

		/// <summary>Test setting some server options.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestServerOptions()
		{
			string TestPath = new FilePath(sockDir.GetDir(), "test_sock_server_options").GetAbsolutePath
				();
			DomainSocket serv = DomainSocket.BindAndListen(TestPath);
			try
			{
				// Let's set a new receive buffer size
				int bufSize = serv.GetAttribute(DomainSocket.ReceiveBufferSize);
				int newBufSize = bufSize / 2;
				serv.SetAttribute(DomainSocket.ReceiveBufferSize, newBufSize);
				int nextBufSize = serv.GetAttribute(DomainSocket.ReceiveBufferSize);
				NUnit.Framework.Assert.AreEqual(newBufSize, nextBufSize);
				// Let's set a server timeout
				int newTimeout = 1000;
				serv.SetAttribute(DomainSocket.ReceiveTimeout, newTimeout);
				int nextTimeout = serv.GetAttribute(DomainSocket.ReceiveTimeout);
				NUnit.Framework.Assert.AreEqual(newTimeout, nextTimeout);
				try
				{
					serv.Accept();
					NUnit.Framework.Assert.Fail("expected the accept() to time out and fail");
				}
				catch (SocketTimeoutException e)
				{
					GenericTestUtils.AssertExceptionContains("accept(2) error: ", e);
				}
			}
			finally
			{
				serv.Close();
				NUnit.Framework.Assert.IsFalse(serv.IsOpen());
			}
		}

		/// <summary>A Throwable representing success.</summary>
		/// <remarks>
		/// A Throwable representing success.
		/// We can't use null to represent this, because you cannot insert null into
		/// ArrayBlockingQueue.
		/// </remarks>
		[System.Serializable]
		internal class Success : Exception
		{
			private const long serialVersionUID = 1L;
		}

		internal interface WriteStrategy
		{
			/// <summary>Initialize a WriteStrategy object from a Socket.</summary>
			/// <exception cref="System.IO.IOException"/>
			void Init(DomainSocket s);

			/// <summary>Write some bytes.</summary>
			/// <exception cref="System.IO.IOException"/>
			void Write(byte[] b);
		}

		internal class OutputStreamWriteStrategy : TestDomainSocket.WriteStrategy
		{
			private OutputStream outs = null;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Init(DomainSocket s)
			{
				outs = s.GetOutputStream();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(byte[] b)
			{
				outs.Write(b);
			}
		}

		internal abstract class ReadStrategy
		{
			/// <summary>Initialize a ReadStrategy object from a DomainSocket.</summary>
			/// <exception cref="System.IO.IOException"/>
			public abstract void Init(DomainSocket s);

			/// <summary>Read some bytes.</summary>
			/// <exception cref="System.IO.IOException"/>
			public abstract int Read(byte[] b, int off, int length);

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFully(byte[] buf, int off, int len)
			{
				int toRead = len;
				while (toRead > 0)
				{
					int ret = Read(buf, off, toRead);
					if (ret < 0)
					{
						throw new IOException("Premature EOF from inputStream");
					}
					toRead -= ret;
					off += ret;
				}
			}
		}

		internal class InputStreamReadStrategy : TestDomainSocket.ReadStrategy
		{
			private InputStream ins = null;

			/// <exception cref="System.IO.IOException"/>
			public override void Init(DomainSocket s)
			{
				ins = s.GetInputStream();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int length)
			{
				return ins.Read(b, off, length);
			}
		}

		internal class DirectByteBufferReadStrategy : TestDomainSocket.ReadStrategy
		{
			private DomainSocket.DomainChannel ch = null;

			/// <exception cref="System.IO.IOException"/>
			public override void Init(DomainSocket s)
			{
				ch = s.GetChannel();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int length)
			{
				ByteBuffer buf = ByteBuffer.AllocateDirect(b.Length);
				int nread = ch.Read(buf);
				if (nread < 0)
				{
					return nread;
				}
				buf.Flip();
				buf.Get(b, off, nread);
				return nread;
			}
		}

		internal class ArrayBackedByteBufferReadStrategy : TestDomainSocket.ReadStrategy
		{
			private DomainSocket.DomainChannel ch = null;

			/// <exception cref="System.IO.IOException"/>
			public override void Init(DomainSocket s)
			{
				ch = s.GetChannel();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int length)
			{
				ByteBuffer buf = ByteBuffer.Wrap(b);
				int nread = ch.Read(buf);
				if (nread < 0)
				{
					return nread;
				}
				buf.Flip();
				buf.Get(b, off, nread);
				return nread;
			}
		}

		/// <summary>Test a simple client/server interaction.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void TestClientServer1(Type writeStrategyClass, Type readStrategyClass
			, DomainSocket[] preConnectedSockets)
		{
			string TestPath = new FilePath(sockDir.GetDir(), "test_sock_client_server1").GetAbsolutePath
				();
			byte[] clientMsg1 = new byte[] { unchecked((int)(0x1)), unchecked((int)(0x2)), unchecked(
				(int)(0x3)), unchecked((int)(0x4)), unchecked((int)(0x5)), unchecked((int)(0x6))
				 };
			byte[] serverMsg1 = new byte[] { unchecked((int)(0x9)), unchecked((int)(0x8)), unchecked(
				(int)(0x7)), unchecked((int)(0x6)), unchecked((int)(0x5)) };
			byte clientMsg2 = unchecked((int)(0x45));
			ArrayBlockingQueue<Exception> threadResults = new ArrayBlockingQueue<Exception>(2
				);
			DomainSocket serv = (preConnectedSockets != null) ? null : DomainSocket.BindAndListen
				(TestPath);
			Sharpen.Thread serverThread = new _Thread_435(preConnectedSockets, serv, clientMsg1
				, readStrategyClass, writeStrategyClass, serverMsg1, clientMsg2, threadResults);
			// Run server
			serverThread.Start();
			Sharpen.Thread clientThread = new _Thread_463(preConnectedSockets, TestPath, writeStrategyClass
				, clientMsg1, readStrategyClass, serverMsg1, clientMsg2, threadResults);
			clientThread.Start();
			for (int i = 0; i < 2; i++)
			{
				Exception t = threadResults.Take();
				if (!(t is TestDomainSocket.Success))
				{
					NUnit.Framework.Assert.Fail(t.Message + ExceptionUtils.GetStackTrace(t));
				}
			}
			serverThread.Join(120000);
			clientThread.Join(120000);
			if (serv != null)
			{
				serv.Close();
			}
		}

		private sealed class _Thread_435 : Sharpen.Thread
		{
			public _Thread_435(DomainSocket[] preConnectedSockets, DomainSocket serv, byte[] 
				clientMsg1, Type readStrategyClass, Type writeStrategyClass, byte[] serverMsg1, 
				byte clientMsg2, ArrayBlockingQueue<Exception> threadResults)
			{
				this.preConnectedSockets = preConnectedSockets;
				this.serv = serv;
				this.clientMsg1 = clientMsg1;
				this.readStrategyClass = readStrategyClass;
				this.writeStrategyClass = writeStrategyClass;
				this.serverMsg1 = serverMsg1;
				this.clientMsg2 = clientMsg2;
				this.threadResults = threadResults;
			}

			public override void Run()
			{
				DomainSocket conn = null;
				try
				{
					conn = preConnectedSockets != null ? preConnectedSockets[0] : serv.Accept();
					byte[] in1 = new byte[clientMsg1.Length];
					TestDomainSocket.ReadStrategy reader = System.Activator.CreateInstance(readStrategyClass
						);
					reader.Init(conn);
					reader.ReadFully(in1, 0, in1.Length);
					NUnit.Framework.Assert.IsTrue(Arrays.Equals(clientMsg1, in1));
					TestDomainSocket.WriteStrategy writer = System.Activator.CreateInstance(writeStrategyClass
						);
					writer.Init(conn);
					writer.Write(serverMsg1);
					InputStream connInputStream = conn.GetInputStream();
					int in2 = connInputStream.Read();
					NUnit.Framework.Assert.AreEqual((int)clientMsg2, in2);
					conn.Close();
				}
				catch (Exception e)
				{
					threadResults.AddItem(e);
					NUnit.Framework.Assert.Fail(e.Message);
				}
				threadResults.AddItem(new TestDomainSocket.Success());
			}

			private readonly DomainSocket[] preConnectedSockets;

			private readonly DomainSocket serv;

			private readonly byte[] clientMsg1;

			private readonly Type readStrategyClass;

			private readonly Type writeStrategyClass;

			private readonly byte[] serverMsg1;

			private readonly byte clientMsg2;

			private readonly ArrayBlockingQueue<Exception> threadResults;
		}

		private sealed class _Thread_463 : Sharpen.Thread
		{
			public _Thread_463(DomainSocket[] preConnectedSockets, string TestPath, Type writeStrategyClass
				, byte[] clientMsg1, Type readStrategyClass, byte[] serverMsg1, byte clientMsg2, 
				ArrayBlockingQueue<Exception> threadResults)
			{
				this.preConnectedSockets = preConnectedSockets;
				this.TestPath = TestPath;
				this.writeStrategyClass = writeStrategyClass;
				this.clientMsg1 = clientMsg1;
				this.readStrategyClass = readStrategyClass;
				this.serverMsg1 = serverMsg1;
				this.clientMsg2 = clientMsg2;
				this.threadResults = threadResults;
			}

			public override void Run()
			{
				try
				{
					DomainSocket client = preConnectedSockets != null ? preConnectedSockets[1] : DomainSocket
						.Connect(TestPath);
					TestDomainSocket.WriteStrategy writer = System.Activator.CreateInstance(writeStrategyClass
						);
					writer.Init(client);
					writer.Write(clientMsg1);
					TestDomainSocket.ReadStrategy reader = System.Activator.CreateInstance(readStrategyClass
						);
					reader.Init(client);
					byte[] in1 = new byte[serverMsg1.Length];
					reader.ReadFully(in1, 0, in1.Length);
					NUnit.Framework.Assert.IsTrue(Arrays.Equals(serverMsg1, in1));
					OutputStream clientOutputStream = client.GetOutputStream();
					clientOutputStream.Write(clientMsg2);
					client.Close();
				}
				catch (Exception e)
				{
					threadResults.AddItem(e);
				}
				threadResults.AddItem(new TestDomainSocket.Success());
			}

			private readonly DomainSocket[] preConnectedSockets;

			private readonly string TestPath;

			private readonly Type writeStrategyClass;

			private readonly byte[] clientMsg1;

			private readonly Type readStrategyClass;

			private readonly byte[] serverMsg1;

			private readonly byte clientMsg2;

			private readonly ArrayBlockingQueue<Exception> threadResults;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientServerOutStreamInStream()
		{
			TestClientServer1(typeof(TestDomainSocket.OutputStreamWriteStrategy), typeof(TestDomainSocket.InputStreamReadStrategy
				), null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientServerOutStreamInStreamWithSocketpair()
		{
			TestClientServer1(typeof(TestDomainSocket.OutputStreamWriteStrategy), typeof(TestDomainSocket.InputStreamReadStrategy
				), DomainSocket.Socketpair());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientServerOutStreamInDbb()
		{
			TestClientServer1(typeof(TestDomainSocket.OutputStreamWriteStrategy), typeof(TestDomainSocket.DirectByteBufferReadStrategy
				), null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientServerOutStreamInDbbWithSocketpair()
		{
			TestClientServer1(typeof(TestDomainSocket.OutputStreamWriteStrategy), typeof(TestDomainSocket.DirectByteBufferReadStrategy
				), DomainSocket.Socketpair());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientServerOutStreamInAbb()
		{
			TestClientServer1(typeof(TestDomainSocket.OutputStreamWriteStrategy), typeof(TestDomainSocket.ArrayBackedByteBufferReadStrategy
				), null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientServerOutStreamInAbbWithSocketpair()
		{
			TestClientServer1(typeof(TestDomainSocket.OutputStreamWriteStrategy), typeof(TestDomainSocket.ArrayBackedByteBufferReadStrategy
				), DomainSocket.Socketpair());
		}

		private class PassedFile
		{
			private readonly int idx;

			private readonly byte[] contents;

			private FileInputStream fis;

			/// <exception cref="System.IO.IOException"/>
			public PassedFile(int idx)
			{
				this.idx = idx;
				this.contents = new byte[] { unchecked((byte)(idx % 127)) };
				Files.Write(contents, new FilePath(GetPath()));
				this.fis = new FileInputStream(GetPath());
			}

			public virtual string GetPath()
			{
				return new FilePath(sockDir.GetDir(), "passed_file" + idx).GetAbsolutePath();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileInputStream GetInputStream()
			{
				return fis;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Cleanup()
			{
				new FilePath(GetPath()).Delete();
				fis.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void CheckInputStream(FileInputStream fis)
			{
				byte[] buf = new byte[contents.Length];
				IOUtils.ReadFully(fis, buf, 0, buf.Length);
				Arrays.Equals(contents, buf);
			}

			~PassedFile()
			{
				try
				{
					Cleanup();
				}
				catch
				{
				}
			}
			// ignore
		}

		/// <summary>Test file descriptor passing.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestFdPassing()
		{
			string TestPath = new FilePath(sockDir.GetDir(), "test_sock").GetAbsolutePath();
			byte[] clientMsg1 = new byte[] { unchecked((int)(0x11)), unchecked((int)(0x22)), 
				unchecked((int)(0x33)), unchecked((int)(0x44)), unchecked((int)(0x55)), unchecked(
				(int)(0x66)) };
			byte[] serverMsg1 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x30)), 
				unchecked((int)(0x32)), unchecked((int)(0x34)), unchecked((int)(0x31)), unchecked(
				(int)(0x33)), unchecked((int)(0x44)), unchecked((int)(0x1)), unchecked((int)(0x1
				)), unchecked((int)(0x1)), unchecked((int)(0x1)), unchecked((int)(0x1)) };
			ArrayBlockingQueue<System.Exception> threadResults = new ArrayBlockingQueue<System.Exception
				>(2);
			DomainSocket serv = DomainSocket.BindAndListen(TestPath);
			TestDomainSocket.PassedFile[] passedFiles = new TestDomainSocket.PassedFile[] { new 
				TestDomainSocket.PassedFile(1), new TestDomainSocket.PassedFile(2) };
			FileDescriptor[] passedFds = new FileDescriptor[passedFiles.Length];
			for (int i = 0; i < passedFiles.Length; i++)
			{
				passedFds[i] = passedFiles[i].GetInputStream().GetFD();
			}
			Sharpen.Thread serverThread = new _Thread_597(serv, clientMsg1, passedFds, serverMsg1
				, threadResults);
			// Run server
			serverThread.Start();
			Sharpen.Thread clientThread = new _Thread_620(TestPath, clientMsg1, serverMsg1, passedFds
				, passedFiles, threadResults);
			clientThread.Start();
			for (int i_1 = 0; i_1 < 2; i_1++)
			{
				System.Exception t = threadResults.Take();
				if (!(t is TestDomainSocket.Success))
				{
					NUnit.Framework.Assert.Fail(t.Message + ExceptionUtils.GetStackTrace(t));
				}
			}
			serverThread.Join(120000);
			clientThread.Join(120000);
			serv.Close();
			foreach (TestDomainSocket.PassedFile pf in passedFiles)
			{
				pf.Cleanup();
			}
		}

		private sealed class _Thread_597 : Sharpen.Thread
		{
			public _Thread_597(DomainSocket serv, byte[] clientMsg1, FileDescriptor[] passedFds
				, byte[] serverMsg1, ArrayBlockingQueue<System.Exception> threadResults)
			{
				this.serv = serv;
				this.clientMsg1 = clientMsg1;
				this.passedFds = passedFds;
				this.serverMsg1 = serverMsg1;
				this.threadResults = threadResults;
			}

			public override void Run()
			{
				DomainSocket conn = null;
				try
				{
					conn = serv.Accept();
					byte[] in1 = new byte[clientMsg1.Length];
					InputStream connInputStream = conn.GetInputStream();
					IOUtils.ReadFully(connInputStream, in1, 0, in1.Length);
					NUnit.Framework.Assert.IsTrue(Arrays.Equals(clientMsg1, in1));
					DomainSocket domainConn = (DomainSocket)conn;
					domainConn.SendFileDescriptors(passedFds, serverMsg1, 0, serverMsg1.Length);
					conn.Close();
				}
				catch (System.Exception e)
				{
					threadResults.AddItem(e);
					NUnit.Framework.Assert.Fail(e.Message);
				}
				threadResults.AddItem(new TestDomainSocket.Success());
			}

			private readonly DomainSocket serv;

			private readonly byte[] clientMsg1;

			private readonly FileDescriptor[] passedFds;

			private readonly byte[] serverMsg1;

			private readonly ArrayBlockingQueue<System.Exception> threadResults;
		}

		private sealed class _Thread_620 : Sharpen.Thread
		{
			public _Thread_620(string TestPath, byte[] clientMsg1, byte[] serverMsg1, FileDescriptor
				[] passedFds, TestDomainSocket.PassedFile[] passedFiles, ArrayBlockingQueue<System.Exception
				> threadResults)
			{
				this.TestPath = TestPath;
				this.clientMsg1 = clientMsg1;
				this.serverMsg1 = serverMsg1;
				this.passedFds = passedFds;
				this.passedFiles = passedFiles;
				this.threadResults = threadResults;
			}

			public override void Run()
			{
				try
				{
					DomainSocket client = DomainSocket.Connect(TestPath);
					OutputStream clientOutputStream = client.GetOutputStream();
					InputStream clientInputStream = client.GetInputStream();
					clientOutputStream.Write(clientMsg1);
					DomainSocket domainConn = (DomainSocket)client;
					byte[] in1 = new byte[serverMsg1.Length];
					FileInputStream[] recvFis = new FileInputStream[passedFds.Length];
					int r = domainConn.RecvFileInputStreams(recvFis, in1, 0, in1.Length - 1);
					NUnit.Framework.Assert.IsTrue(r > 0);
					IOUtils.ReadFully(clientInputStream, in1, r, in1.Length - r);
					NUnit.Framework.Assert.IsTrue(Arrays.Equals(serverMsg1, in1));
					for (int i = 0; i < passedFds.Length; i++)
					{
						NUnit.Framework.Assert.IsNotNull(recvFis[i]);
						passedFiles[i].CheckInputStream(recvFis[i]);
					}
					foreach (FileInputStream fis in recvFis)
					{
						fis.Close();
					}
					client.Close();
				}
				catch (System.Exception e)
				{
					threadResults.AddItem(e);
				}
				threadResults.AddItem(new TestDomainSocket.Success());
			}

			private readonly string TestPath;

			private readonly byte[] clientMsg1;

			private readonly byte[] serverMsg1;

			private readonly FileDescriptor[] passedFds;

			private readonly TestDomainSocket.PassedFile[] passedFiles;

			private readonly ArrayBlockingQueue<System.Exception> threadResults;
		}

		/// <summary>Run validateSocketPathSecurity</summary>
		/// <param name="str">The path to validate</param>
		/// <param name="prefix">A prefix to skip validation for</param>
		/// <exception cref="System.IO.IOException"/>
		private static void TestValidateSocketPath(string str, string prefix)
		{
			int skipComponents = 1;
			FilePath prefixFile = new FilePath(prefix);
			while (true)
			{
				prefixFile = prefixFile.GetParentFile();
				if (prefixFile == null)
				{
					break;
				}
				skipComponents++;
			}
			DomainSocket.ValidateSocketPathSecurity0(str, skipComponents);
		}

		/// <summary>Test file descriptor path security.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestFdPassingPathSecurity()
		{
			TemporarySocketDirectory tmp = new TemporarySocketDirectory();
			try
			{
				string prefix = tmp.GetDir().GetAbsolutePath();
				Shell.ExecCommand(new string[] { "mkdir", "-p", prefix + "/foo/bar/baz" });
				Shell.ExecCommand(new string[] { "chmod", "0700", prefix + "/foo/bar/baz" });
				Shell.ExecCommand(new string[] { "chmod", "0700", prefix + "/foo/bar" });
				Shell.ExecCommand(new string[] { "chmod", "0707", prefix + "/foo" });
				Shell.ExecCommand(new string[] { "mkdir", "-p", prefix + "/q1/q2" });
				Shell.ExecCommand(new string[] { "chmod", "0700", prefix + "/q1" });
				Shell.ExecCommand(new string[] { "chmod", "0700", prefix + "/q1/q2" });
				TestValidateSocketPath(prefix + "/q1/q2", prefix);
				try
				{
					TestValidateSocketPath(prefix + "/foo/bar/baz", prefix);
				}
				catch (IOException e)
				{
					GenericTestUtils.AssertExceptionContains("/foo' is world-writable.  " + "Its permissions are 0707.  Please fix this or select a "
						 + "different socket path.", e);
				}
				try
				{
					TestValidateSocketPath(prefix + "/nope", prefix);
				}
				catch (IOException e)
				{
					GenericTestUtils.AssertExceptionContains("failed to stat a path " + "component: "
						, e);
				}
				// Root should be secure
				DomainSocket.ValidateSocketPathSecurity0("/foo", 1);
			}
			finally
			{
				tmp.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShutdown()
		{
			AtomicInteger bytesRead = new AtomicInteger(0);
			AtomicBoolean failed = new AtomicBoolean(false);
			DomainSocket[] socks = DomainSocket.Socketpair();
			Runnable reader = new _Runnable_737(socks, bytesRead, failed);
			Sharpen.Thread readerThread = new Sharpen.Thread(reader);
			readerThread.Start();
			socks[0].GetOutputStream().Write(1);
			socks[0].GetOutputStream().Write(2);
			socks[0].GetOutputStream().Write(3);
			NUnit.Framework.Assert.IsTrue(readerThread.IsAlive());
			socks[0].Shutdown();
			readerThread.Join();
			NUnit.Framework.Assert.IsFalse(failed.Get());
			NUnit.Framework.Assert.AreEqual(3, bytesRead.Get());
			IOUtils.Cleanup(null, socks);
		}

		private sealed class _Runnable_737 : Runnable
		{
			public _Runnable_737(DomainSocket[] socks, AtomicInteger bytesRead, AtomicBoolean
				 failed)
			{
				this.socks = socks;
				this.bytesRead = bytesRead;
				this.failed = failed;
			}

			public void Run()
			{
				while (true)
				{
					try
					{
						int ret = socks[1].GetInputStream().Read();
						if (ret == -1)
						{
							return;
						}
						bytesRead.AddAndGet(1);
					}
					catch (IOException e)
					{
						DomainSocket.Log.Error("reader error", e);
						failed.Set(true);
						return;
					}
				}
			}

			private readonly DomainSocket[] socks;

			private readonly AtomicInteger bytesRead;

			private readonly AtomicBoolean failed;
		}
	}
}
