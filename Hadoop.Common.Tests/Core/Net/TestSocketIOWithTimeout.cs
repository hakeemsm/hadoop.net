using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// This tests timout out from SocketInputStream and
	/// SocketOutputStream using pipes.
	/// </summary>
	/// <remarks>
	/// This tests timout out from SocketInputStream and
	/// SocketOutputStream using pipes.
	/// Normal read and write using these streams are tested by pretty much
	/// every DFS unit test.
	/// </remarks>
	public class TestSocketIOWithTimeout
	{
		internal static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Net.TestSocketIOWithTimeout
			));

		private static int Timeout = 1 * 1000;

		private static string TestString = "1234567890";

		private MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext
			();

		private static readonly int PageSize = (int)NativeIO.POSIX.GetCacheManipulator().
			GetOperatingSystemPageSize();

		/// <exception cref="System.IO.IOException"/>
		private void DoIO(InputStream @in, OutputStream @out, int expectedTimeout)
		{
			/* Keep on writing or reading until we get SocketTimeoutException.
			* It expects this exception to occur within 100 millis of TIMEOUT.
			*/
			byte[] buf = new byte[PageSize + 19];
			while (true)
			{
				long start = Time.Now();
				try
				{
					if (@in != null)
					{
						@in.Read(buf);
					}
					else
					{
						@out.Write(buf);
					}
				}
				catch (SocketTimeoutException e)
				{
					long diff = Time.Now() - start;
					Log.Info("Got SocketTimeoutException as expected after " + diff + " millis : " + 
						e.Message);
					Assert.True(Math.Abs(expectedTimeout - diff) <= TestNetUtils.TimeFudgeMillis
						);
					break;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSocketIOWithTimeout()
		{
			// first open pipe:
			Pipe pipe = Pipe.Open();
			Pipe.SourceChannel source = pipe.Source();
			Pipe.SinkChannel sink = pipe.Sink();
			try
			{
				InputStream @in = new SocketInputStream(source, Timeout);
				OutputStream @out = new SocketOutputStream(sink, Timeout);
				byte[] writeBytes = Sharpen.Runtime.GetBytesForString(TestString);
				byte[] readBytes = new byte[writeBytes.Length];
				byte byteWithHighBit = unchecked((byte)unchecked((int)(0x80)));
				@out.Write(writeBytes);
				@out.Write(byteWithHighBit);
				DoIO(null, @out, Timeout);
				@in.Read(readBytes);
				Assert.True(Arrays.Equals(writeBytes, readBytes));
				Assert.Equal(byteWithHighBit & unchecked((int)(0xff)), @in.Read
					());
				DoIO(@in, null, Timeout);
				// Change timeout on the read side.
				((SocketInputStream)@in).SetTimeout(Timeout * 2);
				DoIO(@in, null, Timeout * 2);
				/*
				* Verify that it handles interrupted threads properly.
				* Use a large timeout and expect the thread to return quickly
				* upon interruption.
				*/
				((SocketInputStream)@in).SetTimeout(0);
				MultithreadedTestUtil.TestingThread thread = new _TestingThread_121(@in, ctx);
				ctx.AddThread(thread);
				ctx.StartThreads();
				// If the thread is interrupted before it calls read()
				// then it throws ClosedByInterruptException due to
				// some Java quirk. Waiting for it to call read()
				// gets it into select(), so we get the expected
				// InterruptedIOException.
				Sharpen.Thread.Sleep(1000);
				thread.Interrupt();
				ctx.Stop();
				//make sure the channels are still open
				Assert.True(source.IsOpen());
				Assert.True(sink.IsOpen());
				// Nevertheless, the output stream is closed, because
				// a partial write may have succeeded (see comment in
				// SocketOutputStream#write(byte[]), int, int)
				// This portion of the test cannot pass on Windows due to differences in
				// behavior of partial writes.  Windows appears to buffer large amounts of
				// written data and send it all atomically, thus making it impossible to
				// simulate a partial write scenario.  Attempts were made to switch the
				// test from using a pipe to a network socket and also to use larger and
				// larger buffers in doIO.  Nothing helped the situation though.
				if (!Shell.Windows)
				{
					try
					{
						@out.Write(1);
						NUnit.Framework.Assert.Fail("Did not throw");
					}
					catch (IOException ioe)
					{
						GenericTestUtils.AssertExceptionContains("stream is closed", ioe);
					}
				}
				@out.Close();
				NUnit.Framework.Assert.IsFalse(sink.IsOpen());
				// close sink and expect -1 from source.read()
				Assert.Equal(-1, @in.Read());
				// make sure close() closes the underlying channel.
				@in.Close();
				NUnit.Framework.Assert.IsFalse(source.IsOpen());
			}
			finally
			{
				if (source != null)
				{
					source.Close();
				}
				if (sink != null)
				{
					sink.Close();
				}
			}
		}

		private sealed class _TestingThread_121 : MultithreadedTestUtil.TestingThread
		{
			public _TestingThread_121(InputStream @in, MultithreadedTestUtil.TestContext baseArg1
				)
				: base(baseArg1)
			{
				this.@in = @in;
			}

			/// <exception cref="System.Exception"/>
			public override void DoWork()
			{
				try
				{
					@in.Read();
					NUnit.Framework.Assert.Fail("Did not fail with interrupt");
				}
				catch (ThreadInterruptedException ste)
				{
					Org.Apache.Hadoop.Net.TestSocketIOWithTimeout.Log.Info("Got expection while reading as expected : "
						 + ste.Message);
				}
			}

			private readonly InputStream @in;
		}
	}
}
