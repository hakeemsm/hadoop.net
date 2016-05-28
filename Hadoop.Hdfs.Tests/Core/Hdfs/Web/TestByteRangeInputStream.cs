using System;
using System.IO;
using Com.Google.Common.Net;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestByteRangeInputStream
	{
		private class ByteRangeInputStreamImpl : ByteRangeInputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public ByteRangeInputStreamImpl(TestByteRangeInputStream _enclosing, ByteRangeInputStream.URLOpener
				 o, ByteRangeInputStream.URLOpener r)
				: base(o, r)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override Uri GetResolvedUrl(HttpURLConnection connection)
			{
				return new Uri("http://resolvedurl/");
			}

			private readonly TestByteRangeInputStream _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private ByteRangeInputStream.URLOpener GetMockURLOpener(Uri url)
		{
			ByteRangeInputStream.URLOpener opener = Org.Mockito.Mockito.Mock<ByteRangeInputStream.URLOpener
				>(Org.Mockito.Mockito.CallsRealMethods);
			opener.SetURL(url);
			Org.Mockito.Mockito.DoReturn(GetMockConnection("65535")).When(opener).Connect(Matchers.AnyLong
				(), Matchers.AnyBoolean());
			return opener;
		}

		/// <exception cref="System.IO.IOException"/>
		private HttpURLConnection GetMockConnection(string length)
		{
			HttpURLConnection mockConnection = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.DoReturn(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString
				("asdf"))).When(mockConnection).GetInputStream();
			Org.Mockito.Mockito.DoReturn(length).When(mockConnection).GetHeaderField(HttpHeaders
				.ContentLength);
			return mockConnection;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestByteRange()
		{
			ByteRangeInputStream.URLOpener oMock = GetMockURLOpener(new Uri("http://test"));
			ByteRangeInputStream.URLOpener rMock = GetMockURLOpener(null);
			ByteRangeInputStream bris = new TestByteRangeInputStream.ByteRangeInputStreamImpl
				(this, oMock, rMock);
			bris.Seek(0);
			NUnit.Framework.Assert.AreEqual("getPos wrong", 0, bris.GetPos());
			bris.Read();
			NUnit.Framework.Assert.AreEqual("Initial call made incorrectly (offset check)", 0
				, bris.startPos);
			NUnit.Framework.Assert.AreEqual("getPos should return 1 after reading one byte", 
				1, bris.GetPos());
			Org.Mockito.Mockito.Verify(oMock, Org.Mockito.Mockito.Times(1)).Connect(0, false);
			bris.Read();
			NUnit.Framework.Assert.AreEqual("getPos should return 2 after reading two bytes", 
				2, bris.GetPos());
			// No additional connections should have been made (no seek)
			Org.Mockito.Mockito.Verify(oMock, Org.Mockito.Mockito.Times(1)).Connect(0, false);
			rMock.SetURL(new Uri("http://resolvedurl/"));
			bris.Seek(100);
			bris.Read();
			NUnit.Framework.Assert.AreEqual("Seek to 100 bytes made incorrectly (offset Check)"
				, 100, bris.startPos);
			NUnit.Framework.Assert.AreEqual("getPos should return 101 after reading one byte"
				, 101, bris.GetPos());
			Org.Mockito.Mockito.Verify(rMock, Org.Mockito.Mockito.Times(1)).Connect(100, true
				);
			bris.Seek(101);
			bris.Read();
			// Seek to 101 should not result in another request
			Org.Mockito.Mockito.Verify(rMock, Org.Mockito.Mockito.Times(1)).Connect(100, true
				);
			Org.Mockito.Mockito.Verify(rMock, Org.Mockito.Mockito.Times(0)).Connect(101, true
				);
			bris.Seek(2500);
			bris.Read();
			NUnit.Framework.Assert.AreEqual("Seek to 2500 bytes made incorrectly (offset Check)"
				, 2500, bris.startPos);
			Org.Mockito.Mockito.DoReturn(GetMockConnection(null)).When(rMock).Connect(Matchers.AnyLong
				(), Matchers.AnyBoolean());
			bris.Seek(500);
			try
			{
				bris.Read();
				NUnit.Framework.Assert.Fail("Exception should be thrown when content-length is not given"
					);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("Incorrect response message: " + e.Message, e.Message
					.StartsWith(HttpHeaders.ContentLength + " is missing: "));
			}
			bris.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPropagatedClose()
		{
			ByteRangeInputStream bris = Org.Mockito.Mockito.Mock<ByteRangeInputStream>(Org.Mockito.Mockito
				.CallsRealMethods);
			InputStream mockStream = Org.Mockito.Mockito.Mock<InputStream>();
			Org.Mockito.Mockito.DoReturn(mockStream).When(bris).OpenInputStream();
			Whitebox.SetInternalState(bris, "status", ByteRangeInputStream.StreamStatus.Seek);
			int brisOpens = 0;
			int brisCloses = 0;
			int isCloses = 0;
			// first open, shouldn't close underlying stream
			bris.GetInputStream();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(++brisOpens)).OpenInputStream
				();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(isCloses)).Close
				();
			// stream is open, shouldn't close underlying stream
			bris.GetInputStream();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisOpens)).OpenInputStream
				();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(isCloses)).Close
				();
			// seek forces a reopen, should close underlying stream
			bris.Seek(1);
			bris.GetInputStream();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(++brisOpens)).OpenInputStream
				();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(++isCloses)).Close
				();
			// verify that the underlying stream isn't closed after a seek
			// ie. the state was correctly updated
			bris.GetInputStream();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisOpens)).OpenInputStream
				();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(isCloses)).Close
				();
			// seeking to same location should be a no-op
			bris.Seek(1);
			bris.GetInputStream();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisOpens)).OpenInputStream
				();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(isCloses)).Close
				();
			// close should of course close
			bris.Close();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(++brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(++isCloses)).Close
				();
			// it's already closed, underlying stream should not close
			bris.Close();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(++brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(isCloses)).Close
				();
			// it's closed, don't reopen it
			bool errored = false;
			try
			{
				bris.GetInputStream();
			}
			catch (IOException e)
			{
				errored = true;
				NUnit.Framework.Assert.AreEqual("Stream closed", e.Message);
			}
			finally
			{
				NUnit.Framework.Assert.IsTrue("Read a closed steam", errored);
			}
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisOpens)).OpenInputStream
				();
			Org.Mockito.Mockito.Verify(bris, Org.Mockito.Mockito.Times(brisCloses)).Close();
			Org.Mockito.Mockito.Verify(mockStream, Org.Mockito.Mockito.Times(isCloses)).Close
				();
		}
	}
}
