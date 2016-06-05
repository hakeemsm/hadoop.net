using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	/// <summary>Test that the Fetcher does what we expect it to.</summary>
	public class TestFetcher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFetcher));

		internal JobConf job = null;

		internal JobConf jobWithRetry = null;

		internal TaskAttemptID id = null;

		internal ShuffleSchedulerImpl<Text, Text> ss = null;

		internal MergeManagerImpl<Text, Text> mm = null;

		internal Reporter r = null;

		internal ShuffleClientMetrics metrics = null;

		internal ExceptionReporter except = null;

		internal SecretKey key = null;

		internal HttpURLConnection connection = null;

		internal Counters.Counter allErrs = null;

		internal readonly string encHash = "vFE234EIFCiBgYs2tCXY/SjT8Kg=";

		internal readonly MapHost host = new MapHost("localhost", "http://localhost:8080/"
			);

		internal readonly TaskAttemptID map1ID = TaskAttemptID.ForName("attempt_0_1_m_1_1"
			);

		internal readonly TaskAttemptID map2ID = TaskAttemptID.ForName("attempt_0_1_m_2_1"
			);

		internal FileSystem fs = null;

		[Rule]
		public TestName name = new TestName();

		[SetUp]
		public virtual void Setup()
		{
			// mocked generics
			Log.Info(">>>> " + name.GetMethodName());
			job = new JobConf();
			job.SetBoolean(MRJobConfig.ShuffleFetchRetryEnabled, false);
			jobWithRetry = new JobConf();
			jobWithRetry.SetBoolean(MRJobConfig.ShuffleFetchRetryEnabled, true);
			id = TaskAttemptID.ForName("attempt_0_1_r_1_1");
			ss = Org.Mockito.Mockito.Mock<ShuffleSchedulerImpl>();
			mm = Org.Mockito.Mockito.Mock<MergeManagerImpl>();
			r = Org.Mockito.Mockito.Mock<Reporter>();
			metrics = Org.Mockito.Mockito.Mock<ShuffleClientMetrics>();
			except = Org.Mockito.Mockito.Mock<ExceptionReporter>();
			key = JobTokenSecretManager.CreateSecretKey(new byte[] { 0, 0, 0, 0 });
			connection = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			allErrs = Org.Mockito.Mockito.Mock<Counters.Counter>();
			Org.Mockito.Mockito.When(r.GetCounter(Matchers.AnyString(), Matchers.AnyString())
				).ThenReturn(allErrs);
			AList<TaskAttemptID> maps = new AList<TaskAttemptID>(1);
			maps.AddItem(map1ID);
			maps.AddItem(map2ID);
			Org.Mockito.Mockito.When(ss.GetMapsForHost(host)).ThenReturn(maps);
		}

		/// <exception cref="System.ArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Teardown()
		{
			Log.Info("<<<< " + name.GetMethodName());
			if (fs != null)
			{
				fs.Delete(new Path(name.GetMethodName()), true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceOutOfDiskSpace()
		{
			Log.Info("testReduceOutOfDiskSpace");
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenThrow(new DiskChecker.DiskErrorException("No disk space available"
				));
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(ss).ReportLocalError(Matchers.Any<IOException>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyFromHostConnectionTimeout()
		{
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenThrow(new SocketTimeoutException
				("This is a fake timeout :)"));
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection);
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(connection).AddRequestProperty(SecureShuffleUtils.HttpHeaderUrlHash
				, encHash);
			Org.Mockito.Mockito.Verify(allErrs).Increment(1);
			Org.Mockito.Mockito.Verify(ss).CopyFailed(map1ID, host, false, false);
			Org.Mockito.Mockito.Verify(ss).CopyFailed(map2ID, host, false, false);
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map1ID));
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map2ID));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCopyFromHostBogusHeader()
		{
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ByteArrayInputStream @in = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString
				("\u00010 BOGUS DATA\nBOGUS DATA\nBOGUS DATA\n"));
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(connection).AddRequestProperty(SecureShuffleUtils.HttpHeaderUrlHash
				, encHash);
			Org.Mockito.Mockito.Verify(allErrs).Increment(1);
			Org.Mockito.Mockito.Verify(ss).CopyFailed(map1ID, host, true, false);
			Org.Mockito.Mockito.Verify(ss).CopyFailed(map2ID, host, true, false);
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map1ID));
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map2ID));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCopyFromHostIncompatibleShuffleVersion()
		{
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn("mapreduce").ThenReturn("other").ThenReturn("other");
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn("1.0.1").ThenReturn("1.0.0").ThenReturn("1.0.1");
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ByteArrayInputStream @in = new ByteArrayInputStream(new byte[0]);
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			for (int i = 0; i < 3; ++i)
			{
				Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
					ss, mm, r, metrics, except, key, connection);
				underTest.CopyFromHost(host);
			}
			Org.Mockito.Mockito.Verify(connection, Org.Mockito.Mockito.Times(3)).AddRequestProperty
				(SecureShuffleUtils.HttpHeaderUrlHash, encHash);
			Org.Mockito.Mockito.Verify(allErrs, Org.Mockito.Mockito.Times(3)).Increment(1);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).CopyFailed(map1ID, host
				, false, false);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).CopyFailed(map2ID, host
				, false, false);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).PutBackKnownMapOutput
				(Matchers.Any<MapHost>(), Matchers.Eq(map1ID));
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).PutBackKnownMapOutput
				(Matchers.Any<MapHost>(), Matchers.Eq(map2ID));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCopyFromHostIncompatibleShuffleVersionWithRetry()
		{
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn("mapreduce").ThenReturn("other").ThenReturn("other");
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn("1.0.1").ThenReturn("1.0.0").ThenReturn("1.0.1");
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ByteArrayInputStream @in = new ByteArrayInputStream(new byte[0]);
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			for (int i = 0; i < 3; ++i)
			{
				Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(jobWithRetry
					, id, ss, mm, r, metrics, except, key, connection);
				underTest.CopyFromHost(host);
			}
			Org.Mockito.Mockito.Verify(connection, Org.Mockito.Mockito.Times(3)).AddRequestProperty
				(SecureShuffleUtils.HttpHeaderUrlHash, encHash);
			Org.Mockito.Mockito.Verify(allErrs, Org.Mockito.Mockito.Times(3)).Increment(1);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).CopyFailed(map1ID, host
				, false, false);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).CopyFailed(map2ID, host
				, false, false);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).PutBackKnownMapOutput
				(Matchers.Any<MapHost>(), Matchers.Eq(map1ID));
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(3)).PutBackKnownMapOutput
				(Matchers.Any<MapHost>(), Matchers.Eq(map2ID));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCopyFromHostWait()
		{
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			//Defaults to null, which is what we want to test
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(null);
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(connection).AddRequestProperty(SecureShuffleUtils.HttpHeaderUrlHash
				, encHash);
			Org.Mockito.Mockito.Verify(allErrs, Org.Mockito.Mockito.Never()).Increment(1);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Never()).CopyFailed(map1ID, host
				, true, false);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Never()).CopyFailed(map2ID, host
				, true, false);
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map1ID));
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map2ID));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyFromHostCompressFailure()
		{
			InMemoryMapOutput<Text, Text> immo = Org.Mockito.Mockito.Mock<InMemoryMapOutput>(
				);
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(immo);
			Org.Mockito.Mockito.DoThrow(new InternalError()).When(immo).Shuffle(Matchers.Any<
				MapHost>(), Matchers.Any<InputStream>(), Matchers.AnyLong(), Matchers.AnyLong(), 
				Matchers.Any<ShuffleClientMetrics>(), Matchers.Any<Reporter>());
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(connection).AddRequestProperty(SecureShuffleUtils.HttpHeaderUrlHash
				, encHash);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Times(1)).CopyFailed(map1ID, host
				, true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyFromHostWithRetry()
		{
			InMemoryMapOutput<Text, Text> immo = Org.Mockito.Mockito.Mock<InMemoryMapOutput>(
				);
			ss = Org.Mockito.Mockito.Mock<ShuffleSchedulerImpl>();
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(jobWithRetry
				, id, ss, mm, r, metrics, except, key, connection, true);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(immo);
			long retryTime = Time.MonotonicNow();
			Org.Mockito.Mockito.DoAnswer(new _Answer_375(retryTime)).When(immo).Shuffle(Matchers.Any
				<MapHost>(), Matchers.Any<InputStream>(), Matchers.AnyLong(), Matchers.AnyLong()
				, Matchers.Any<ShuffleClientMetrics>(), Matchers.Any<Reporter>());
			// Emulate host down for 3 seconds.
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Never()).CopyFailed(Matchers.Any
				<TaskAttemptID>(), Matchers.Any<MapHost>(), Matchers.AnyBoolean(), Matchers.AnyBoolean
				());
		}

		private sealed class _Answer_375 : Answer<Void>
		{
			public _Answer_375(long retryTime)
			{
				this.retryTime = retryTime;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Answer(InvocationOnMock ignore)
			{
				if ((Time.MonotonicNow() - retryTime) <= 3000)
				{
					throw new InternalError();
				}
				return null;
			}

			private readonly long retryTime;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyFromHostWithRetryThenTimeout()
		{
			InMemoryMapOutput<Text, Text> immo = Org.Mockito.Mockito.Mock<InMemoryMapOutput>(
				);
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(jobWithRetry
				, id, ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200).ThenThrow(
				new SocketTimeoutException("forced timeout"));
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(immo);
			Org.Mockito.Mockito.DoThrow(new IOException("forced error")).When(immo).Shuffle(Matchers.Any
				<MapHost>(), Matchers.Any<InputStream>(), Matchers.AnyLong(), Matchers.AnyLong()
				, Matchers.Any<ShuffleClientMetrics>(), Matchers.Any<Reporter>());
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(allErrs).Increment(1);
			Org.Mockito.Mockito.Verify(ss).CopyFailed(map1ID, host, false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCopyFromHostExtraBytes()
		{
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 14, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(bout);
			IFileOutputStream ios = new IFileOutputStream(dos);
			header.Write(dos);
			ios.Write(Sharpen.Runtime.GetBytesForString("MAPDATA123"));
			ios.Finish();
			ShuffleHeader header2 = new ShuffleHeader(map2ID.ToString(), 14, 10, 1);
			IFileOutputStream ios2 = new IFileOutputStream(dos);
			header2.Write(dos);
			ios2.Write(Sharpen.Runtime.GetBytesForString("MAPDATA456"));
			ios2.Finish();
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			// 8 < 10 therefore there appear to be extra bytes in the IFileInputStream
			InMemoryMapOutput<Text, Text> mapOut = new InMemoryMapOutput<Text, Text>(job, map1ID
				, mm, 8, null, true);
			InMemoryMapOutput<Text, Text> mapOut2 = new InMemoryMapOutput<Text, Text>(job, map2ID
				, mm, 10, null, true);
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Eq(map1ID), Matchers.AnyLong(), Matchers.AnyInt
				())).ThenReturn(mapOut);
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Eq(map2ID), Matchers.AnyLong(), Matchers.AnyInt
				())).ThenReturn(mapOut2);
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(allErrs).Increment(1);
			Org.Mockito.Mockito.Verify(ss).CopyFailed(map1ID, host, true, false);
			Org.Mockito.Mockito.Verify(ss, Org.Mockito.Mockito.Never()).CopyFailed(map2ID, host
				, true, false);
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map1ID));
			Org.Mockito.Mockito.Verify(ss).PutBackKnownMapOutput(Matchers.Any<MapHost>(), Matchers.Eq
				(map2ID));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptedIFile()
		{
			int fetcher = 7;
			Path onDiskMapOutputPath = new Path(name.GetMethodName() + "/foo");
			Path shuffledToDisk = OnDiskMapOutput.GetTempPath(onDiskMapOutputPath, fetcher);
			fs = FileSystem.GetLocal(job).GetRaw();
			MapOutputFile mof = Org.Mockito.Mockito.Mock<MapOutputFile>();
			OnDiskMapOutput<Text, Text> odmo = new OnDiskMapOutput<Text, Text>(map1ID, id, mm
				, 100L, job, mof, fetcher, true, fs, onDiskMapOutputPath);
			string mapData = "MAPDATA12345678901234567890";
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 14, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(bout);
			IFileOutputStream ios = new IFileOutputStream(dos);
			header.Write(dos);
			int headerSize = dos.Size();
			try
			{
				ios.Write(Sharpen.Runtime.GetBytesForString(mapData));
			}
			finally
			{
				ios.Close();
			}
			int dataSize = bout.Size() - headerSize;
			// Ensure that the OnDiskMapOutput shuffler can successfully read the data.
			MapHost host = new MapHost("TestHost", "http://test/url");
			ByteArrayInputStream bin = new ByteArrayInputStream(bout.ToByteArray());
			try
			{
				// Read past the shuffle header.
				bin.Read(new byte[headerSize], 0, headerSize);
				odmo.Shuffle(host, bin, dataSize, dataSize, metrics, Reporter.Null);
			}
			finally
			{
				bin.Close();
			}
			// Now corrupt the IFile data.
			byte[] corrupted = bout.ToByteArray();
			corrupted[headerSize + (dataSize / 2)] = unchecked((int)(0x0));
			try
			{
				bin = new ByteArrayInputStream(corrupted);
				// Read past the shuffle header.
				bin.Read(new byte[headerSize], 0, headerSize);
				odmo.Shuffle(host, bin, dataSize, dataSize, metrics, Reporter.Null);
				NUnit.Framework.Assert.Fail("OnDiskMapOutput.shuffle didn't detect the corrupted map partition file"
					);
			}
			catch (ChecksumException e)
			{
				Log.Info("The expected checksum exception was thrown.", e);
			}
			finally
			{
				bin.Close();
			}
			// Ensure that the shuffled file can be read.
			IFileInputStream iFin = new IFileInputStream(fs.Open(shuffledToDisk), dataSize, job
				);
			try
			{
				iFin.Read(new byte[dataSize], 0, dataSize);
			}
			finally
			{
				iFin.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInterruptInMemory()
		{
			int Fetcher = 2;
			InMemoryMapOutput<Text, Text> immo = Org.Mockito.Mockito.Spy(new InMemoryMapOutput
				<Text, Text>(job, id, mm, 100, null, true));
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(immo);
			Org.Mockito.Mockito.DoNothing().When(mm).WaitForResource();
			Org.Mockito.Mockito.When(ss.GetHost()).ThenReturn(host);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			TestFetcher.StuckInputStream @in = new TestFetcher.StuckInputStream(new ByteArrayInputStream
				(bout.ToByteArray()));
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.DoAnswer(new _Answer_562(@in)).When(connection).Disconnect();
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection, Fetcher);
			underTest.Start();
			// wait for read in inputstream
			@in.WaitForFetcher();
			underTest.ShutDown();
			underTest.Join();
			// rely on test timeout to kill if stuck
			NUnit.Framework.Assert.IsTrue(@in.WasClosedProperly());
			Org.Mockito.Mockito.Verify(immo).Abort();
		}

		private sealed class _Answer_562 : Answer<Void>
		{
			public _Answer_562(TestFetcher.StuckInputStream @in)
			{
				this.@in = @in;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Answer(InvocationOnMock ignore)
			{
				@in.Close();
				return null;
			}

			private readonly TestFetcher.StuckInputStream @in;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInterruptOnDisk()
		{
			int Fetcher = 7;
			Path p = new Path("file:///tmp/foo");
			Path pTmp = OnDiskMapOutput.GetTempPath(p, Fetcher);
			FileSystem mFs = Org.Mockito.Mockito.Mock<FileSystem>(ReturnsDeepStubs);
			MapOutputFile mof = Org.Mockito.Mockito.Mock<MapOutputFile>();
			Org.Mockito.Mockito.When(mof.GetInputFileForWrite(Matchers.Any<TaskID>(), Matchers.AnyLong
				())).ThenReturn(p);
			OnDiskMapOutput<Text, Text> odmo = Org.Mockito.Mockito.Spy(new OnDiskMapOutput<Text
				, Text>(map1ID, id, mm, 100L, job, mof, Fetcher, true, mFs, p));
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(odmo);
			Org.Mockito.Mockito.DoNothing().When(mm).WaitForResource();
			Org.Mockito.Mockito.When(ss.GetHost()).ThenReturn(host);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			TestFetcher.StuckInputStream @in = new TestFetcher.StuckInputStream(new ByteArrayInputStream
				(bout.ToByteArray()));
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			Org.Mockito.Mockito.DoAnswer(new _Answer_610(@in)).When(connection).Disconnect();
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(job, id, 
				ss, mm, r, metrics, except, key, connection, Fetcher);
			underTest.Start();
			// wait for read in inputstream
			@in.WaitForFetcher();
			underTest.ShutDown();
			underTest.Join();
			// rely on test timeout to kill if stuck
			NUnit.Framework.Assert.IsTrue(@in.WasClosedProperly());
			Org.Mockito.Mockito.Verify(mFs).Create(Matchers.Eq(pTmp));
			Org.Mockito.Mockito.Verify(mFs).Delete(Matchers.Eq(pTmp), Matchers.Eq(false));
			Org.Mockito.Mockito.Verify(odmo).Abort();
		}

		private sealed class _Answer_610 : Answer<Void>
		{
			public _Answer_610(TestFetcher.StuckInputStream @in)
			{
				this.@in = @in;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Answer(InvocationOnMock ignore)
			{
				@in.Close();
				return null;
			}

			private readonly TestFetcher.StuckInputStream @in;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyFromHostWithRetryUnreserve()
		{
			InMemoryMapOutput<Text, Text> immo = Org.Mockito.Mockito.Mock<InMemoryMapOutput>(
				);
			Fetcher<Text, Text> underTest = new TestFetcher.FakeFetcher<Text, Text>(jobWithRetry
				, id, ss, mm, r, metrics, except, key, connection);
			string replyHash = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString
				(encHash), key);
			Org.Mockito.Mockito.When(connection.GetResponseCode()).ThenReturn(200);
			Org.Mockito.Mockito.When(connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				)).ThenReturn(replyHash);
			ShuffleHeader header = new ShuffleHeader(map1ID.ToString(), 10, 10, 1);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			header.Write(new DataOutputStream(bout));
			ByteArrayInputStream @in = new ByteArrayInputStream(bout.ToByteArray());
			Org.Mockito.Mockito.When(connection.GetInputStream()).ThenReturn(@in);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderName))
				.ThenReturn(ShuffleHeader.DefaultHttpHeaderName);
			Org.Mockito.Mockito.When(connection.GetHeaderField(ShuffleHeader.HttpHeaderVersion
				)).ThenReturn(ShuffleHeader.DefaultHttpHeaderVersion);
			// Verify that unreserve occurs if an exception happens after shuffle
			// buffer is reserved.
			Org.Mockito.Mockito.When(mm.Reserve(Matchers.Any<TaskAttemptID>(), Matchers.AnyLong
				(), Matchers.AnyInt())).ThenReturn(immo);
			Org.Mockito.Mockito.DoThrow(new IOException("forced error")).When(immo).Shuffle(Matchers.Any
				<MapHost>(), Matchers.Any<InputStream>(), Matchers.AnyLong(), Matchers.AnyLong()
				, Matchers.Any<ShuffleClientMetrics>(), Matchers.Any<Reporter>());
			underTest.CopyFromHost(host);
			Org.Mockito.Mockito.Verify(immo).Abort();
		}

		public class FakeFetcher<K, V> : Fetcher<K, V>
		{
			private bool renewConnection = false;

			public FakeFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, V
				> scheduler, MergeManagerImpl<K, V> merger, Reporter reporter, ShuffleClientMetrics
				 metrics, ExceptionReporter exceptionReporter, SecretKey jobTokenSecret, HttpURLConnection
				 connection)
				: base(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, jobTokenSecret
					)
			{
				// If connection need to be reopen.
				this.connection = connection;
			}

			public FakeFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, V
				> scheduler, MergeManagerImpl<K, V> merger, Reporter reporter, ShuffleClientMetrics
				 metrics, ExceptionReporter exceptionReporter, SecretKey jobTokenSecret, HttpURLConnection
				 connection, bool renewConnection)
				: base(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, jobTokenSecret
					)
			{
				this.connection = connection;
				this.renewConnection = renewConnection;
			}

			public FakeFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, V
				> scheduler, MergeManagerImpl<K, V> merger, Reporter reporter, ShuffleClientMetrics
				 metrics, ExceptionReporter exceptionReporter, SecretKey jobTokenSecret, HttpURLConnection
				 connection, int id)
				: base(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, jobTokenSecret
					, id)
			{
				this.connection = connection;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void OpenConnection(Uri url)
			{
				if (null == connection || renewConnection)
				{
					base.OpenConnection(url);
				}
				// already 'opened' the mocked connection
				return;
			}
		}

		internal class StuckInputStream : FilterInputStream
		{
			internal bool stuck = false;

			internal volatile bool closed = false;

			internal StuckInputStream(InputStream inner)
				: base(inner)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual int Freeze()
			{
				lock (this)
				{
					stuck = true;
					Sharpen.Runtime.Notify(this);
				}
				// connection doesn't throw InterruptedException, but may return some
				// bytes geq 0 or throw an exception
				while (!Sharpen.Thread.CurrentThread().IsInterrupted() || closed)
				{
					// spin
					if (closed)
					{
						throw new IOException("underlying stream closed, triggered an error");
					}
				}
				return 0;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				int ret = base.Read();
				if (ret != -1)
				{
					return ret;
				}
				return Freeze();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b)
			{
				int ret = base.Read(b);
				if (ret != -1)
				{
					return ret;
				}
				return Freeze();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int len)
			{
				int ret = base.Read(b, off, len);
				if (ret != -1)
				{
					return ret;
				}
				return Freeze();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				closed = true;
			}

			/// <exception cref="System.Exception"/>
			public virtual void WaitForFetcher()
			{
				lock (this)
				{
					while (!stuck)
					{
						Sharpen.Runtime.Wait(this);
					}
				}
			}

			public virtual bool WasClosedProperly()
			{
				return closed;
			}
		}
	}
}
