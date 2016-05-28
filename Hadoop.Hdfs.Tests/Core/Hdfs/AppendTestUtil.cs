using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Utilities for append-related tests</summary>
	public class AppendTestUtil
	{
		/// <summary>
		/// For specifying the random number generator seed,
		/// change the following value:
		/// </summary>
		internal static readonly long RandomNumberGeneratorSeed = null;

		internal static readonly Log Log = LogFactory.GetLog(typeof(AppendTestUtil));

		private static readonly Random Seed = new Random();

		static AppendTestUtil()
		{
			long seed = RandomNumberGeneratorSeed == null ? Seed.NextLong() : RandomNumberGeneratorSeed;
			Log.Info("seed=" + seed);
			Seed.SetSeed(seed);
		}

		private sealed class _ThreadLocal_56 : ThreadLocal<Random>
		{
			public _ThreadLocal_56()
			{
			}

			protected override Random InitialValue()
			{
				Random r = new Random();
				lock (AppendTestUtil.Seed)
				{
					long seed = AppendTestUtil.Seed.NextLong();
					r.SetSeed(seed);
					AppendTestUtil.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": seed=" + seed
						);
				}
				return r;
			}
		}

		private static readonly ThreadLocal<Random> Random = new _ThreadLocal_56();

		internal const int BlockSize = 1024;

		internal const int NumBlocks = 10;

		internal const int FileSize = NumBlocks * BlockSize + 1;

		internal static long seed = -1;

		internal static int NextInt()
		{
			return Random.Get().Next();
		}

		internal static int NextInt(int n)
		{
			return Random.Get().Next(n);
		}

		internal static int NextLong()
		{
			return Random.Get().Next();
		}

		public static byte[] RandomBytes(long seed, int size)
		{
			Log.Info("seed=" + seed + ", size=" + size);
			byte[] b = new byte[size];
			Random rand = new Random(seed);
			rand.NextBytes(b);
			return b;
		}

		/// <returns>a random file partition of length n.</returns>
		public static int[] RandomFilePartition(int n, int parts)
		{
			int[] p = new int[parts];
			for (int i = 0; i < p.Length; i++)
			{
				p[i] = NextInt(n - i - 1) + 1;
			}
			Arrays.Sort(p);
			for (int i_1 = 1; i_1 < p.Length; i_1++)
			{
				if (p[i_1] <= p[i_1 - 1])
				{
					p[i_1] = p[i_1 - 1] + 1;
				}
			}
			Log.Info("partition=" + Arrays.ToString(p));
			NUnit.Framework.Assert.IsTrue("i=0", p[0] > 0 && p[0] < n);
			for (int i_2 = 1; i_2 < p.Length; i_2++)
			{
				NUnit.Framework.Assert.IsTrue("i=" + i_2, p[i_2] > p[i_2 - 1] && p[i_2] < n);
			}
			return p;
		}

		internal static void Sleep(long ms)
		{
			try
			{
				Sharpen.Thread.Sleep(ms);
			}
			catch (Exception e)
			{
				Log.Info("ms=" + ms, e);
			}
		}

		/// <summary>
		/// Returns the reference to a new instance of FileSystem created
		/// with different user name
		/// </summary>
		/// <param name="conf">current Configuration</param>
		/// <returns>FileSystem instance</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		public static FileSystem CreateHdfsWithDifferentUsername(Configuration conf)
		{
			string username = UserGroupInformation.GetCurrentUser().GetShortUserName() + "_XXX";
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(username, new 
				string[] { "supergroup" });
			return DFSTestUtil.GetFileSystemAs(ugi, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Write(OutputStream @out, int offset, int length)
		{
			byte[] bytes = new byte[length];
			for (int i = 0; i < length; i++)
			{
				bytes[i] = unchecked((byte)(offset + i));
			}
			@out.Write(bytes);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Check(FileSystem fs, Path p, long length)
		{
			int i = -1;
			try
			{
				FileStatus status = fs.GetFileStatus(p);
				FSDataInputStream @in = fs.Open(p);
				if (@in.GetWrappedStream() is DFSInputStream)
				{
					long len = ((DFSInputStream)@in.GetWrappedStream()).GetFileLength();
					NUnit.Framework.Assert.AreEqual(length, len);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(length, status.GetLen());
				}
				for (i++; i < length; i++)
				{
					NUnit.Framework.Assert.AreEqual(unchecked((byte)i), unchecked((byte)@in.Read()));
				}
				i = -(int)length;
				NUnit.Framework.Assert.AreEqual(-1, @in.Read());
				//EOF  
				@in.Close();
			}
			catch (IOException ioe)
			{
				throw new IOException("p=" + p + ", length=" + length + ", i=" + i, ioe);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Check(DistributedFileSystem fs, Path p, int position, int length
			)
		{
			byte[] buf = new byte[length];
			int i = 0;
			try
			{
				FSDataInputStream @in = fs.Open(p);
				@in.Read(position, buf, 0, buf.Length);
				for (i = position; i < length + position; i++)
				{
					NUnit.Framework.Assert.AreEqual(unchecked((byte)i), buf[i - position]);
				}
				@in.Close();
			}
			catch (IOException ioe)
			{
				throw new IOException("p=" + p + ", length=" + length + ", i=" + i, ioe);
			}
		}

		/// <summary>create a buffer that contains the entire test file data.</summary>
		public static byte[] InitBuffer(int size)
		{
			if (seed == -1)
			{
				seed = NextLong();
			}
			return RandomBytes(seed, size);
		}

		/// <summary>
		/// Creates a file but does not close it
		/// Make sure to call close() on the returned stream
		/// </summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		public static FSDataOutputStream CreateFile(FileSystem fileSys, Path name, int repl
			)
		{
			return fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, BlockSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckFullFile(FileSystem fs, Path file, int len, byte[] compareContent
			)
		{
			CheckFullFile(fs, file, len, compareContent, file.ToString());
		}

		/// <summary>
		/// Compare the content of a file created from FileSystem and Path with
		/// the specified byte[] buffer's content
		/// </summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		public static void CheckFullFile(FileSystem fs, Path name, int len, byte[] compareContent
			, string message)
		{
			CheckFullFile(fs, name, len, compareContent, message, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckFullFile(FileSystem fs, Path name, int len, byte[] compareContent
			, string message, bool checkFileStatus)
		{
			if (checkFileStatus)
			{
				FileStatus status = fs.GetFileStatus(name);
				NUnit.Framework.Assert.AreEqual("len=" + len + " but status.getLen()=" + status.GetLen
					(), len, status.GetLen());
			}
			FSDataInputStream stm = fs.Open(name);
			byte[] actual = new byte[len];
			stm.ReadFully(0, actual);
			CheckData(actual, 0, compareContent, message);
			stm.Close();
		}

		private static void CheckData(byte[] actual, int from, byte[] expected, string message
			)
		{
			for (int idx = 0; idx < actual.Length; idx++)
			{
				NUnit.Framework.Assert.AreEqual(message + " byte " + (from + idx) + " differs. expected "
					 + expected[from + idx] + " actual " + actual[idx], expected[from + idx], actual
					[idx]);
				actual[idx] = 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TestAppend(FileSystem fs, Path p)
		{
			byte[] bytes = new byte[1000];
			{
				//create file
				FSDataOutputStream @out = fs.Create(p, (short)1);
				@out.Write(bytes);
				@out.Close();
				NUnit.Framework.Assert.AreEqual(bytes.Length, fs.GetFileStatus(p).GetLen());
			}
			for (int i = 2; i < 500; i++)
			{
				//append
				FSDataOutputStream @out = fs.Append(p);
				@out.Write(bytes);
				@out.Close();
				NUnit.Framework.Assert.AreEqual(i * bytes.Length, fs.GetFileStatus(p).GetLen());
			}
		}
	}
}
