using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Shell;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFileSystem : TestCase
	{
		private static readonly Log Log = FileSystem.Log;

		private static Configuration conf = new Configuration();

		private static int BufferSize = conf.GetInt("io.file.buffer.size", 4096);

		private const long Mega = 1024 * 1024;

		private const int SeeksPerFile = 4;

		private static string Root = Runtime.GetProperty("test.build.data", "fs_test");

		private static Path ControlDir = new Path(Root, "fs_control");

		private static Path WriteDir = new Path(Root, "fs_write");

		private static Path ReadDir = new Path(Root, "fs_read");

		private static Path DataDir = new Path(Root, "fs_data");

		/// <exception cref="System.Exception"/>
		public virtual void TestFs()
		{
			TestFs(10 * Mega, 100, 0);
		}

		/// <exception cref="System.Exception"/>
		public static void TestFs(long megaBytes, int numFiles, long seed)
		{
			FileSystem fs = FileSystem.Get(conf);
			if (seed == 0)
			{
				seed = new Random().NextLong();
			}
			Log.Info("seed = " + seed);
			CreateControlFile(fs, megaBytes, numFiles, seed);
			WriteTest(fs, false);
			ReadTest(fs, false);
			SeekTest(fs, false);
			fs.Delete(ControlDir, true);
			fs.Delete(DataDir, true);
			fs.Delete(WriteDir, true);
			fs.Delete(ReadDir, true);
		}

		/// <exception cref="System.Exception"/>
		public static void TestCommandFormat()
		{
			// This should go to TestFsShell.java when it is added.
			CommandFormat cf;
			cf = new CommandFormat("copyToLocal", 2, 2, "crc", "ignoreCrc");
			NUnit.Framework.Assert.AreEqual(cf.Parse(new string[] { "-get", "file", "-" }, 1)
				[1], "-");
			try
			{
				cf.Parse(new string[] { "-get", "file", "-ignoreCrc", "/foo" }, 1);
				Fail("Expected parsing to fail as it should stop at first non-option");
			}
			catch (Exception)
			{
			}
			// Expected
			cf = new CommandFormat("tail", 1, 1, "f");
			NUnit.Framework.Assert.AreEqual(cf.Parse(new string[] { "-tail", "fileName" }, 1)
				[0], "fileName");
			NUnit.Framework.Assert.AreEqual(cf.Parse(new string[] { "-tail", "-f", "fileName"
				 }, 1)[0], "fileName");
			cf = new CommandFormat("setrep", 2, 2, "R", "w");
			NUnit.Framework.Assert.AreEqual(cf.Parse(new string[] { "-setrep", "-R", "2", "/foo/bar"
				 }, 1)[1], "/foo/bar");
			cf = new CommandFormat("put", 2, 10000);
			NUnit.Framework.Assert.AreEqual(cf.Parse(new string[] { "-put", "-", "dest" }, 1)
				[1], "dest");
		}

		/// <exception cref="System.Exception"/>
		public static void CreateControlFile(FileSystem fs, long megaBytes, int numFiles, 
			long seed)
		{
			Log.Info("creating control file: " + megaBytes + " bytes, " + numFiles + " files"
				);
			Path controlFile = new Path(ControlDir, "files");
			fs.Delete(controlFile, true);
			Random random = new Random(seed);
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, controlFile, typeof(
				Text), typeof(LongWritable), SequenceFile.CompressionType.None);
			long totalSize = 0;
			long maxSize = ((megaBytes / numFiles) * 2) + 1;
			try
			{
				while (totalSize < megaBytes)
				{
					Text name = new Text(System.Convert.ToString(random.NextLong()));
					long size = random.NextLong();
					if (size < 0)
					{
						size = -size;
					}
					size = size % maxSize;
					//LOG.info(" adding: name="+name+" size="+size);
					writer.Append(name, new LongWritable(size));
					totalSize += size;
				}
			}
			finally
			{
				writer.Close();
			}
			Log.Info("created control file for: " + totalSize + " bytes");
		}

		public class WriteMapper : Configured, Mapper<Text, LongWritable, Text, LongWritable
			>
		{
			private Random random = new Random();

			private byte[] buffer = new byte[BufferSize];

			private FileSystem fs;

			private bool fastCheck;

			private string suffix = "-" + random.NextLong();

			public WriteMapper()
				: base(null)
			{
				{
					// a random suffix per task
					try
					{
						fs = FileSystem.Get(conf);
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public WriteMapper(Configuration conf)
				: base(conf)
			{
				{
					try
					{
						fs = FileSystem.Get(conf);
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public virtual void Configure(JobConf job)
			{
				SetConf(job);
				fastCheck = job.GetBoolean("fs.test.fastCheck", false);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(Text key, LongWritable value, OutputCollector<Text, LongWritable
				> collector, Reporter reporter)
			{
				string name = key.ToString();
				long size = value.Get();
				long seed = long.Parse(name);
				random.SetSeed(seed);
				reporter.SetStatus("creating " + name);
				// write to temp file initially to permit parallel execution
				Path tempFile = new Path(DataDir, name + suffix);
				OutputStream @out = fs.Create(tempFile);
				long written = 0;
				try
				{
					while (written < size)
					{
						if (fastCheck)
						{
							Arrays.Fill(buffer, unchecked((byte)random.Next(byte.MaxValue)));
						}
						else
						{
							random.NextBytes(buffer);
						}
						long remains = size - written;
						int length = (remains <= buffer.Length) ? (int)remains : buffer.Length;
						@out.Write(buffer, 0, length);
						written += length;
						reporter.SetStatus("writing " + name + "@" + written + "/" + size);
					}
				}
				finally
				{
					@out.Close();
				}
				// rename to final location
				fs.Rename(tempFile, new Path(DataDir, name));
				collector.Collect(new Text("bytes"), new LongWritable(written));
				reporter.SetStatus("wrote " + name);
			}

			public virtual void Close()
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public static void WriteTest(FileSystem fs, bool fastCheck)
		{
			fs.Delete(DataDir, true);
			fs.Delete(WriteDir, true);
			JobConf job = new JobConf(conf, typeof(TestFileSystem));
			job.SetBoolean("fs.test.fastCheck", fastCheck);
			FileInputFormat.SetInputPaths(job, ControlDir);
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(typeof(TestFileSystem.WriteMapper));
			job.SetReducerClass(typeof(LongSumReducer));
			FileOutputFormat.SetOutputPath(job, WriteDir);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(LongWritable));
			job.SetNumReduceTasks(1);
			JobClient.RunJob(job);
		}

		public class ReadMapper : Configured, Mapper<Text, LongWritable, Text, LongWritable
			>
		{
			private Random random = new Random();

			private byte[] buffer = new byte[BufferSize];

			private byte[] check = new byte[BufferSize];

			private FileSystem fs;

			private bool fastCheck;

			public ReadMapper()
				: base(null)
			{
				{
					try
					{
						fs = FileSystem.Get(conf);
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public ReadMapper(Configuration conf)
				: base(conf)
			{
				{
					try
					{
						fs = FileSystem.Get(conf);
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public virtual void Configure(JobConf job)
			{
				SetConf(job);
				fastCheck = job.GetBoolean("fs.test.fastCheck", false);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(Text key, LongWritable value, OutputCollector<Text, LongWritable
				> collector, Reporter reporter)
			{
				string name = key.ToString();
				long size = value.Get();
				long seed = long.Parse(name);
				random.SetSeed(seed);
				reporter.SetStatus("opening " + name);
				DataInputStream @in = new DataInputStream(fs.Open(new Path(DataDir, name)));
				long read = 0;
				try
				{
					while (read < size)
					{
						long remains = size - read;
						int n = (remains <= buffer.Length) ? (int)remains : buffer.Length;
						@in.ReadFully(buffer, 0, n);
						read += n;
						if (fastCheck)
						{
							Arrays.Fill(check, unchecked((byte)random.Next(byte.MaxValue)));
						}
						else
						{
							random.NextBytes(check);
						}
						if (n != buffer.Length)
						{
							Arrays.Fill(buffer, n, buffer.Length, unchecked((byte)0));
							Arrays.Fill(check, n, check.Length, unchecked((byte)0));
						}
						NUnit.Framework.Assert.IsTrue(Arrays.Equals(buffer, check));
						reporter.SetStatus("reading " + name + "@" + read + "/" + size);
					}
				}
				finally
				{
					@in.Close();
				}
				collector.Collect(new Text("bytes"), new LongWritable(read));
				reporter.SetStatus("read " + name);
			}

			public virtual void Close()
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public static void ReadTest(FileSystem fs, bool fastCheck)
		{
			fs.Delete(ReadDir, true);
			JobConf job = new JobConf(conf, typeof(TestFileSystem));
			job.SetBoolean("fs.test.fastCheck", fastCheck);
			FileInputFormat.SetInputPaths(job, ControlDir);
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(typeof(TestFileSystem.ReadMapper));
			job.SetReducerClass(typeof(LongSumReducer));
			FileOutputFormat.SetOutputPath(job, ReadDir);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(LongWritable));
			job.SetNumReduceTasks(1);
			JobClient.RunJob(job);
		}

		public class SeekMapper<K> : Configured, Mapper<Text, LongWritable, K, LongWritable
			>
		{
			private Random random = new Random();

			private byte[] check = new byte[BufferSize];

			private FileSystem fs;

			private bool fastCheck;

			public SeekMapper()
				: base(null)
			{
				{
					try
					{
						fs = FileSystem.Get(conf);
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public SeekMapper(Configuration conf)
				: base(conf)
			{
				{
					try
					{
						fs = FileSystem.Get(conf);
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public virtual void Configure(JobConf job)
			{
				SetConf(job);
				fastCheck = job.GetBoolean("fs.test.fastCheck", false);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(Text key, LongWritable value, OutputCollector<K, LongWritable
				> collector, Reporter reporter)
			{
				string name = key.ToString();
				long size = value.Get();
				long seed = long.Parse(name);
				if (size == 0)
				{
					return;
				}
				reporter.SetStatus("opening " + name);
				FSDataInputStream @in = fs.Open(new Path(DataDir, name));
				try
				{
					for (int i = 0; i < SeeksPerFile; i++)
					{
						// generate a random position
						long position = Math.Abs(random.NextLong()) % size;
						// seek file to that position
						reporter.SetStatus("seeking " + name);
						@in.Seek(position);
						byte b = @in.ReadByte();
						// check that byte matches
						byte checkByte = 0;
						// advance random state to that position
						random.SetSeed(seed);
						for (int p = 0; p <= position; p += check.Length)
						{
							reporter.SetStatus("generating data for " + name);
							if (fastCheck)
							{
								checkByte = unchecked((byte)random.Next(byte.MaxValue));
							}
							else
							{
								random.NextBytes(check);
								checkByte = check[(int)(position % check.Length)];
							}
						}
						NUnit.Framework.Assert.AreEqual(b, checkByte);
					}
				}
				finally
				{
					@in.Close();
				}
			}

			public virtual void Close()
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public static void SeekTest(FileSystem fs, bool fastCheck)
		{
			fs.Delete(ReadDir, true);
			JobConf job = new JobConf(conf, typeof(TestFileSystem));
			job.SetBoolean("fs.test.fastCheck", fastCheck);
			FileInputFormat.SetInputPaths(job, ControlDir);
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(typeof(TestFileSystem.SeekMapper));
			job.SetReducerClass(typeof(LongSumReducer));
			FileOutputFormat.SetOutputPath(job, ReadDir);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(LongWritable));
			job.SetNumReduceTasks(1);
			JobClient.RunJob(job);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int megaBytes = 10;
			int files = 100;
			bool noRead = false;
			bool noWrite = false;
			bool noSeek = false;
			bool fastCheck = false;
			long seed = new Random().NextLong();
			string usage = "Usage: TestFileSystem -files N -megaBytes M [-noread] [-nowrite] [-noseek] [-fastcheck]";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-files"))
				{
					files = System.Convert.ToInt32(args[++i]);
				}
				else
				{
					if (args[i].Equals("-megaBytes"))
					{
						megaBytes = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-noread"))
						{
							noRead = true;
						}
						else
						{
							if (args[i].Equals("-nowrite"))
							{
								noWrite = true;
							}
							else
							{
								if (args[i].Equals("-noseek"))
								{
									noSeek = true;
								}
								else
								{
									if (args[i].Equals("-fastcheck"))
									{
										fastCheck = true;
									}
								}
							}
						}
					}
				}
			}
			Log.Info("seed = " + seed);
			Log.Info("files = " + files);
			Log.Info("megaBytes = " + megaBytes);
			FileSystem fs = FileSystem.Get(conf);
			if (!noWrite)
			{
				CreateControlFile(fs, megaBytes * Mega, files, seed);
				WriteTest(fs, fastCheck);
			}
			if (!noRead)
			{
				ReadTest(fs, fastCheck);
			}
			if (!noSeek)
			{
				SeekTest(fs, fastCheck);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFsCache()
		{
			{
				long now = Runtime.CurrentTimeMillis();
				string[] users = new string[] { "foo", "bar" };
				Configuration conf = new Configuration();
				FileSystem[] fs = new FileSystem[users.Length];
				for (int i = 0; i < users.Length; i++)
				{
					UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(users[i]);
					fs[i] = ugi.DoAs(new _PrivilegedExceptionAction_500(conf));
					for (int j = 0; j < i; j++)
					{
						NUnit.Framework.Assert.IsFalse(fs[j] == fs[i]);
					}
				}
				FileSystem.CloseAll();
			}
			{
				try
				{
					RunTestCache(NameNode.DefaultPort);
				}
				catch (BindException be)
				{
					Log.Warn("Cannot test NameNode.DEFAULT_PORT (=" + NameNode.DefaultPort + ")", be);
				}
				RunTestCache(0);
			}
		}

		private sealed class _PrivilegedExceptionAction_500 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_500(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileSystem Run()
			{
				return FileSystem.Get(conf);
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		internal static void RunTestCache(int port)
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(port).NumDataNodes(2).Build
					();
				URI uri = cluster.GetFileSystem().GetUri();
				Log.Info("uri=" + uri);
				{
					FileSystem fs = FileSystem.Get(uri, new Configuration());
					CheckPath(cluster, fs);
					for (int i = 0; i < 100; i++)
					{
						NUnit.Framework.Assert.IsTrue(fs == FileSystem.Get(uri, new Configuration()));
					}
				}
				if (port == NameNode.DefaultPort)
				{
					//test explicit default port
					URI uri2 = new URI(uri.GetScheme(), uri.GetUserInfo(), uri.GetHost(), NameNode.DefaultPort
						, uri.GetPath(), uri.GetQuery(), uri.GetFragment());
					Log.Info("uri2=" + uri2);
					FileSystem fs = FileSystem.Get(uri2, conf);
					CheckPath(cluster, fs);
					for (int i = 0; i < 100; i++)
					{
						NUnit.Framework.Assert.IsTrue(fs == FileSystem.Get(uri2, new Configuration()));
					}
				}
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
		internal static void CheckPath(MiniDFSCluster cluster, FileSystem fileSys)
		{
			IPEndPoint add = cluster.GetNameNode().GetNameNodeAddress();
			// Test upper/lower case
			fileSys.CheckPath(new Path("hdfs://" + StringUtils.ToUpperCase(add.GetHostName())
				 + ":" + add.Port));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFsClose()
		{
			{
				Configuration conf = new Configuration();
				new Path("file:///").GetFileSystem(conf);
				FileSystem.CloseAll();
			}
			{
				Configuration conf = new Configuration();
				new Path("hftp://localhost:12345/").GetFileSystem(conf);
				FileSystem.CloseAll();
			}
			{
				Configuration conf = new Configuration();
				FileSystem fs = new Path("hftp://localhost:12345/").GetFileSystem(conf);
				FileSystem.CloseAll();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFsShutdownHook()
		{
			ICollection<FileSystem> closed = Collections.SynchronizedSet(new HashSet<FileSystem
				>());
			Configuration conf = new Configuration();
			Configuration confNoAuto = new Configuration();
			conf.SetClass("fs.test.impl", typeof(TestFileSystem.TestShutdownFileSystem), typeof(
				FileSystem));
			confNoAuto.SetClass("fs.test.impl", typeof(TestFileSystem.TestShutdownFileSystem)
				, typeof(FileSystem));
			confNoAuto.SetBoolean("fs.automatic.close", false);
			TestFileSystem.TestShutdownFileSystem fsWithAuto = (TestFileSystem.TestShutdownFileSystem
				)(new Path("test://a/").GetFileSystem(conf));
			TestFileSystem.TestShutdownFileSystem fsWithoutAuto = (TestFileSystem.TestShutdownFileSystem
				)(new Path("test://b/").GetFileSystem(confNoAuto));
			fsWithAuto.SetClosedSet(closed);
			fsWithoutAuto.SetClosedSet(closed);
			// Different URIs should result in different FS instances
			NUnit.Framework.Assert.AreNotSame(fsWithAuto, fsWithoutAuto);
			FileSystem.Cache.CloseAll(true);
			NUnit.Framework.Assert.AreEqual(1, closed.Count);
			NUnit.Framework.Assert.IsTrue(closed.Contains(fsWithAuto));
			closed.Clear();
			FileSystem.CloseAll();
			NUnit.Framework.Assert.AreEqual(1, closed.Count);
			NUnit.Framework.Assert.IsTrue(closed.Contains(fsWithoutAuto));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCacheKeysAreCaseInsensitive()
		{
			Configuration conf = new Configuration();
			// check basic equality
			FileSystem.Cache.Key lowercaseCachekey1 = new FileSystem.Cache.Key(new URI("hftp://localhost:12345/"
				), conf);
			FileSystem.Cache.Key lowercaseCachekey2 = new FileSystem.Cache.Key(new URI("hftp://localhost:12345/"
				), conf);
			NUnit.Framework.Assert.AreEqual(lowercaseCachekey1, lowercaseCachekey2);
			// check insensitive equality    
			FileSystem.Cache.Key uppercaseCachekey = new FileSystem.Cache.Key(new URI("HFTP://Localhost:12345/"
				), conf);
			NUnit.Framework.Assert.AreEqual(lowercaseCachekey2, uppercaseCachekey);
			// check behaviour with collections
			IList<FileSystem.Cache.Key> list = new AList<FileSystem.Cache.Key>();
			list.AddItem(uppercaseCachekey);
			NUnit.Framework.Assert.IsTrue(list.Contains(uppercaseCachekey));
			NUnit.Framework.Assert.IsTrue(list.Contains(lowercaseCachekey2));
			ICollection<FileSystem.Cache.Key> set = new HashSet<FileSystem.Cache.Key>();
			set.AddItem(uppercaseCachekey);
			NUnit.Framework.Assert.IsTrue(set.Contains(uppercaseCachekey));
			NUnit.Framework.Assert.IsTrue(set.Contains(lowercaseCachekey2));
			IDictionary<FileSystem.Cache.Key, string> map = new Dictionary<FileSystem.Cache.Key
				, string>();
			map[uppercaseCachekey] = string.Empty;
			NUnit.Framework.Assert.IsTrue(map.Contains(uppercaseCachekey));
			NUnit.Framework.Assert.IsTrue(map.Contains(lowercaseCachekey2));
		}

		/// <exception cref="System.Exception"/>
		public static void TestFsUniqueness(long megaBytes, int numFiles, long seed)
		{
			// multiple invocations of FileSystem.get return the same object.
			FileSystem fs1 = FileSystem.Get(conf);
			FileSystem fs2 = FileSystem.Get(conf);
			NUnit.Framework.Assert.IsTrue(fs1 == fs2);
			// multiple invocations of FileSystem.newInstance return different objects
			fs1 = FileSystem.NewInstance(conf);
			fs2 = FileSystem.NewInstance(conf);
			NUnit.Framework.Assert.IsTrue(fs1 != fs2 && !fs1.Equals(fs2));
			fs1.Close();
			fs2.Close();
		}

		public class TestShutdownFileSystem : RawLocalFileSystem
		{
			private ICollection<FileSystem> closedSet;

			public virtual void SetClosedSet(ICollection<FileSystem> closedSet)
			{
				this.closedSet = closedSet;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (closedSet != null)
				{
					closedSet.AddItem(this);
				}
				base.Close();
			}
		}
	}
}
