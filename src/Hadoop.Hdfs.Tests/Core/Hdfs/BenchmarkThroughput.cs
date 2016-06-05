using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class benchmarks the performance of the local file system, raw local
	/// file system and HDFS at reading and writing files.
	/// </summary>
	/// <remarks>
	/// This class benchmarks the performance of the local file system, raw local
	/// file system and HDFS at reading and writing files. The user should invoke
	/// the main of this class and optionally include a repetition count.
	/// </remarks>
	public class BenchmarkThroughput : Configured, Tool
	{
		private LocalDirAllocator dir;

		private long startTime;

		private int BufferSize;

		// the property in the config that specifies a working directory
		// the size of the buffer to use
		private void ResetMeasurements()
		{
			startTime = Time.Now();
		}

		private void PrintMeasurements()
		{
			System.Console.Out.WriteLine(" time: " + ((Time.Now() - startTime) / 1000));
		}

		/// <exception cref="System.IO.IOException"/>
		private Path WriteLocalFile(string name, Configuration conf, long total)
		{
			Path path = dir.GetLocalPathForWrite(name, total, conf);
			System.Console.Out.Write("Writing " + name);
			ResetMeasurements();
			OutputStream @out = new FileOutputStream(new FilePath(path.ToString()));
			byte[] data = new byte[BufferSize];
			for (long size = 0; size < total; size += BufferSize)
			{
				@out.Write(data);
			}
			@out.Close();
			PrintMeasurements();
			return path;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadLocalFile(Path path, string name, Configuration conf)
		{
			System.Console.Out.Write("Reading " + name);
			ResetMeasurements();
			InputStream @in = new FileInputStream(new FilePath(path.ToString()));
			byte[] data = new byte[BufferSize];
			long size = 0;
			while (size >= 0)
			{
				size = @in.Read(data);
			}
			@in.Close();
			PrintMeasurements();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteAndReadLocalFile(string name, Configuration conf, long size)
		{
			Path f = null;
			try
			{
				f = WriteLocalFile(name, conf, size);
				ReadLocalFile(f, name, conf);
			}
			finally
			{
				if (f != null)
				{
					new FilePath(f.ToString()).Delete();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Path WriteFile(FileSystem fs, string name, Configuration conf, long total
			)
		{
			Path f = dir.GetLocalPathForWrite(name, total, conf);
			System.Console.Out.Write("Writing " + name);
			ResetMeasurements();
			OutputStream @out = fs.Create(f);
			byte[] data = new byte[BufferSize];
			for (long size = 0; size < total; size += BufferSize)
			{
				@out.Write(data);
			}
			@out.Close();
			PrintMeasurements();
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadFile(FileSystem fs, Path f, string name, Configuration conf)
		{
			System.Console.Out.Write("Reading " + name);
			ResetMeasurements();
			InputStream @in = fs.Open(f);
			byte[] data = new byte[BufferSize];
			long val = 0;
			while (val >= 0)
			{
				val = @in.Read(data);
			}
			@in.Close();
			PrintMeasurements();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteAndReadFile(FileSystem fs, string name, Configuration conf, long
			 size)
		{
			Path f = null;
			try
			{
				f = WriteFile(fs, name, conf, size);
				ReadFile(fs, f, name, conf);
			}
			finally
			{
				try
				{
					if (f != null)
					{
						fs.Delete(f, true);
					}
				}
				catch (IOException)
				{
				}
			}
		}

		// IGNORE
		private static void PrintUsage()
		{
			ToolRunner.PrintGenericCommandUsage(System.Console.Error);
			System.Console.Error.WriteLine("Usage: dfsthroughput [#reps]");
			System.Console.Error.WriteLine("Config properties:\n" + "  dfsthroughput.file.size:\tsize of each write/read (10GB)\n"
				 + "  dfsthroughput.buffer.size:\tbuffer size for write/read (4k)\n");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] args)
		{
			// silence the minidfs cluster
			Log hadoopLog = LogFactory.GetLog("org");
			if (hadoopLog is Log4JLogger)
			{
				((Log4JLogger)hadoopLog).GetLogger().SetLevel(Level.Warn);
			}
			int reps = 1;
			if (args.Length == 1)
			{
				try
				{
					reps = System.Convert.ToInt32(args[0]);
				}
				catch (FormatException)
				{
					PrintUsage();
					return -1;
				}
			}
			else
			{
				if (args.Length > 1)
				{
					PrintUsage();
					return -1;
				}
			}
			Configuration conf = GetConf();
			// the size of the file to write
			long Size = conf.GetLong("dfsthroughput.file.size", 10L * 1024 * 1024 * 1024);
			BufferSize = conf.GetInt("dfsthroughput.buffer.size", 4 * 1024);
			string localDir = conf.Get("mapred.temp.dir");
			if (localDir == null)
			{
				localDir = conf.Get("hadoop.tmp.dir");
				conf.Set("mapred.temp.dir", localDir);
			}
			dir = new LocalDirAllocator("mapred.temp.dir");
			Runtime.SetProperty("test.build.data", localDir);
			System.Console.Out.WriteLine("Local = " + localDir);
			ChecksumFileSystem checkedLocal = FileSystem.GetLocal(conf);
			FileSystem rawLocal = checkedLocal.GetRawFileSystem();
			for (int i = 0; i < reps; ++i)
			{
				WriteAndReadLocalFile("local", conf, Size);
				WriteAndReadFile(rawLocal, "raw", conf, Size);
				WriteAndReadFile(checkedLocal, "checked", conf, Size);
			}
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Racks(new string[] { "/foo" }).Build();
				cluster.WaitActive();
				FileSystem dfs = cluster.GetFileSystem();
				for (int i_1 = 0; i_1 < reps; ++i_1)
				{
					WriteAndReadFile(dfs, "dfs", conf, Size);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
					// clean up minidfs junk
					rawLocal.Delete(new Path(localDir, "dfs"), true);
				}
			}
			return 0;
		}

		/// <param name="args">arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new HdfsConfiguration(), new BenchmarkThroughput(), args
				);
			System.Environment.Exit(res);
		}
	}
}
