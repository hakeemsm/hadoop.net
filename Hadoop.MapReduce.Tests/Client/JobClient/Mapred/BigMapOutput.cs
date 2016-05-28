using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class BigMapOutput : Configured, Tool
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(BigMapOutput).FullName);

		private static Random random = new Random();

		public static string MinKey = "mapreduce.bmo.minkey";

		public static string MinValue = "mapreduce.bmo.minvalue";

		public static string MaxKey = "mapreduce.bmo.maxkey";

		public static string MaxValue = "mapreduce.bmo.maxvalue";

		private static void RandomizeBytes(byte[] data, int offset, int length)
		{
			for (int i = offset + length - 1; i >= offset; --i)
			{
				data[i] = unchecked((byte)random.Next(256));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateBigMapInputFile(Configuration conf, FileSystem fs, Path
			 dir, long fileSizeInMB)
		{
			// Check if the input path exists and is non-empty
			if (fs.Exists(dir))
			{
				FileStatus[] list = fs.ListStatus(dir);
				if (list.Length > 0)
				{
					throw new IOException("Input path: " + dir + " already exists... ");
				}
			}
			Path file = new Path(dir, "part-0");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(BytesWritable
				), typeof(BytesWritable), SequenceFile.CompressionType.None);
			long numBytesToWrite = fileSizeInMB * 1024 * 1024;
			int minKeySize = conf.GetInt(MinKey, 10);
			int keySizeRange = conf.GetInt(MaxKey, 1000) - minKeySize;
			int minValueSize = conf.GetInt(MinValue, 0);
			int valueSizeRange = conf.GetInt(MaxValue, 20000) - minValueSize;
			BytesWritable randomKey = new BytesWritable();
			BytesWritable randomValue = new BytesWritable();
			Log.Info("Writing " + numBytesToWrite + " bytes to " + file + " with " + "minKeySize: "
				 + minKeySize + " keySizeRange: " + keySizeRange + " minValueSize: " + minValueSize
				 + " valueSizeRange: " + valueSizeRange);
			long start = Runtime.CurrentTimeMillis();
			while (numBytesToWrite > 0)
			{
				int keyLength = minKeySize + (keySizeRange != 0 ? random.Next(keySizeRange) : 0);
				randomKey.SetSize(keyLength);
				RandomizeBytes(randomKey.GetBytes(), 0, randomKey.GetLength());
				int valueLength = minValueSize + (valueSizeRange != 0 ? random.Next(valueSizeRange
					) : 0);
				randomValue.SetSize(valueLength);
				RandomizeBytes(randomValue.GetBytes(), 0, randomValue.GetLength());
				writer.Append(randomKey, randomValue);
				numBytesToWrite -= keyLength + valueLength;
			}
			writer.Close();
			long end = Runtime.CurrentTimeMillis();
			Log.Info("Created " + file + " of size: " + fileSizeInMB + "MB in " + (end - start
				) / 1000 + "secs");
		}

		private static void Usage()
		{
			System.Console.Error.WriteLine("BigMapOutput -input <input-dir> -output <output-dir> "
				 + "[-create <filesize in MB>]");
			ToolRunner.PrintGenericCommandUsage(System.Console.Error);
			System.Environment.Exit(1);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 4)
			{
				//input-dir should contain a huge file ( > 2GB)
				Usage();
			}
			Path bigMapInput = null;
			Path outputPath = null;
			bool createInput = false;
			long fileSizeInMB = 3 * 1024;
			// default of 3GB (>2GB)
			for (int i = 0; i < args.Length; ++i)
			{
				if ("-input".Equals(args[i]))
				{
					bigMapInput = new Path(args[++i]);
				}
				else
				{
					if ("-output".Equals(args[i]))
					{
						outputPath = new Path(args[++i]);
					}
					else
					{
						if ("-create".Equals(args[i]))
						{
							createInput = true;
							fileSizeInMB = long.Parse(args[++i]);
						}
						else
						{
							Usage();
						}
					}
				}
			}
			FileSystem fs = FileSystem.Get(GetConf());
			JobConf jobConf = new JobConf(GetConf(), typeof(BigMapOutput));
			jobConf.SetJobName("BigMapOutput");
			jobConf.SetInputFormat(typeof(SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat
				));
			jobConf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			FileInputFormat.SetInputPaths(jobConf, bigMapInput);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			FileOutputFormat.SetOutputPath(jobConf, outputPath);
			jobConf.SetMapperClass(typeof(IdentityMapper));
			jobConf.SetReducerClass(typeof(IdentityReducer));
			jobConf.SetOutputKeyClass(typeof(BytesWritable));
			jobConf.SetOutputValueClass(typeof(BytesWritable));
			if (createInput)
			{
				CreateBigMapInputFile(jobConf, fs, bigMapInput, fileSizeInMB);
			}
			DateTime startTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			JobClient.RunJob(jobConf);
			DateTime end_time = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + end_time);
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new Configuration(), new BigMapOutput(), argv);
			System.Environment.Exit(res);
		}
	}
}
