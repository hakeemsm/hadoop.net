using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// MapredLoadTest generates a bunch of work that exercises
	/// a Hadoop Map-Reduce system (and DFS, too).
	/// </summary>
	/// <remarks>
	/// MapredLoadTest generates a bunch of work that exercises
	/// a Hadoop Map-Reduce system (and DFS, too).  It goes through
	/// the following steps:
	/// 1) Take inputs 'range' and 'counts'.
	/// 2) Generate 'counts' random integers between 0 and range-1.
	/// 3) Create a file that lists each integer between 0 and range-1,
	/// and lists the number of times that integer was generated.
	/// 4) Emit a (very large) file that contains all the integers
	/// in the order generated.
	/// 5) After the file has been generated, read it back and count
	/// how many times each int was generated.
	/// 6) Compare this big count-map against the original one.  If
	/// they match, then SUCCESS!  Otherwise, FAILURE!
	/// OK, that's how we can think about it.  What are the map-reduce
	/// steps that get the job done?
	/// 1) In a non-mapred thread, take the inputs 'range' and 'counts'.
	/// 2) In a non-mapread thread, generate the answer-key and write to disk.
	/// 3) In a mapred job, divide the answer key into K jobs.
	/// 4) A mapred 'generator' task consists of K map jobs.  Each reads
	/// an individual "sub-key", and generates integers according to
	/// to it (though with a random ordering).
	/// 5) The generator's reduce task agglomerates all of those files
	/// into a single one.
	/// 6) A mapred 'reader' task consists of M map jobs.  The output
	/// file is cut into M pieces. Each of the M jobs counts the
	/// individual ints in its chunk and creates a map of all seen ints.
	/// 7) A mapred job integrates all the count files into a single one.
	/// </remarks>
	public class TestMapRed : Configured, Tool
	{
		/// <summary>Modified to make it a junit test.</summary>
		/// <remarks>
		/// Modified to make it a junit test.
		/// The RandomGen Job does the actual work of creating
		/// a huge file of assorted numbers.  It receives instructions
		/// as to how many times each number should be counted.  Then
		/// it emits those numbers in a crazy order.
		/// The map() function takes a key/val pair that describes
		/// a value-to-be-emitted (the key) and how many times it
		/// should be emitted (the value), aka "numtimes".  map() then
		/// emits a series of intermediate key/val pairs.  It emits
		/// 'numtimes' of these.  The key is a random number and the
		/// value is the 'value-to-be-emitted'.
		/// The system collates and merges these pairs according to
		/// the random number.  reduce() function takes in a key/value
		/// pair that consists of a crazy random number and a series
		/// of values that should be emitted.  The random number key
		/// is now dropped, and reduce() emits a pair for every intermediate value.
		/// The emitted key is an intermediate value.  The emitted value
		/// is just a blank string.  Thus, we've created a huge file
		/// of numbers in random order, but where each number appears
		/// as many times as we were instructed.
		/// </remarks>
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestMapRed-mapred");

		internal class RandomGenMapper : Mapper<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(IntWritable key, IntWritable val, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				int randomVal = key.Get();
				int randomCount = val.Get();
				for (int i = 0; i < randomCount; i++)
				{
					@out.Collect(new IntWritable(Math.Abs(r.Next())), new IntWritable(randomVal));
				}
			}

			public virtual void Close()
			{
			}
		}

		internal class RandomGenReducer : Reducer<IntWritable, IntWritable, IntWritable, 
			IntWritable>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> it, OutputCollector
				<IntWritable, IntWritable> @out, Reporter reporter)
			{
				while (it.HasNext())
				{
					@out.Collect(it.Next(), null);
				}
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>The RandomCheck Job does a lot of our work.</summary>
		/// <remarks>
		/// The RandomCheck Job does a lot of our work.  It takes
		/// in a num/string keyspace, and transforms it into a
		/// key/count(int) keyspace.
		/// The map() function just emits a num/1 pair for every
		/// num/string input pair.
		/// The reduce() function sums up all the 1s that were
		/// emitted for a single key.  It then emits the key/total
		/// pair.
		/// This is used to regenerate the random number "answer key".
		/// Each key here is a random number, and the count is the
		/// number of times the number was emitted.
		/// </remarks>
		internal class RandomCheckMapper : Mapper<WritableComparable, Text, IntWritable, 
			IntWritable>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Text val, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				@out.Collect(new IntWritable(System.Convert.ToInt32(val.ToString().Trim())), new 
					IntWritable(1));
			}

			public virtual void Close()
			{
			}
		}

		internal class RandomCheckReducer : Reducer<IntWritable, IntWritable, IntWritable
			, IntWritable>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> it, OutputCollector
				<IntWritable, IntWritable> @out, Reporter reporter)
			{
				int keyint = key.Get();
				int count = 0;
				while (it.HasNext())
				{
					it.Next();
					count++;
				}
				@out.Collect(new IntWritable(keyint), new IntWritable(count));
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>The Merge Job is a really simple one.</summary>
		/// <remarks>
		/// The Merge Job is a really simple one.  It takes in
		/// an int/int key-value set, and emits the same set.
		/// But it merges identical keys by adding their values.
		/// Thus, the map() function is just the identity function
		/// and reduce() just sums.  Nothing to see here!
		/// </remarks>
		internal class MergeMapper : Mapper<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(IntWritable key, IntWritable val, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				int keyint = key.Get();
				int valint = val.Get();
				@out.Collect(new IntWritable(keyint), new IntWritable(valint));
			}

			public virtual void Close()
			{
			}
		}

		internal class MergeReducer : Reducer<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> it, OutputCollector
				<IntWritable, IntWritable> @out, Reporter reporter)
			{
				int keyint = key.Get();
				int total = 0;
				while (it.HasNext())
				{
					total += it.Next().Get();
				}
				@out.Collect(new IntWritable(keyint), new IntWritable(total));
			}

			public virtual void Close()
			{
			}
		}

		private static int range = 10;

		private static int counts = 100;

		private static Random r = new Random();

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(TestDir);
		}

		/// <summary>
		/// public TestMapRed(int range, int counts, Configuration conf) throws IOException {
		/// this.range = range;
		/// this.counts = counts;
		/// this.conf = conf;
		/// }
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapred()
		{
			Launch();
		}

		private class MyMap : Mapper<WritableComparable, Text, Text, Text>
		{
			public virtual void Configure(JobConf conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Text value, OutputCollector<Text, 
				Text> output, Reporter reporter)
			{
				string str = StringUtils.ToLowerCase(value.ToString());
				output.Collect(new Text(str), value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		private class MyReduce : IdentityReducer
		{
			private JobConf conf;

			private bool compressInput;

			private bool first = true;

			public override void Configure(JobConf conf)
			{
				this.conf = conf;
				compressInput = conf.GetCompressMapOutput();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(WritableComparable key, IEnumerator values, OutputCollector
				 output, Reporter reporter)
			{
				if (first)
				{
					first = false;
					MapOutputFile mapOutputFile = new MROutputFiles();
					mapOutputFile.SetConf(conf);
					Path input = mapOutputFile.GetInputFile(0);
					FileSystem fs = FileSystem.Get(conf);
					NUnit.Framework.Assert.IsTrue("reduce input exists " + input, fs.Exists(input));
					SequenceFile.Reader rdr = new SequenceFile.Reader(fs, input, conf);
					NUnit.Framework.Assert.AreEqual("is reduce input compressed " + input, compressInput
						, rdr.IsCompressed());
					rdr.Close();
				}
			}
		}

		public class NullMapper : Mapper<NullWritable, Text, NullWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(NullWritable key, Text val, OutputCollector<NullWritable, 
				Text> output, Reporter reporter)
			{
				output.Collect(NullWritable.Get(), val);
			}

			public virtual void Configure(JobConf conf)
			{
			}

			public virtual void Close()
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNullKeys()
		{
			JobConf conf = new JobConf(typeof(TestMapRed));
			FileSystem fs = FileSystem.GetLocal(conf);
			HashSet<string> values = new HashSet<string>();
			string m = "AAAAAAAAAAAAAA";
			for (int i = 1; i < 11; ++i)
			{
				values.AddItem(m);
				m = m.Replace((char)('A' + i - 1), (char)('A' + i));
			}
			Path testdir = new Path(Runtime.GetProperty("test.build.data", "/tmp")).MakeQualified
				(fs);
			fs.Delete(testdir, true);
			Path inFile = new Path(testdir, "nullin/blah");
			SequenceFile.Writer w = SequenceFile.CreateWriter(fs, conf, inFile, typeof(NullWritable
				), typeof(Text), SequenceFile.CompressionType.None);
			Text t = new Text();
			foreach (string s in values)
			{
				t.Set(s);
				w.Append(NullWritable.Get(), t);
			}
			w.Close();
			FileInputFormat.SetInputPaths(conf, inFile);
			FileOutputFormat.SetOutputPath(conf, new Path(testdir, "nullout"));
			conf.SetMapperClass(typeof(TestMapRed.NullMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			conf.SetOutputKeyClass(typeof(NullWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetInputFormat(typeof(SequenceFileInputFormat));
			conf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			conf.SetNumReduceTasks(1);
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			JobClient.RunJob(conf);
			// Since null keys all equal, allow any ordering
			SequenceFile.Reader r = new SequenceFile.Reader(fs, new Path(testdir, "nullout/part-00000"
				), conf);
			m = "AAAAAAAAAAAAAA";
			for (int i_1 = 1; r.Next(NullWritable.Get(), t); ++i_1)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected value: " + t, values.Remove(t.ToString(
					)));
				m = m.Replace((char)('A' + i_1 - 1), (char)('A' + i_1));
			}
			NUnit.Framework.Assert.IsTrue("Missing values: " + values.ToString(), values.IsEmpty
				());
		}

		/// <exception cref="System.Exception"/>
		private void CheckCompression(bool compressMapOutputs, SequenceFile.CompressionType
			 redCompression, bool includeCombine)
		{
			JobConf conf = new JobConf(typeof(TestMapRed));
			Path testdir = new Path(TestDir.GetAbsolutePath());
			Path inDir = new Path(testdir, "in");
			Path outDir = new Path(testdir, "out");
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(testdir, true);
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetMapperClass(typeof(TestMapRed.MyMap));
			conf.SetReducerClass(typeof(TestMapRed.MyReduce));
			conf.SetOutputKeyClass(typeof(Text));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			if (includeCombine)
			{
				conf.SetCombinerClass(typeof(IdentityReducer));
			}
			conf.SetCompressMapOutput(compressMapOutputs);
			SequenceFileOutputFormat.SetOutputCompressionType(conf, redCompression);
			try
			{
				if (!fs.Mkdirs(testdir))
				{
					throw new IOException("Mkdirs failed to create " + testdir.ToString());
				}
				if (!fs.Mkdirs(inDir))
				{
					throw new IOException("Mkdirs failed to create " + inDir.ToString());
				}
				Path inFile = new Path(inDir, "part0");
				DataOutputStream f = fs.Create(inFile);
				f.WriteBytes("Owen was here\n");
				f.WriteBytes("Hadoop is fun\n");
				f.WriteBytes("Is this done, yet?\n");
				f.Close();
				RunningJob rj = JobClient.RunJob(conf);
				NUnit.Framework.Assert.IsTrue("job was complete", rj.IsComplete());
				NUnit.Framework.Assert.IsTrue("job was successful", rj.IsSuccessful());
				Path output = new Path(outDir, Task.GetOutputName(0));
				NUnit.Framework.Assert.IsTrue("reduce output exists " + output, fs.Exists(output)
					);
				SequenceFile.Reader rdr = new SequenceFile.Reader(fs, output, conf);
				NUnit.Framework.Assert.AreEqual("is reduce output compressed " + output, redCompression
					 != SequenceFile.CompressionType.None, rdr.IsCompressed());
				rdr.Close();
			}
			finally
			{
				fs.Delete(testdir, true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCompression()
		{
			EnumSet<SequenceFile.CompressionType> seq = EnumSet.AllOf<SequenceFile.CompressionType
				>();
			foreach (SequenceFile.CompressionType redCompression in seq)
			{
				for (int combine = 0; combine < 2; ++combine)
				{
					CheckCompression(false, redCompression, combine == 1);
					CheckCompression(true, redCompression, combine == 1);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void Launch()
		{
			//
			// Generate distribution of ints.  This is the answer key.
			//
			JobConf conf;
			//Check to get configuration and check if it is configured thro' Configured
			//interface. This would happen when running testcase thro' command line.
			if (GetConf() == null)
			{
				conf = new JobConf();
			}
			else
			{
				conf = new JobConf(GetConf());
			}
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			conf.SetJarByClass(typeof(TestMapRed));
			int countsToGo = counts;
			int[] dist = new int[range];
			for (int i = 0; i < range; i++)
			{
				double avgInts = (1.0 * countsToGo) / (range - i);
				dist[i] = (int)Math.Max(0, Math.Round(avgInts + (Math.Sqrt(avgInts) * r.NextGaussian
					())));
				countsToGo -= dist[i];
			}
			if (countsToGo > 0)
			{
				dist[dist.Length - 1] += countsToGo;
			}
			//
			// Write the answer key to a file.  
			//
			FileSystem fs = FileSystem.Get(conf);
			Path testdir = new Path(TestDir.GetAbsolutePath(), "mapred.loadtest");
			if (!fs.Mkdirs(testdir))
			{
				throw new IOException("Mkdirs failed to create " + testdir.ToString());
			}
			Path randomIns = new Path(testdir, "genins");
			if (!fs.Mkdirs(randomIns))
			{
				throw new IOException("Mkdirs failed to create " + randomIns.ToString());
			}
			Path answerkey = new Path(randomIns, "answer.key");
			SequenceFile.Writer @out = SequenceFile.CreateWriter(fs, conf, answerkey, typeof(
				IntWritable), typeof(IntWritable), SequenceFile.CompressionType.None);
			try
			{
				for (int i_1 = 0; i_1 < range; i_1++)
				{
					@out.Append(new IntWritable(i_1), new IntWritable(dist[i_1]));
				}
			}
			finally
			{
				@out.Close();
			}
			//printFiles(randomIns, conf);
			//
			// Now we need to generate the random numbers according to
			// the above distribution.
			//
			// We create a lot of map tasks, each of which takes at least
			// one "line" of the distribution.  (That is, a certain number
			// X is to be generated Y number of times.)
			//
			// A map task emits Y key/val pairs.  The val is X.  The key
			// is a randomly-generated number.
			//
			// The reduce task gets its input sorted by key.  That is, sorted
			// in random order.  It then emits a single line of text that
			// for the given values.  It does not emit the key.
			//
			// Because there's just one reduce task, we emit a single big
			// file of random numbers.
			//
			Path randomOuts = new Path(testdir, "genouts");
			fs.Delete(randomOuts, true);
			JobConf genJob = new JobConf(conf, typeof(TestMapRed));
			FileInputFormat.SetInputPaths(genJob, randomIns);
			genJob.SetInputFormat(typeof(SequenceFileInputFormat));
			genJob.SetMapperClass(typeof(TestMapRed.RandomGenMapper));
			FileOutputFormat.SetOutputPath(genJob, randomOuts);
			genJob.SetOutputKeyClass(typeof(IntWritable));
			genJob.SetOutputValueClass(typeof(IntWritable));
			genJob.SetOutputFormat(typeof(TextOutputFormat));
			genJob.SetReducerClass(typeof(TestMapRed.RandomGenReducer));
			genJob.SetNumReduceTasks(1);
			JobClient.RunJob(genJob);
			//printFiles(randomOuts, conf);
			//
			// Next, we read the big file in and regenerate the 
			// original map.  It's split into a number of parts.
			// (That number is 'intermediateReduces'.)
			//
			// We have many map tasks, each of which read at least one
			// of the output numbers.  For each number read in, the
			// map task emits a key/value pair where the key is the
			// number and the value is "1".
			//
			// We have a single reduce task, which receives its input
			// sorted by the key emitted above.  For each key, there will
			// be a certain number of "1" values.  The reduce task sums
			// these values to compute how many times the given key was
			// emitted.
			//
			// The reduce task then emits a key/val pair where the key
			// is the number in question, and the value is the number of
			// times the key was emitted.  This is the same format as the
			// original answer key (except that numbers emitted zero times
			// will not appear in the regenerated key.)  The answer set
			// is split into a number of pieces.  A final MapReduce job
			// will merge them.
			//
			// There's not really a need to go to 10 reduces here 
			// instead of 1.  But we want to test what happens when
			// you have multiple reduces at once.
			//
			int intermediateReduces = 10;
			Path intermediateOuts = new Path(testdir, "intermediateouts");
			fs.Delete(intermediateOuts, true);
			JobConf checkJob = new JobConf(conf, typeof(TestMapRed));
			FileInputFormat.SetInputPaths(checkJob, randomOuts);
			checkJob.SetInputFormat(typeof(TextInputFormat));
			checkJob.SetMapperClass(typeof(TestMapRed.RandomCheckMapper));
			FileOutputFormat.SetOutputPath(checkJob, intermediateOuts);
			checkJob.SetOutputKeyClass(typeof(IntWritable));
			checkJob.SetOutputValueClass(typeof(IntWritable));
			checkJob.SetOutputFormat(typeof(MapFileOutputFormat));
			checkJob.SetReducerClass(typeof(TestMapRed.RandomCheckReducer));
			checkJob.SetNumReduceTasks(intermediateReduces);
			JobClient.RunJob(checkJob);
			//printFiles(intermediateOuts, conf); 
			//
			// OK, now we take the output from the last job and
			// merge it down to a single file.  The map() and reduce()
			// functions don't really do anything except reemit tuples.
			// But by having a single reduce task here, we end up merging
			// all the files.
			//
			Path finalOuts = new Path(testdir, "finalouts");
			fs.Delete(finalOuts, true);
			JobConf mergeJob = new JobConf(conf, typeof(TestMapRed));
			FileInputFormat.SetInputPaths(mergeJob, intermediateOuts);
			mergeJob.SetInputFormat(typeof(SequenceFileInputFormat));
			mergeJob.SetMapperClass(typeof(TestMapRed.MergeMapper));
			FileOutputFormat.SetOutputPath(mergeJob, finalOuts);
			mergeJob.SetOutputKeyClass(typeof(IntWritable));
			mergeJob.SetOutputValueClass(typeof(IntWritable));
			mergeJob.SetOutputFormat(typeof(SequenceFileOutputFormat));
			mergeJob.SetReducerClass(typeof(TestMapRed.MergeReducer));
			mergeJob.SetNumReduceTasks(1);
			JobClient.RunJob(mergeJob);
			//printFiles(finalOuts, conf); 
			//
			// Finally, we compare the reconstructed answer key with the
			// original one.  Remember, we need to ignore zero-count items
			// in the original key.
			//
			bool success = true;
			Path recomputedkey = new Path(finalOuts, "part-00000");
			SequenceFile.Reader @in = new SequenceFile.Reader(fs, recomputedkey, conf);
			int totalseen = 0;
			try
			{
				IntWritable key = new IntWritable();
				IntWritable val = new IntWritable();
				for (int i_1 = 0; i_1 < range; i_1++)
				{
					if (dist[i_1] == 0)
					{
						continue;
					}
					if (!@in.Next(key, val))
					{
						System.Console.Error.WriteLine("Cannot read entry " + i_1);
						success = false;
						break;
					}
					else
					{
						if (!((key.Get() == i_1) && (val.Get() == dist[i_1])))
						{
							System.Console.Error.WriteLine("Mismatch!  Pos=" + key.Get() + ", i=" + i_1 + ", val="
								 + val.Get() + ", dist[i]=" + dist[i_1]);
							success = false;
						}
						totalseen += val.Get();
					}
				}
				if (success)
				{
					if (@in.Next(key, val))
					{
						System.Console.Error.WriteLine("Unnecessary lines in recomputed key!");
						success = false;
					}
				}
			}
			finally
			{
				@in.Close();
			}
			int originalTotal = 0;
			foreach (int aDist in dist)
			{
				originalTotal += aDist;
			}
			System.Console.Out.WriteLine("Original sum: " + originalTotal);
			System.Console.Out.WriteLine("Recomputed sum: " + totalseen);
			//
			// Write to "results" whether the test succeeded or not.
			//
			Path resultFile = new Path(testdir, "results");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.Create(resultFile
				)));
			try
			{
				bw.Write("Success=" + success + "\n");
				System.Console.Out.WriteLine("Success=" + success);
			}
			finally
			{
				bw.Close();
			}
			NUnit.Framework.Assert.IsTrue("testMapRed failed", success);
			fs.Delete(testdir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void PrintTextFile(FileSystem fs, Path p)
		{
			BufferedReader @in = new BufferedReader(new InputStreamReader(fs.Open(p)));
			string line;
			while ((line = @in.ReadLine()) != null)
			{
				System.Console.Out.WriteLine("  Row: " + line);
			}
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void PrintSequenceFile(FileSystem fs, Path p, Configuration conf)
		{
			SequenceFile.Reader r = new SequenceFile.Reader(fs, p, conf);
			object key = null;
			object value = null;
			while ((key = r.Next(key)) != null)
			{
				value = r.GetCurrentValue(value);
				System.Console.Out.WriteLine("  Row: " + key + ", " + value);
			}
			r.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static bool IsSequenceFile(FileSystem fs, Path f)
		{
			DataInputStream @in = fs.Open(f);
			byte[] seq = Sharpen.Runtime.GetBytesForString("SEQ");
			for (int i = 0; i < seq.Length; ++i)
			{
				if (seq[i] != @in.Read())
				{
					return false;
				}
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void PrintFiles(Path dir, Configuration conf)
		{
			FileSystem fs = dir.GetFileSystem(conf);
			foreach (FileStatus f in fs.ListStatus(dir))
			{
				System.Console.Out.WriteLine("Reading " + f.GetPath() + ": ");
				if (f.IsDirectory())
				{
					System.Console.Out.WriteLine("  it is a map file.");
					PrintSequenceFile(fs, new Path(f.GetPath(), "data"), conf);
				}
				else
				{
					if (IsSequenceFile(fs, f.GetPath()))
					{
						System.Console.Out.WriteLine("  it is a sequence file.");
						PrintSequenceFile(fs, f.GetPath(), conf);
					}
					else
					{
						System.Console.Out.WriteLine("  it is a text file.");
						PrintTextFile(fs, f.GetPath());
					}
				}
			}
		}

		/// <summary>Launches all the tasks in order.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new TestMapRed(), argv);
			System.Environment.Exit(res);
		}

		[NUnit.Framework.Test]
		public virtual void TestSmallInput()
		{
			RunJob(100);
		}

		[NUnit.Framework.Test]
		public virtual void TestBiggerInput()
		{
			RunJob(1000);
		}

		public virtual void RunJob(int items)
		{
			try
			{
				JobConf conf = new JobConf(typeof(TestMapRed));
				Path testdir = new Path(TestDir.GetAbsolutePath());
				Path inDir = new Path(testdir, "in");
				Path outDir = new Path(testdir, "out");
				FileSystem fs = FileSystem.Get(conf);
				fs.Delete(testdir, true);
				conf.SetInt(JobContext.IoSortMb, 1);
				conf.SetInputFormat(typeof(SequenceFileInputFormat));
				FileInputFormat.SetInputPaths(conf, inDir);
				FileOutputFormat.SetOutputPath(conf, outDir);
				conf.SetMapperClass(typeof(IdentityMapper));
				conf.SetReducerClass(typeof(IdentityReducer));
				conf.SetOutputKeyClass(typeof(Text));
				conf.SetOutputValueClass(typeof(Text));
				conf.SetOutputFormat(typeof(SequenceFileOutputFormat));
				conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
				if (!fs.Mkdirs(testdir))
				{
					throw new IOException("Mkdirs failed to create " + testdir.ToString());
				}
				if (!fs.Mkdirs(inDir))
				{
					throw new IOException("Mkdirs failed to create " + inDir.ToString());
				}
				Path inFile = new Path(inDir, "part0");
				SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, inFile, typeof(Text
					), typeof(Text));
				StringBuilder content = new StringBuilder();
				for (int i = 0; i < 1000; i++)
				{
					content.Append(i).Append(": This is one more line of content\n");
				}
				Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text(content.ToString()
					);
				for (int i_1 = 0; i_1 < items; i_1++)
				{
					writer.Append(new Org.Apache.Hadoop.IO.Text("rec:" + i_1), text);
				}
				writer.Close();
				JobClient.RunJob(conf);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Threw exception:" + e, false);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			if (argv.Length < 2)
			{
				System.Console.Error.WriteLine("Usage: TestMapRed <range> <counts>");
				System.Console.Error.WriteLine();
				System.Console.Error.WriteLine("Note: a good test will have a " + "<counts> value that is substantially larger than the <range>"
					);
				return -1;
			}
			int i = 0;
			range = System.Convert.ToInt32(argv[i++]);
			counts = System.Convert.ToInt32(argv[i++]);
			Launch();
			return 0;
		}
	}
}
