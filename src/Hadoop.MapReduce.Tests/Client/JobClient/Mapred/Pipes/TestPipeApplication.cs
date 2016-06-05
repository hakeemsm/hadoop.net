using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class TestPipeApplication
	{
		private static FilePath workSpace = new FilePath("target", typeof(TestPipeApplication
			).FullName + "-workSpace");

		private static string taskName = "attempt_001_02_r03_04_05";

		/// <summary>test PipesMapRunner    test the transfer data from reader</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRunner()
		{
			// clean old password files
			FilePath[] psw = CleanTokenPasswordFile();
			try
			{
				RecordReader<FloatWritable, NullWritable> rReader = new TestPipeApplication.ReaderPipesMapRunner
					(this);
				JobConf conf = new JobConf();
				conf.Set(Submitter.IsJavaRr, "true");
				// for stdour and stderror
				conf.Set(MRJobConfig.TaskAttemptId, taskName);
				TestPipeApplication.CombineOutputCollector<IntWritable, Text> output = new TestPipeApplication.CombineOutputCollector
					<IntWritable, Text>(this, new Counters.Counter(), new TestPipeApplication.Progress
					(this));
				FileSystem fs = new RawLocalFileSystem();
				fs.SetConf(conf);
				IFile.Writer<IntWritable, Text> wr = new IFile.Writer<IntWritable, Text>(conf, fs
					.Create(new Path(workSpace + FilePath.separator + "outfile")), typeof(IntWritable
					), typeof(Text), null, null, true);
				output.SetWriter(wr);
				// stub for client
				FilePath fCommand = GetFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicationRunnableStub"
					);
				conf.Set(MRJobConfig.CacheLocalfiles, fCommand.GetAbsolutePath());
				// token for authorization
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<AMRMTokenIdentifier>(Sharpen.Runtime.GetBytesForString("user"), Sharpen.Runtime.GetBytesForString
					("password"), new Text("kind"), new Text("service"));
				TokenCache.SetJobToken(token, conf.GetCredentials());
				conf.SetBoolean(MRJobConfig.SkipRecords, true);
				TestPipeApplication.TestTaskReporter reporter = new TestPipeApplication.TestTaskReporter
					(this);
				PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text> runner = new PipesMapRunner
					<FloatWritable, NullWritable, IntWritable, Text>();
				InitStdOut(conf);
				runner.Configure(conf);
				runner.Run(rReader, output, reporter);
				string stdOut = ReadStdOut(conf);
				// test part of translated data. As common file for client and test -
				// clients stdOut
				// check version
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("CURRENT_PROTOCOL_VERSION:0"));
				// check key and value classes
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("Key class:org.apache.hadoop.io.FloatWritable"
					));
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("Value class:org.apache.hadoop.io.NullWritable"
					));
				// test have sent all data from reader
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("value:0.0"));
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("value:9.0"));
			}
			finally
			{
				if (psw != null)
				{
					// remove password files
					foreach (FilePath file in psw)
					{
						file.DeleteOnExit();
					}
				}
			}
		}

		/// <summary>
		/// test org.apache.hadoop.mapred.pipes.Application
		/// test a internal functions: MessageType.REGISTER_COUNTER,  INCREMENT_COUNTER, STATUS, PROGRESS...
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplication()
		{
			JobConf conf = new JobConf();
			RecordReader<FloatWritable, NullWritable> rReader = new TestPipeApplication.Reader
				(this);
			// client for test
			FilePath fCommand = GetFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicationStub"
				);
			TestPipeApplication.TestTaskReporter reporter = new TestPipeApplication.TestTaskReporter
				(this);
			FilePath[] psw = CleanTokenPasswordFile();
			try
			{
				conf.Set(MRJobConfig.TaskAttemptId, taskName);
				conf.Set(MRJobConfig.CacheLocalfiles, fCommand.GetAbsolutePath());
				// token for authorization
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<AMRMTokenIdentifier>(Sharpen.Runtime.GetBytesForString("user"), Sharpen.Runtime.GetBytesForString
					("password"), new Text("kind"), new Text("service"));
				TokenCache.SetJobToken(token, conf.GetCredentials());
				TestPipeApplication.FakeCollector output = new TestPipeApplication.FakeCollector(
					this, new Counters.Counter(), new TestPipeApplication.Progress(this));
				FileSystem fs = new RawLocalFileSystem();
				fs.SetConf(conf);
				IFile.Writer<IntWritable, Text> wr = new IFile.Writer<IntWritable, Text>(conf, fs
					.Create(new Path(workSpace.GetAbsolutePath() + FilePath.separator + "outfile")), 
					typeof(IntWritable), typeof(Text), null, null, true);
				output.SetWriter(wr);
				conf.Set(Submitter.PreserveCommandfile, "true");
				InitStdOut(conf);
				Application<WritableComparable<IntWritable>, Writable, IntWritable, Text> application
					 = new Application<WritableComparable<IntWritable>, Writable, IntWritable, Text>
					(conf, rReader, output, reporter, typeof(IntWritable), typeof(Text));
				application.GetDownlink().Flush();
				application.GetDownlink().MapItem(new IntWritable(3), new Text("txt"));
				application.GetDownlink().Flush();
				application.WaitForFinish();
				wr.Close();
				// test getDownlink().mapItem();
				string stdOut = ReadStdOut(conf);
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("key:3"));
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("value:txt"));
				// reporter test counter, and status should be sended
				// test MessageType.REGISTER_COUNTER and INCREMENT_COUNTER
				NUnit.Framework.Assert.AreEqual(1.0, reporter.GetProgress(), 0.01);
				NUnit.Framework.Assert.IsNotNull(reporter.GetCounter("group", "name"));
				// test status MessageType.STATUS
				NUnit.Framework.Assert.AreEqual(reporter.GetStatus(), "PROGRESS");
				stdOut = ReadFile(new FilePath(workSpace.GetAbsolutePath() + FilePath.separator +
					 "outfile"));
				// check MessageType.PROGRESS
				NUnit.Framework.Assert.AreEqual(0.55f, rReader.GetProgress(), 0.001);
				application.GetDownlink().Close();
				// test MessageType.OUTPUT
				KeyValuePair<IntWritable, Text> entry = output.GetCollect().GetEnumerator().Next(
					);
				NUnit.Framework.Assert.AreEqual(123, entry.Key.Get());
				NUnit.Framework.Assert.AreEqual("value", entry.Value.ToString());
				try
				{
					// try to abort
					application.Abort(new Exception());
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException e)
				{
					// abort works ?
					NUnit.Framework.Assert.AreEqual("pipe child exception", e.Message);
				}
			}
			finally
			{
				if (psw != null)
				{
					// remove password files
					foreach (FilePath file in psw)
					{
						file.DeleteOnExit();
					}
				}
			}
		}

		/// <summary>test org.apache.hadoop.mapred.pipes.Submitter</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSubmitter()
		{
			JobConf conf = new JobConf();
			FilePath[] psw = CleanTokenPasswordFile();
			Runtime.SetProperty("test.build.data", "target/tmp/build/TEST_SUBMITTER_MAPPER/data"
				);
			conf.Set("hadoop.log.dir", "target/tmp");
			// prepare configuration
			Submitter.SetIsJavaMapper(conf, false);
			Submitter.SetIsJavaReducer(conf, false);
			Submitter.SetKeepCommandFile(conf, false);
			Submitter.SetIsJavaRecordReader(conf, false);
			Submitter.SetIsJavaRecordWriter(conf, false);
			PipesPartitioner<IntWritable, Text> partitioner = new PipesPartitioner<IntWritable
				, Text>();
			partitioner.Configure(conf);
			Submitter.SetJavaPartitioner(conf, partitioner.GetType());
			NUnit.Framework.Assert.AreEqual(typeof(PipesPartitioner), (Submitter.GetJavaPartitioner
				(conf)));
			// test going to call main method with System.exit(). Change Security
			SecurityManager securityManager = Runtime.GetSecurityManager();
			// store System.out
			TextWriter oldps = System.Console.Out;
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			ExitUtil.DisableSystemExit();
			// test without parameters
			try
			{
				Runtime.SetOut(new TextWriter(@out));
				Submitter.Main(new string[0]);
				NUnit.Framework.Assert.Fail();
			}
			catch (ExitUtil.ExitException)
			{
				// System.exit prohibited! output message test
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains(string.Empty));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("bin/hadoop pipes"));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-input <path>] // Input directory"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-output <path>] // Output directory"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-jar <jar file> // jar filename"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-inputformat <class>] // InputFormat class"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-map <class>] // Java Map class"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-partitioner <class>] // Java Partitioner"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-reduce <class>] // Java Reduce class"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-writer <class>] // Java RecordWriter"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-program <executable>] // executable URI"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-reduces <num>] // number of reduces"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("[-lazyOutput <true/false>] // createOutputLazily"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-conf <configuration file>     specify an application configuration file"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-D <property=value>            use value for given property"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-fs <local|namenode:port>      specify a namenode"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-jt <local|resourcemanager:port>    specify a ResourceManager"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster"
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath."
					));
				NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines."
					));
			}
			finally
			{
				Runtime.SetOut(oldps);
				// restore
				Runtime.SetSecurityManager(securityManager);
				if (psw != null)
				{
					// remove password files
					foreach (FilePath file in psw)
					{
						file.DeleteOnExit();
					}
				}
			}
			// test call Submitter form command line
			try
			{
				FilePath fCommand = GetFileCommand(null);
				string[] args = new string[22];
				FilePath input = new FilePath(workSpace + FilePath.separator + "input");
				if (!input.Exists())
				{
					NUnit.Framework.Assert.IsTrue(input.CreateNewFile());
				}
				FilePath outPut = new FilePath(workSpace + FilePath.separator + "output");
				FileUtil.FullyDelete(outPut);
				args[0] = "-input";
				args[1] = input.GetAbsolutePath();
				// "input";
				args[2] = "-output";
				args[3] = outPut.GetAbsolutePath();
				// "output";
				args[4] = "-inputformat";
				args[5] = "org.apache.hadoop.mapred.TextInputFormat";
				args[6] = "-map";
				args[7] = "org.apache.hadoop.mapred.lib.IdentityMapper";
				args[8] = "-partitioner";
				args[9] = "org.apache.hadoop.mapred.pipes.PipesPartitioner";
				args[10] = "-reduce";
				args[11] = "org.apache.hadoop.mapred.lib.IdentityReducer";
				args[12] = "-writer";
				args[13] = "org.apache.hadoop.mapred.TextOutputFormat";
				args[14] = "-program";
				args[15] = fCommand.GetAbsolutePath();
				// "program";
				args[16] = "-reduces";
				args[17] = "2";
				args[18] = "-lazyOutput";
				args[19] = "lazyOutput";
				args[20] = "-jobconf";
				args[21] = "mapreduce.pipes.isjavarecordwriter=false,mapreduce.pipes.isjavarecordreader=false";
				Submitter.Main(args);
				NUnit.Framework.Assert.Fail();
			}
			catch (ExitUtil.ExitException e)
			{
				// status should be 0
				NUnit.Framework.Assert.AreEqual(e.status, 0);
			}
			finally
			{
				Runtime.SetOut(oldps);
				Runtime.SetSecurityManager(securityManager);
			}
		}

		/// <summary>
		/// test org.apache.hadoop.mapred.pipes.PipesReducer
		/// test the transfer of data: key and value
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPipesReduser()
		{
			FilePath[] psw = CleanTokenPasswordFile();
			JobConf conf = new JobConf();
			try
			{
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<AMRMTokenIdentifier>(Sharpen.Runtime.GetBytesForString("user"), Sharpen.Runtime.GetBytesForString
					("password"), new Text("kind"), new Text("service"));
				TokenCache.SetJobToken(token, conf.GetCredentials());
				FilePath fCommand = GetFileCommand("org.apache.hadoop.mapred.pipes.PipeReducerStub"
					);
				conf.Set(MRJobConfig.CacheLocalfiles, fCommand.GetAbsolutePath());
				PipesReducer<BooleanWritable, Text, IntWritable, Text> reducer = new PipesReducer
					<BooleanWritable, Text, IntWritable, Text>();
				reducer.Configure(conf);
				BooleanWritable bw = new BooleanWritable(true);
				conf.Set(MRJobConfig.TaskAttemptId, taskName);
				InitStdOut(conf);
				conf.SetBoolean(MRJobConfig.SkipRecords, true);
				TestPipeApplication.CombineOutputCollector<IntWritable, Text> output = new TestPipeApplication.CombineOutputCollector
					<IntWritable, Text>(this, new Counters.Counter(), new TestPipeApplication.Progress
					(this));
				Reporter reporter = new TestPipeApplication.TestTaskReporter(this);
				IList<Text> texts = new AList<Text>();
				texts.AddItem(new Text("first"));
				texts.AddItem(new Text("second"));
				texts.AddItem(new Text("third"));
				reducer.Reduce(bw, texts.GetEnumerator(), output, reporter);
				reducer.Close();
				string stdOut = ReadStdOut(conf);
				// test data: key
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("reducer key :true"));
				// and values
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("reduce value  :first"));
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("reduce value  :second"));
				NUnit.Framework.Assert.IsTrue(stdOut.Contains("reduce value  :third"));
			}
			finally
			{
				if (psw != null)
				{
					// remove password files
					foreach (FilePath file in psw)
					{
						file.DeleteOnExit();
					}
				}
			}
		}

		/// <summary>
		/// test PipesPartitioner
		/// test set and get data from  PipesPartitioner
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestPipesPartitioner()
		{
			PipesPartitioner<IntWritable, Text> partitioner = new PipesPartitioner<IntWritable
				, Text>();
			JobConf configuration = new JobConf();
			Submitter.GetJavaPartitioner(configuration);
			partitioner.Configure(new JobConf());
			IntWritable iw = new IntWritable(4);
			// the cache empty
			NUnit.Framework.Assert.AreEqual(0, partitioner.GetPartition(iw, new Text("test"), 
				2));
			// set data into cache
			PipesPartitioner.SetNextPartition(3);
			// get data from cache
			NUnit.Framework.Assert.AreEqual(3, partitioner.GetPartition(iw, new Text("test"), 
				2));
		}

		/// <summary>clean previous std error and outs</summary>
		private void InitStdOut(JobConf configuration)
		{
			TaskAttemptID taskId = ((TaskAttemptID)TaskAttemptID.ForName(configuration.Get(MRJobConfig
				.TaskAttemptId)));
			FilePath stdOut = TaskLog.GetTaskLogFile(taskId, false, TaskLog.LogName.Stdout);
			FilePath stdErr = TaskLog.GetTaskLogFile(taskId, false, TaskLog.LogName.Stderr);
			// prepare folder
			if (!stdOut.GetParentFile().Exists())
			{
				stdOut.GetParentFile().Mkdirs();
			}
			else
			{
				// clean logs
				stdOut.DeleteOnExit();
				stdErr.DeleteOnExit();
			}
		}

		/// <exception cref="System.Exception"/>
		private string ReadStdOut(JobConf conf)
		{
			TaskAttemptID taskId = ((TaskAttemptID)TaskAttemptID.ForName(conf.Get(MRJobConfig
				.TaskAttemptId)));
			FilePath stdOut = TaskLog.GetTaskLogFile(taskId, false, TaskLog.LogName.Stdout);
			return ReadFile(stdOut);
		}

		/// <exception cref="System.Exception"/>
		private string ReadFile(FilePath file)
		{
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			InputStream @is = new FileInputStream(file);
			byte[] buffer = new byte[1024];
			int counter = 0;
			while ((counter = @is.Read(buffer)) >= 0)
			{
				@out.Write(buffer, 0, counter);
			}
			@is.Close();
			return @out.ToString();
		}

		private class Progress : Progressable
		{
			public virtual void Progress()
			{
			}

			internal Progress(TestPipeApplication _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestPipeApplication _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private FilePath[] CleanTokenPasswordFile()
		{
			FilePath[] result = new FilePath[2];
			result[0] = new FilePath("./jobTokenPassword");
			if (result[0].Exists())
			{
				FileUtil.Chmod(result[0].GetAbsolutePath(), "700");
				NUnit.Framework.Assert.IsTrue(result[0].Delete());
			}
			result[1] = new FilePath("./.jobTokenPassword.crc");
			if (result[1].Exists())
			{
				FileUtil.Chmod(result[1].GetAbsolutePath(), "700");
				result[1].Delete();
			}
			return result;
		}

		/// <exception cref="System.Exception"/>
		private FilePath GetFileCommand(string clazz)
		{
			string classpath = Runtime.GetProperty("java.class.path");
			FilePath fCommand = new FilePath(workSpace + FilePath.separator + "cache.sh");
			fCommand.DeleteOnExit();
			if (!fCommand.GetParentFile().Exists())
			{
				fCommand.GetParentFile().Mkdirs();
			}
			fCommand.CreateNewFile();
			OutputStream os = new FileOutputStream(fCommand);
			os.Write(Sharpen.Runtime.GetBytesForString("#!/bin/sh \n"));
			if (clazz == null)
			{
				os.Write(Sharpen.Runtime.GetBytesForString(("ls ")));
			}
			else
			{
				os.Write(Sharpen.Runtime.GetBytesForString(("java -cp " + classpath + " " + clazz
					)));
			}
			os.Flush();
			os.Close();
			FileUtil.Chmod(fCommand.GetAbsolutePath(), "700");
			return fCommand;
		}

		private class CombineOutputCollector<K, V> : OutputCollector<K, V>
		{
			private IFile.Writer<K, V> writer;

			private Counters.Counter outCounter;

			private Progressable progressable;

			public CombineOutputCollector(TestPipeApplication _enclosing, Counters.Counter outCounter
				, Progressable progressable)
			{
				this._enclosing = _enclosing;
				this.outCounter = outCounter;
				this.progressable = progressable;
			}

			public virtual void SetWriter(IFile.Writer<K, V> writer)
			{
				lock (this)
				{
					this.writer = writer;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Collect(K key, V value)
			{
				lock (this)
				{
					this.outCounter.Increment(1);
					this.writer.Append(key, value);
					this.progressable.Progress();
				}
			}

			private readonly TestPipeApplication _enclosing;
		}

		public class FakeSplit : InputSplit
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}

			public virtual long GetLength()
			{
				return 0L;
			}

			public virtual string[] GetLocations()
			{
				return new string[0];
			}
		}

		private class TestTaskReporter : Reporter
		{
			private int recordNum = 0;

			private string status = null;

			private Counters counters = new Counters();

			private InputSplit split = new TestPipeApplication.FakeSplit();

			// number of records processed
			public virtual void Progress()
			{
				this.recordNum++;
			}

			public override void SetStatus(string status)
			{
				this.status = status;
			}

			public virtual string GetStatus()
			{
				return this.status;
			}

			public override Counters.Counter GetCounter(string group, string name)
			{
				Counters.Counter counter = null;
				if (this.counters != null)
				{
					counter = this.counters.FindCounter(group, name);
					if (counter == null)
					{
						Counters.Group grp = this.counters.AddGroup(group, group);
						counter = grp.AddCounter(name, name, 10);
					}
				}
				return counter;
			}

			public override Counters.Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return this.counters == null ? null : this.counters.FindCounter(name);
			}

			public override void IncrCounter<_T0>(Enum<_T0> key, long amount)
			{
				if (this.counters != null)
				{
					this.counters.IncrCounter(key, amount);
				}
			}

			public override void IncrCounter(string group, string counter, long amount)
			{
				if (this.counters != null)
				{
					this.counters.IncrCounter(group, counter, amount);
				}
			}

			/// <exception cref="System.NotSupportedException"/>
			public override InputSplit GetInputSplit()
			{
				return this.split;
			}

			public override float GetProgress()
			{
				return this.recordNum;
			}

			internal TestTaskReporter(TestPipeApplication _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestPipeApplication _enclosing;
		}

		private class Reader : RecordReader<FloatWritable, NullWritable>
		{
			private int index = 0;

			private FloatWritable progress;

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(FloatWritable key, NullWritable value)
			{
				this.progress = key;
				this.index++;
				return this.index <= 10;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual float GetProgress()
			{
				return this.progress.Get();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return this.index;
			}

			public virtual NullWritable CreateValue()
			{
				return NullWritable.Get();
			}

			public virtual FloatWritable CreateKey()
			{
				FloatWritable result = new FloatWritable(this.index);
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			internal Reader(TestPipeApplication _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestPipeApplication _enclosing;
		}

		private class ReaderPipesMapRunner : RecordReader<FloatWritable, NullWritable>
		{
			private int index = 0;

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(FloatWritable key, NullWritable value)
			{
				key.Set(this.index++);
				return this.index <= 10;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual float GetProgress()
			{
				return this.index;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return this.index;
			}

			public virtual NullWritable CreateValue()
			{
				return NullWritable.Get();
			}

			public virtual FloatWritable CreateKey()
			{
				FloatWritable result = new FloatWritable(this.index);
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			internal ReaderPipesMapRunner(TestPipeApplication _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestPipeApplication _enclosing;
		}

		private class FakeCollector : TestPipeApplication.CombineOutputCollector<IntWritable
			, Text>
		{
			private readonly IDictionary<IntWritable, Text> collect = new Dictionary<IntWritable
				, Text>();

			public FakeCollector(TestPipeApplication _enclosing, Counters.Counter outCounter, 
				Progressable progressable)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Collect(IntWritable key, Text value)
			{
				lock (this)
				{
					this.collect[key] = value;
					base.Collect(key, value);
				}
			}

			public virtual IDictionary<IntWritable, Text> GetCollect()
			{
				return this.collect;
			}

			private readonly TestPipeApplication _enclosing;
		}
	}
}
