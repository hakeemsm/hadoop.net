using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class TestPipes : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Pipes.TestPipes
			).FullName);

		private static Path cppExamples = new Path(Runtime.GetProperty("install.c++.examples"
			));

		internal static Path wordCountSimple = new Path(cppExamples, "bin/wordcount-simple"
			);

		internal static Path wordCountPart = new Path(cppExamples, "bin/wordcount-part");

		internal static Path wordCountNoPipes = new Path(cppExamples, "bin/wordcount-nopipe"
			);

		internal static Path nonPipedOutDir;

		/// <exception cref="System.IO.IOException"/>
		internal static void Cleanup(FileSystem fs, Path p)
		{
			fs.Delete(p, true);
			NUnit.Framework.Assert.IsFalse("output not cleaned up", fs.Exists(p));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPipes()
		{
			if (Runtime.GetProperty("compile.c++") == null)
			{
				Log.Info("compile.c++ is not defined, so skipping TestPipes");
				return;
			}
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			Path inputPath = new Path("testing/in");
			Path outputPath = new Path("testing/out");
			try
			{
				int numSlaves = 2;
				Configuration conf = new Configuration();
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(numSlaves).Build();
				mr = new MiniMRCluster(numSlaves, dfs.GetFileSystem().GetUri().ToString(), 1);
				WriteInputFile(dfs.GetFileSystem(), inputPath);
				RunProgram(mr, dfs, wordCountSimple, inputPath, outputPath, 3, 2, twoSplitOutput, 
					null);
				Cleanup(dfs.GetFileSystem(), outputPath);
				RunProgram(mr, dfs, wordCountSimple, inputPath, outputPath, 3, 0, noSortOutput, null
					);
				Cleanup(dfs.GetFileSystem(), outputPath);
				RunProgram(mr, dfs, wordCountPart, inputPath, outputPath, 3, 2, fixedPartitionOutput
					, null);
				RunNonPipedProgram(mr, dfs, wordCountNoPipes, null);
				mr.WaitUntilIdle();
			}
			finally
			{
				mr.Shutdown();
				dfs.Shutdown();
			}
		}

		internal static readonly string[] twoSplitOutput = new string[] { "`and\t1\na\t1\nand\t1\nbeginning\t1\nbook\t1\nbut\t1\nby\t1\n"
			 + "conversation?'\t1\ndo:\t1\nhad\t2\nhaving\t1\nher\t2\nin\t1\nit\t1\n" + "it,\t1\nno\t1\nnothing\t1\nof\t3\non\t1\nonce\t1\nor\t3\npeeped\t1\n"
			 + "pictures\t2\nthe\t3\nthought\t1\nto\t2\nuse\t1\nwas\t2\n", "Alice\t2\n`without\t1\nbank,\t1\nbook,'\t1\nconversations\t1\nget\t1\n"
			 + "into\t1\nis\t1\nreading,\t1\nshe\t1\nsister\t2\nsitting\t1\ntired\t1\n" + "twice\t1\nvery\t1\nwhat\t1\n"
			 };

		internal static readonly string[] noSortOutput = new string[] { "it,\t1\n`and\t1\nwhat\t1\nis\t1\nthe\t1\nuse\t1\nof\t1\na\t1\n"
			 + "book,'\t1\nthought\t1\nAlice\t1\n`without\t1\npictures\t1\nor\t1\n" + "conversation?'\t1\n"
			, "Alice\t1\nwas\t1\nbeginning\t1\nto\t1\nget\t1\nvery\t1\ntired\t1\n" + "of\t1\nsitting\t1\nby\t1\nher\t1\nsister\t1\non\t1\nthe\t1\nbank,\t1\n"
			 + "and\t1\nof\t1\nhaving\t1\nnothing\t1\nto\t1\ndo:\t1\nonce\t1\n", "or\t1\ntwice\t1\nshe\t1\nhad\t1\npeeped\t1\ninto\t1\nthe\t1\nbook\t1\n"
			 + "her\t1\nsister\t1\nwas\t1\nreading,\t1\nbut\t1\nit\t1\nhad\t1\nno\t1\n" + "pictures\t1\nor\t1\nconversations\t1\nin\t1\n"
			 };

		internal static readonly string[] fixedPartitionOutput = new string[] { "Alice\t2\n`and\t1\n`without\t1\na\t1\nand\t1\nbank,\t1\nbeginning\t1\n"
			 + "book\t1\nbook,'\t1\nbut\t1\nby\t1\nconversation?'\t1\nconversations\t1\n" + 
			"do:\t1\nget\t1\nhad\t2\nhaving\t1\nher\t2\nin\t1\ninto\t1\nis\t1\n" + "it\t1\nit,\t1\nno\t1\nnothing\t1\nof\t3\non\t1\nonce\t1\nor\t3\n"
			 + "peeped\t1\npictures\t2\nreading,\t1\nshe\t1\nsister\t2\nsitting\t1\n" + "the\t3\nthought\t1\ntired\t1\nto\t2\ntwice\t1\nuse\t1\n"
			 + "very\t1\nwas\t2\nwhat\t1\n", string.Empty };

		/// <exception cref="System.IO.IOException"/>
		internal static void WriteInputFile(FileSystem fs, Path dir)
		{
			DataOutputStream @out = fs.Create(new Path(dir, "part0"));
			@out.WriteBytes("Alice was beginning to get very tired of sitting by her\n");
			@out.WriteBytes("sister on the bank, and of having nothing to do: once\n");
			@out.WriteBytes("or twice she had peeped into the book her sister was\n");
			@out.WriteBytes("reading, but it had no pictures or conversations in\n");
			@out.WriteBytes("it, `and what is the use of a book,' thought Alice\n");
			@out.WriteBytes("`without pictures or conversation?'\n");
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RunProgram(MiniMRCluster mr, MiniDFSCluster dfs, Path program
			, Path inputPath, Path outputPath, int numMaps, int numReduces, string[] expectedResults
			, JobConf conf)
		{
			Path wordExec = new Path("testing/bin/application");
			JobConf job = null;
			if (conf == null)
			{
				job = mr.CreateJobConf();
			}
			else
			{
				job = new JobConf(conf);
			}
			job.SetNumMapTasks(numMaps);
			job.SetNumReduceTasks(numReduces);
			{
				FileSystem fs = dfs.GetFileSystem();
				fs.Delete(wordExec.GetParent(), true);
				fs.CopyFromLocalFile(program, wordExec);
				Submitter.SetExecutable(job, fs.MakeQualified(wordExec).ToString());
				Submitter.SetIsJavaRecordReader(job, true);
				Submitter.SetIsJavaRecordWriter(job, true);
				FileInputFormat.SetInputPaths(job, inputPath);
				FileOutputFormat.SetOutputPath(job, outputPath);
				RunningJob rJob = null;
				if (numReduces == 0)
				{
					rJob = Submitter.JobSubmit(job);
					while (!rJob.IsComplete())
					{
						try
						{
							Sharpen.Thread.Sleep(1000);
						}
						catch (Exception ie)
						{
							throw new RuntimeException(ie);
						}
					}
				}
				else
				{
					rJob = Submitter.RunJob(job);
				}
				NUnit.Framework.Assert.IsTrue("pipes job failed", rJob.IsSuccessful());
				Counters counters = rJob.GetCounters();
				Counters.Group wordCountCounters = counters.GetGroup("WORDCOUNT");
				int numCounters = 0;
				foreach (Counters.Counter c in wordCountCounters)
				{
					System.Console.Out.WriteLine(c);
					++numCounters;
				}
				NUnit.Framework.Assert.IsTrue("No counters found!", (numCounters > 0));
			}
			IList<string> results = new AList<string>();
			foreach (Path p in FileUtil.Stat2Paths(dfs.GetFileSystem().ListStatus(outputPath, 
				new Utils.OutputFileUtils.OutputFilesFilter())))
			{
				results.AddItem(MapReduceTestUtil.ReadOutput(p, job));
			}
			NUnit.Framework.Assert.AreEqual("number of reduces is wrong", expectedResults.Length
				, results.Count);
			for (int i = 0; i < results.Count; i++)
			{
				NUnit.Framework.Assert.AreEqual("pipes program " + program + " output " + i + " wrong"
					, expectedResults[i], results[i]);
			}
		}

		/// <summary>
		/// Run a map/reduce word count that does all of the map input and reduce
		/// output directly rather than sending it back up to Java.
		/// </summary>
		/// <param name="mr">The mini mr cluster</param>
		/// <param name="dfs">the dfs cluster</param>
		/// <param name="program">the program to run</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void RunNonPipedProgram(MiniMRCluster mr, MiniDFSCluster dfs, Path
			 program, JobConf conf)
		{
			JobConf job;
			if (conf == null)
			{
				job = mr.CreateJobConf();
			}
			else
			{
				job = new JobConf(conf);
			}
			job.SetInputFormat(typeof(WordCountInputFormat));
			FileSystem local = FileSystem.GetLocal(job);
			Path testDir = new Path("file:" + Runtime.GetProperty("test.build.data"), "pipes"
				);
			Path inDir = new Path(testDir, "input");
			nonPipedOutDir = new Path(testDir, "output");
			Path wordExec = new Path("testing/bin/application");
			Path jobXml = new Path(testDir, "job.xml");
			{
				FileSystem fs = dfs.GetFileSystem();
				fs.Delete(wordExec.GetParent(), true);
				fs.CopyFromLocalFile(program, wordExec);
			}
			DataOutputStream @out = local.Create(new Path(inDir, "part0"));
			@out.WriteBytes("i am a silly test\n");
			@out.WriteBytes("you are silly\n");
			@out.WriteBytes("i am a cat test\n");
			@out.WriteBytes("you is silly\n");
			@out.WriteBytes("i am a billy test\n");
			@out.WriteBytes("hello are silly\n");
			@out.Close();
			@out = local.Create(new Path(inDir, "part1"));
			@out.WriteBytes("mall world things drink java\n");
			@out.WriteBytes("hall silly cats drink java\n");
			@out.WriteBytes("all dogs bow wow\n");
			@out.WriteBytes("hello drink java\n");
			@out.Close();
			local.Delete(nonPipedOutDir, true);
			local.Mkdirs(nonPipedOutDir, new FsPermission(FsAction.All, FsAction.All, FsAction
				.All));
			@out = local.Create(jobXml);
			job.WriteXml(@out);
			@out.Close();
			System.Console.Error.WriteLine("About to run: Submitter -conf " + jobXml + " -input "
				 + inDir + " -output " + nonPipedOutDir + " -program " + dfs.GetFileSystem().MakeQualified
				(wordExec));
			try
			{
				int ret = ToolRunner.Run(new Submitter(), new string[] { "-conf", jobXml.ToString
					(), "-input", inDir.ToString(), "-output", nonPipedOutDir.ToString(), "-program"
					, dfs.GetFileSystem().MakeQualified(wordExec).ToString(), "-reduces", "2" });
				NUnit.Framework.Assert.AreEqual(0, ret);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("got exception: " + StringUtils.StringifyException(
					e), false);
			}
		}
	}
}
