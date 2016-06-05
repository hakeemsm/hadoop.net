using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.LoadGenerator
{
	/// <summary>This class tests if a balancer schedules tasks correctly.</summary>
	public class TestLoadGenerator : Configured, Tool
	{
		private static readonly Configuration Conf = new HdfsConfiguration();

		private const int DefaultBlockSize = 10;

		private static readonly FilePath OutDir = PathUtils.GetTestDir(typeof(Org.Apache.Hadoop.FS.LoadGenerator.TestLoadGenerator
			));

		private static readonly FilePath DirStructureFile = new FilePath(OutDir, StructureGenerator
			.DirStructureFileName);

		private static readonly FilePath FileStructureFile = new FilePath(OutDir, StructureGenerator
			.FileStructureFileName);

		private const string DirStructureFirstLine = "/dir0";

		private const string DirStructureSecondLine = "/dir1";

		private const string FileStructureFirstLine = "/dir0/_file_0 0.3754598635933768";

		private const string FileStructureSecondLine = "/dir1/_file_1 1.4729310851145203";

		static TestLoadGenerator()
		{
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			Conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, DefaultBlockSize);
			Conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
		}

		/// <summary>Test if the structure generator works fine</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStructureGenerator()
		{
			StructureGenerator sg = new StructureGenerator();
			string[] args = new string[] { "-maxDepth", "2", "-minWidth", "1", "-maxWidth", "2"
				, "-numOfFiles", "2", "-avgFileSize", "1", "-outDir", OutDir.GetAbsolutePath(), 
				"-seed", "1" };
			int MaxDepth = 1;
			int MinWidth = 3;
			int MaxWidth = 5;
			int NumOfFiles = 7;
			int AvgFileSize = 9;
			int Seed = 13;
			try
			{
				// successful case
				NUnit.Framework.Assert.AreEqual(0, sg.Run(args));
				BufferedReader @in = new BufferedReader(new FileReader(DirStructureFile));
				NUnit.Framework.Assert.AreEqual(DirStructureFirstLine, @in.ReadLine());
				NUnit.Framework.Assert.AreEqual(DirStructureSecondLine, @in.ReadLine());
				NUnit.Framework.Assert.AreEqual(null, @in.ReadLine());
				@in.Close();
				@in = new BufferedReader(new FileReader(FileStructureFile));
				NUnit.Framework.Assert.AreEqual(FileStructureFirstLine, @in.ReadLine());
				NUnit.Framework.Assert.AreEqual(FileStructureSecondLine, @in.ReadLine());
				NUnit.Framework.Assert.AreEqual(null, @in.ReadLine());
				@in.Close();
				string oldArg = args[MaxDepth];
				args[MaxDepth] = "0";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[MaxDepth] = oldArg;
				oldArg = args[MinWidth];
				args[MinWidth] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[MinWidth] = oldArg;
				oldArg = args[MaxWidth];
				args[MaxWidth] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[MaxWidth] = oldArg;
				oldArg = args[NumOfFiles];
				args[NumOfFiles] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[NumOfFiles] = oldArg;
				oldArg = args[NumOfFiles];
				args[NumOfFiles] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[NumOfFiles] = oldArg;
				oldArg = args[AvgFileSize];
				args[AvgFileSize] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[AvgFileSize] = oldArg;
				oldArg = args[Seed];
				args[Seed] = "34.d4";
				NUnit.Framework.Assert.AreEqual(-1, sg.Run(args));
				args[Seed] = oldArg;
			}
			finally
			{
				DirStructureFile.Delete();
				FileStructureFile.Delete();
			}
		}

		/// <summary>Test if the load generator works fine</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLoadGenerator()
		{
			string TestSpaceRoot = "/test";
			string ScriptTestDir = OutDir.GetAbsolutePath();
			string script = ScriptTestDir + "/" + "loadgenscript";
			string script2 = ScriptTestDir + "/" + "loadgenscript2";
			FilePath scriptFile1 = new FilePath(script);
			FilePath scriptFile2 = new FilePath(script2);
			FileWriter writer = new FileWriter(DirStructureFile);
			writer.Write(DirStructureFirstLine + "\n");
			writer.Write(DirStructureSecondLine + "\n");
			writer.Close();
			writer = new FileWriter(FileStructureFile);
			writer.Write(FileStructureFirstLine + "\n");
			writer.Write(FileStructureSecondLine + "\n");
			writer.Close();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			try
			{
				DataGenerator dg = new DataGenerator();
				dg.SetConf(Conf);
				string[] args = new string[] { "-inDir", OutDir.GetAbsolutePath(), "-root", TestSpaceRoot
					 };
				NUnit.Framework.Assert.AreEqual(0, dg.Run(args));
				int ReadProbability = 1;
				int WriteProbability = 3;
				int MaxDelayBetweenOps = 7;
				int NumOfThreads = 9;
				int StartTime = 11;
				int ElapsedTime = 13;
				Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator lg = new Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator
					();
				lg.SetConf(Conf);
				args = new string[] { "-readProbability", "0.3", "-writeProbability", "0.3", "-root"
					, TestSpaceRoot, "-maxDelayBetweenOps", "0", "-numOfThreads", "1", "-startTime", 
					System.Convert.ToString(Time.Now()), "-elapsedTime", "10" };
				NUnit.Framework.Assert.AreEqual(0, lg.Run(args));
				string oldArg = args[ReadProbability];
				args[ReadProbability] = "1.1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[ReadProbability] = "-1.1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[ReadProbability] = oldArg;
				oldArg = args[WriteProbability];
				args[WriteProbability] = "1.1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[WriteProbability] = "-1.1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[WriteProbability] = "0.9";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[ReadProbability] = oldArg;
				oldArg = args[MaxDelayBetweenOps];
				args[MaxDelayBetweenOps] = "1.x1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[MaxDelayBetweenOps] = oldArg;
				oldArg = args[MaxDelayBetweenOps];
				args[MaxDelayBetweenOps] = "1.x1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[MaxDelayBetweenOps] = oldArg;
				oldArg = args[NumOfThreads];
				args[NumOfThreads] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[NumOfThreads] = oldArg;
				oldArg = args[StartTime];
				args[StartTime] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[StartTime] = oldArg;
				oldArg = args[ElapsedTime];
				args[ElapsedTime] = "-1";
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(args));
				args[ElapsedTime] = oldArg;
				// test scripted operation
				// Test with good script
				FileWriter fw = new FileWriter(scriptFile1);
				fw.Write("2 .22 .33\n");
				fw.Write("3 .10 .6\n");
				fw.Write("6 0 .7\n");
				fw.Close();
				string[] scriptArgs = new string[] { "-root", TestSpaceRoot, "-maxDelayBetweenOps"
					, "0", "-numOfThreads", "10", "-startTime", System.Convert.ToString(Time.Now()), 
					"-scriptFile", script };
				NUnit.Framework.Assert.AreEqual(0, lg.Run(scriptArgs));
				// Test with bad script
				fw = new FileWriter(scriptFile2);
				fw.Write("2 .22 .33\n");
				fw.Write("3 blah blah blah .6\n");
				fw.Write("6 0 .7\n");
				fw.Close();
				scriptArgs[scriptArgs.Length - 1] = script2;
				NUnit.Framework.Assert.AreEqual(-1, lg.Run(scriptArgs));
			}
			finally
			{
				cluster.Shutdown();
				DirStructureFile.Delete();
				FileStructureFile.Delete();
				scriptFile1.Delete();
				scriptFile2.Delete();
			}
		}

		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Org.Apache.Hadoop.FS.LoadGenerator.TestLoadGenerator
				(), args);
			System.Environment.Exit(res);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Org.Apache.Hadoop.FS.LoadGenerator.TestLoadGenerator loadGeneratorTest = new Org.Apache.Hadoop.FS.LoadGenerator.TestLoadGenerator
				();
			loadGeneratorTest.TestStructureGenerator();
			loadGeneratorTest.TestLoadGenerator();
			return 0;
		}
	}
}
