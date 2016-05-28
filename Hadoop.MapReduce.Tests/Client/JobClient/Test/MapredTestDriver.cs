using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.LoadGenerator;
using Org.Apache.Hadoop.FS.Slive;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>Driver for Map-reduce tests.</summary>
	public class MapredTestDriver
	{
		private ProgramDriver pgd;

		public MapredTestDriver()
			: this(new ProgramDriver())
		{
		}

		public MapredTestDriver(ProgramDriver pgd)
		{
			this.pgd = pgd;
			try
			{
				pgd.AddClass("testsequencefile", typeof(TestSequenceFile), "A test for flat files of binary key value pairs."
					);
				pgd.AddClass("threadedmapbench", typeof(ThreadedMapBenchmark), "A map/reduce benchmark that compares the performance "
					 + "of maps with multiple spills over maps with 1 spill");
				pgd.AddClass("mrbench", typeof(MRBench), "A map/reduce benchmark that can create many small jobs"
					);
				pgd.AddClass("mapredtest", typeof(TestMapRed), "A map/reduce test check.");
				pgd.AddClass("testsequencefileinputformat", typeof(TestSequenceFileInputFormat), 
					"A test for sequence file input format.");
				pgd.AddClass("testtextinputformat", typeof(TestTextInputFormat), "A test for text input format."
					);
				pgd.AddClass("testmapredsort", typeof(SortValidator), "A map/reduce program that validates the "
					 + "map-reduce framework's sort.");
				pgd.AddClass("testbigmapoutput", typeof(BigMapOutput), "A map/reduce program that works on a very big "
					 + "non-splittable file and does identity map/reduce");
				pgd.AddClass("loadgen", typeof(GenericMRLoadGenerator), "Generic map/reduce load generator"
					);
				pgd.AddClass("MRReliabilityTest", typeof(ReliabilityTest), "A program that tests the reliability of the MR framework by "
					 + "injecting faults/failures");
				pgd.AddClass("fail", typeof(FailJob), "a job that always fails");
				pgd.AddClass("sleep", typeof(SleepJob), "A job that sleeps at each map and reduce task."
					);
				pgd.AddClass("nnbench", typeof(NNBench), "A benchmark that stresses the namenode."
					);
				pgd.AddClass("testfilesystem", typeof(TestFileSystem), "A test for FileSystem read/write."
					);
				pgd.AddClass(typeof(TestDFSIO).Name, typeof(TestDFSIO), "Distributed i/o benchmark."
					);
				pgd.AddClass("DFSCIOTest", typeof(DFSCIOTest), string.Empty + "Distributed i/o benchmark of libhdfs."
					);
				pgd.AddClass("DistributedFSCheck", typeof(DistributedFSCheck), "Distributed checkup of the file system consistency."
					);
				pgd.AddClass("filebench", typeof(FileBench), "Benchmark SequenceFile(Input|Output)Format "
					 + "(block,record compressed and uncompressed), " + "Text(Input|Output)Format (compressed and uncompressed)"
					);
				pgd.AddClass(typeof(JHLogAnalyzer).Name, typeof(JHLogAnalyzer), "Job History Log analyzer."
					);
				pgd.AddClass(typeof(SliveTest).Name, typeof(SliveTest), "HDFS Stress Test and Live Data Verification."
					);
				pgd.AddClass("minicluster", typeof(MiniHadoopClusterManager), "Single process HDFS and MR cluster."
					);
				pgd.AddClass("largesorter", typeof(LargeSorter), "Large-Sort tester");
				pgd.AddClass("NNloadGenerator", typeof(Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator
					), "Generate load on Namenode using NN loadgenerator run WITHOUT MR");
				pgd.AddClass("NNloadGeneratorMR", typeof(LoadGeneratorMR), "Generate load on Namenode using NN loadgenerator run as MR job"
					);
				pgd.AddClass("NNstructureGenerator", typeof(StructureGenerator), "Generate the structure to be used by NNdataGenerator"
					);
				pgd.AddClass("NNdataGenerator", typeof(DataGenerator), "Generate the data to be used by NNloadGenerator"
					);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		public virtual void Run(string[] argv)
		{
			int exitCode = -1;
			try
			{
				exitCode = pgd.Run(argv);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			System.Environment.Exit(exitCode);
		}

		public static void Main(string[] argv)
		{
			new Org.Apache.Hadoop.Test.MapredTestDriver().Run(argv);
		}
	}
}
