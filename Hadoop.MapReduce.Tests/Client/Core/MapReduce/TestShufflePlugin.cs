using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Task.Reduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>A JUnit for testing availability and accessibility of shuffle related API.
	/// 	</summary>
	/// <remarks>
	/// A JUnit for testing availability and accessibility of shuffle related API.
	/// It is needed for maintaining comptability with external sub-classes of
	/// ShuffleConsumerPlugin and AuxiliaryService(s) like ShuffleHandler.
	/// The importance of this test is for preserving API with 3rd party plugins.
	/// </remarks>
	public class TestShufflePlugin<K, V>
	{
		internal class TestShuffleConsumerPlugin<K, V> : ShuffleConsumerPlugin<K, V>
		{
			public override void Init(ShuffleConsumerPlugin.Context<K, V> context)
			{
				// just verify that Context has kept its public interface
				context.GetReduceId();
				context.GetJobConf();
				context.GetLocalFS();
				context.GetUmbilical();
				context.GetLocalDirAllocator();
				context.GetReporter();
				context.GetCodec();
				context.GetCombinerClass();
				context.GetCombineCollector();
				context.GetSpilledRecordsCounter();
				context.GetReduceCombineInputCounter();
				context.GetShuffledMapsCounter();
				context.GetReduceShuffleBytes();
				context.GetFailedShuffleCounter();
				context.GetMergedMapOutputsCounter();
				context.GetStatus();
				context.GetCopyPhase();
				context.GetMergePhase();
				context.GetReduceTask();
				context.GetMapOutputFile();
			}

			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RawKeyValueIterator Run()
			{
				return null;
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestPluginAbility()
		{
			try
			{
				// create JobConf with mapreduce.job.shuffle.consumer.plugin=TestShuffleConsumerPlugin
				JobConf jobConf = new JobConf();
				jobConf.SetClass(MRConfig.ShuffleConsumerPlugin, typeof(TestShufflePlugin.TestShuffleConsumerPlugin
					), typeof(ShuffleConsumerPlugin));
				ShuffleConsumerPlugin shuffleConsumerPlugin = null;
				Type clazz = jobConf.GetClass<ShuffleConsumerPlugin>(MRConfig.ShuffleConsumerPlugin
					, typeof(Shuffle));
				NUnit.Framework.Assert.IsNotNull("Unable to get " + MRConfig.ShuffleConsumerPlugin
					, clazz);
				// load 3rd party plugin through core's factory method
				shuffleConsumerPlugin = ReflectionUtils.NewInstance(clazz, jobConf);
				NUnit.Framework.Assert.IsNotNull("Unable to load " + MRConfig.ShuffleConsumerPlugin
					, shuffleConsumerPlugin);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Threw exception:" + e, false);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestConsumerApi()
		{
			JobConf jobConf = new JobConf();
			ShuffleConsumerPlugin<K, V> shuffleConsumerPlugin = new TestShufflePlugin.TestShuffleConsumerPlugin
				<K, V>();
			//mock creation
			ReduceTask mockReduceTask = Org.Mockito.Mockito.Mock<ReduceTask>();
			TaskUmbilicalProtocol mockUmbilical = Org.Mockito.Mockito.Mock<TaskUmbilicalProtocol
				>();
			Reporter mockReporter = Org.Mockito.Mockito.Mock<Reporter>();
			FileSystem mockFileSystem = Org.Mockito.Mockito.Mock<FileSystem>();
			Type combinerClass = jobConf.GetCombinerClass();
			Task.CombineOutputCollector<K, V> mockCombineOutputCollector = (Task.CombineOutputCollector
				<K, V>)Org.Mockito.Mockito.Mock<Task.CombineOutputCollector>();
			// needed for mock with generic
			TaskAttemptID mockTaskAttemptID = Org.Mockito.Mockito.Mock<TaskAttemptID>();
			LocalDirAllocator mockLocalDirAllocator = Org.Mockito.Mockito.Mock<LocalDirAllocator
				>();
			CompressionCodec mockCompressionCodec = Org.Mockito.Mockito.Mock<CompressionCodec
				>();
			Counters.Counter mockCounter = Org.Mockito.Mockito.Mock<Counters.Counter>();
			TaskStatus mockTaskStatus = Org.Mockito.Mockito.Mock<TaskStatus>();
			Progress mockProgress = Org.Mockito.Mockito.Mock<Progress>();
			MapOutputFile mockMapOutputFile = Org.Mockito.Mockito.Mock<MapOutputFile>();
			Org.Apache.Hadoop.Mapred.Task mockTask = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapred.Task
				>();
			try
			{
				string[] dirs = jobConf.GetLocalDirs();
				// verify that these APIs are available through super class handler
				ShuffleConsumerPlugin.Context<K, V> context = new ShuffleConsumerPlugin.Context<K
					, V>(mockTaskAttemptID, jobConf, mockFileSystem, mockUmbilical, mockLocalDirAllocator
					, mockReporter, mockCompressionCodec, combinerClass, mockCombineOutputCollector, 
					mockCounter, mockCounter, mockCounter, mockCounter, mockCounter, mockCounter, mockTaskStatus
					, mockProgress, mockProgress, mockTask, mockMapOutputFile, null);
				shuffleConsumerPlugin.Init(context);
				shuffleConsumerPlugin.Run();
				shuffleConsumerPlugin.Close();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Threw exception:" + e, false);
			}
			// verify that these APIs are available for 3rd party plugins
			mockReduceTask.GetTaskID();
			mockReduceTask.GetJobID();
			mockReduceTask.GetNumMaps();
			mockReduceTask.GetPartition();
			mockReporter.Progress();
		}

		[NUnit.Framework.Test]
		public virtual void TestProviderApi()
		{
			LocalDirAllocator mockLocalDirAllocator = Org.Mockito.Mockito.Mock<LocalDirAllocator
				>();
			JobConf mockJobConf = Org.Mockito.Mockito.Mock<JobConf>();
			try
			{
				mockLocalDirAllocator.GetLocalPathToRead(string.Empty, mockJobConf);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Threw exception:" + e, false);
			}
		}
	}
}
