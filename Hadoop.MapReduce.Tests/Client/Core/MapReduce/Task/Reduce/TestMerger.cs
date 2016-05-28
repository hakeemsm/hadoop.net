using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class TestMerger
	{
		private Configuration conf;

		private JobConf jobConf;

		private FileSystem fs;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			jobConf = new JobConf();
			fs = FileSystem.GetLocal(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedMerger()
		{
			jobConf.SetBoolean(MRJobConfig.MrEncryptedIntermediateData, true);
			conf.SetBoolean(MRJobConfig.MrEncryptedIntermediateData, true);
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			TokenCache.SetEncryptedSpillKey(new byte[16], credentials);
			UserGroupInformation.GetCurrentUser().AddCredentials(credentials);
			TestInMemoryAndOnDiskMerger();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInMemoryAndOnDiskMerger()
		{
			JobID jobId = new JobID("a", 0);
			TaskAttemptID reduceId1 = new TaskAttemptID(new TaskID(jobId, TaskType.Reduce, 0)
				, 0);
			TaskAttemptID mapId1 = new TaskAttemptID(new TaskID(jobId, TaskType.Map, 1), 0);
			TaskAttemptID mapId2 = new TaskAttemptID(new TaskID(jobId, TaskType.Map, 2), 0);
			LocalDirAllocator lda = new LocalDirAllocator(MRConfig.LocalDir);
			MergeManagerImpl<Text, Text> mergeManager = new MergeManagerImpl<Text, Text>(reduceId1
				, jobConf, fs, lda, Reporter.Null, null, null, null, null, null, null, null, new 
				Progress(), new MROutputFiles());
			// write map outputs
			IDictionary<string, string> map1 = new SortedDictionary<string, string>();
			map1["apple"] = "disgusting";
			map1["carrot"] = "delicious";
			IDictionary<string, string> map2 = new SortedDictionary<string, string>();
			map1["banana"] = "pretty good";
			byte[] mapOutputBytes1 = WriteMapOutput(conf, map1);
			byte[] mapOutputBytes2 = WriteMapOutput(conf, map2);
			InMemoryMapOutput<Text, Text> mapOutput1 = new InMemoryMapOutput<Text, Text>(conf
				, mapId1, mergeManager, mapOutputBytes1.Length, null, true);
			InMemoryMapOutput<Text, Text> mapOutput2 = new InMemoryMapOutput<Text, Text>(conf
				, mapId2, mergeManager, mapOutputBytes2.Length, null, true);
			System.Array.Copy(mapOutputBytes1, 0, mapOutput1.GetMemory(), 0, mapOutputBytes1.
				Length);
			System.Array.Copy(mapOutputBytes2, 0, mapOutput2.GetMemory(), 0, mapOutputBytes2.
				Length);
			// create merger and run merge
			MergeThread<InMemoryMapOutput<Text, Text>, Text, Text> inMemoryMerger = mergeManager
				.CreateInMemoryMerger();
			IList<InMemoryMapOutput<Text, Text>> mapOutputs1 = new AList<InMemoryMapOutput<Text
				, Text>>();
			mapOutputs1.AddItem(mapOutput1);
			mapOutputs1.AddItem(mapOutput2);
			inMemoryMerger.Merge(mapOutputs1);
			NUnit.Framework.Assert.AreEqual(1, mergeManager.onDiskMapOutputs.Count);
			TaskAttemptID reduceId2 = new TaskAttemptID(new TaskID(jobId, TaskType.Reduce, 3)
				, 0);
			TaskAttemptID mapId3 = new TaskAttemptID(new TaskID(jobId, TaskType.Map, 4), 0);
			TaskAttemptID mapId4 = new TaskAttemptID(new TaskID(jobId, TaskType.Map, 5), 0);
			// write map outputs
			IDictionary<string, string> map3 = new SortedDictionary<string, string>();
			map3["apple"] = "awesome";
			map3["carrot"] = "amazing";
			IDictionary<string, string> map4 = new SortedDictionary<string, string>();
			map4["banana"] = "bla";
			byte[] mapOutputBytes3 = WriteMapOutput(conf, map3);
			byte[] mapOutputBytes4 = WriteMapOutput(conf, map4);
			InMemoryMapOutput<Text, Text> mapOutput3 = new InMemoryMapOutput<Text, Text>(conf
				, mapId3, mergeManager, mapOutputBytes3.Length, null, true);
			InMemoryMapOutput<Text, Text> mapOutput4 = new InMemoryMapOutput<Text, Text>(conf
				, mapId4, mergeManager, mapOutputBytes4.Length, null, true);
			System.Array.Copy(mapOutputBytes3, 0, mapOutput3.GetMemory(), 0, mapOutputBytes3.
				Length);
			System.Array.Copy(mapOutputBytes4, 0, mapOutput4.GetMemory(), 0, mapOutputBytes4.
				Length);
			//    // create merger and run merge
			MergeThread<InMemoryMapOutput<Text, Text>, Text, Text> inMemoryMerger2 = mergeManager
				.CreateInMemoryMerger();
			IList<InMemoryMapOutput<Text, Text>> mapOutputs2 = new AList<InMemoryMapOutput<Text
				, Text>>();
			mapOutputs2.AddItem(mapOutput3);
			mapOutputs2.AddItem(mapOutput4);
			inMemoryMerger2.Merge(mapOutputs2);
			NUnit.Framework.Assert.AreEqual(2, mergeManager.onDiskMapOutputs.Count);
			IList<MergeManagerImpl.CompressAwarePath> paths = new AList<MergeManagerImpl.CompressAwarePath
				>();
			IEnumerator<MergeManagerImpl.CompressAwarePath> iterator = mergeManager.onDiskMapOutputs
				.GetEnumerator();
			IList<string> keys = new AList<string>();
			IList<string> values = new AList<string>();
			while (iterator.HasNext())
			{
				MergeManagerImpl.CompressAwarePath next = iterator.Next();
				ReadOnDiskMapOutput(conf, fs, next, keys, values);
				paths.AddItem(next);
			}
			NUnit.Framework.Assert.AreEqual(keys, Arrays.AsList("apple", "banana", "carrot", 
				"apple", "banana", "carrot"));
			NUnit.Framework.Assert.AreEqual(values, Arrays.AsList("awesome", "bla", "amazing"
				, "disgusting", "pretty good", "delicious"));
			mergeManager.Close();
			mergeManager = new MergeManagerImpl<Text, Text>(reduceId2, jobConf, fs, lda, Reporter
				.Null, null, null, null, null, null, null, null, new Progress(), new MROutputFiles
				());
			MergeThread<MergeManagerImpl.CompressAwarePath, Text, Text> onDiskMerger = mergeManager
				.CreateOnDiskMerger();
			onDiskMerger.Merge(paths);
			NUnit.Framework.Assert.AreEqual(1, mergeManager.onDiskMapOutputs.Count);
			keys = new AList<string>();
			values = new AList<string>();
			ReadOnDiskMapOutput(conf, fs, mergeManager.onDiskMapOutputs.GetEnumerator().Next(
				), keys, values);
			NUnit.Framework.Assert.AreEqual(keys, Arrays.AsList("apple", "apple", "banana", "banana"
				, "carrot", "carrot"));
			NUnit.Framework.Assert.AreEqual(values, Arrays.AsList("awesome", "disgusting", "pretty good"
				, "bla", "amazing", "delicious"));
			mergeManager.Close();
			NUnit.Framework.Assert.AreEqual(0, mergeManager.inMemoryMapOutputs.Count);
			NUnit.Framework.Assert.AreEqual(0, mergeManager.inMemoryMergedMapOutputs.Count);
			NUnit.Framework.Assert.AreEqual(0, mergeManager.onDiskMapOutputs.Count);
		}

		/// <exception cref="System.IO.IOException"/>
		private byte[] WriteMapOutput(Configuration conf, IDictionary<string, string> keysToValues
			)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			FSDataOutputStream fsdos = new FSDataOutputStream(baos, null);
			IFile.Writer<Text, Text> writer = new IFile.Writer<Text, Text>(conf, fsdos, typeof(
				Text), typeof(Text), null, null);
			foreach (string key in keysToValues.Keys)
			{
				string value = keysToValues[key];
				writer.Append(new Text(key), new Text(value));
			}
			writer.Close();
			return baos.ToByteArray();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadOnDiskMapOutput(Configuration conf, FileSystem fs, Path path, IList
			<string> keys, IList<string> values)
		{
			FSDataInputStream @in = CryptoUtils.WrapIfNecessary(conf, fs.Open(path));
			IFile.Reader<Text, Text> reader = new IFile.Reader<Text, Text>(conf, @in, fs.GetFileStatus
				(path).GetLen(), null, null);
			DataInputBuffer keyBuff = new DataInputBuffer();
			DataInputBuffer valueBuff = new DataInputBuffer();
			Text key = new Text();
			Text value = new Text();
			while (reader.NextRawKey(keyBuff))
			{
				key.ReadFields(keyBuff);
				keys.AddItem(key.ToString());
				reader.NextRawValue(valueBuff);
				value.ReadFields(valueBuff);
				values.AddItem(value.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCompressed()
		{
			TestMergeShouldReturnProperProgress(GetCompressedSegments());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUncompressed()
		{
			TestMergeShouldReturnProperProgress(GetUncompressedSegments());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMergeShouldReturnProperProgress(IList<Merger.Segment<Text
			, Text>> segments)
		{
			Path tmpDir = new Path("localpath");
			Type keyClass = (Type)jobConf.GetMapOutputKeyClass();
			Type valueClass = (Type)jobConf.GetMapOutputValueClass();
			RawComparator<Text> comparator = jobConf.GetOutputKeyComparator();
			Counters.Counter readsCounter = new Counters.Counter();
			Counters.Counter writesCounter = new Counters.Counter();
			Progress mergePhase = new Progress();
			RawKeyValueIterator mergeQueue = Merger.Merge(conf, fs, keyClass, valueClass, segments
				, 2, tmpDir, comparator, GetReporter(), readsCounter, writesCounter, mergePhase);
			float epsilon = 0.00001f;
			// Reading 6 keys total, 3 each in 2 segments, so each key read moves the
			// progress forward 1/6th of the way. Initially the first keys from each
			// segment have been read as part of the merge setup, so progress = 2/6.
			NUnit.Framework.Assert.AreEqual(2 / 6.0f, mergeQueue.GetProgress().Get(), epsilon
				);
			// The first next() returns one of the keys already read during merge setup
			NUnit.Framework.Assert.IsTrue(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(2 / 6.0f, mergeQueue.GetProgress().Get(), epsilon
				);
			// Subsequent next() calls should read one key and move progress
			NUnit.Framework.Assert.IsTrue(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(3 / 6.0f, mergeQueue.GetProgress().Get(), epsilon
				);
			NUnit.Framework.Assert.IsTrue(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(4 / 6.0f, mergeQueue.GetProgress().Get(), epsilon
				);
			// At this point we've exhausted all of the keys in one segment
			// so getting the next key will return the already cached key from the
			// other segment
			NUnit.Framework.Assert.IsTrue(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(4 / 6.0f, mergeQueue.GetProgress().Get(), epsilon
				);
			// Subsequent next() calls should read one key and move progress
			NUnit.Framework.Assert.IsTrue(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(5 / 6.0f, mergeQueue.GetProgress().Get(), epsilon
				);
			NUnit.Framework.Assert.IsTrue(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(1.0f, mergeQueue.GetProgress().Get(), epsilon);
			// Now there should be no more input
			NUnit.Framework.Assert.IsFalse(mergeQueue.Next());
			NUnit.Framework.Assert.AreEqual(1.0f, mergeQueue.GetProgress().Get(), epsilon);
			NUnit.Framework.Assert.IsTrue(mergeQueue.GetKey() == null);
			NUnit.Framework.Assert.AreEqual(0, mergeQueue.GetValue().GetData().Length);
		}

		private Progressable GetReporter()
		{
			Progressable reporter = new _Progressable_302();
			return reporter;
		}

		private sealed class _Progressable_302 : Progressable
		{
			public _Progressable_302()
			{
			}

			public void Progress()
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<Merger.Segment<Text, Text>> GetUncompressedSegments()
		{
			IList<Merger.Segment<Text, Text>> segments = new AList<Merger.Segment<Text, Text>
				>();
			for (int i = 0; i < 2; i++)
			{
				segments.AddItem(GetUncompressedSegment(i));
			}
			return segments;
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<Merger.Segment<Text, Text>> GetCompressedSegments()
		{
			IList<Merger.Segment<Text, Text>> segments = new AList<Merger.Segment<Text, Text>
				>();
			for (int i = 0; i < 2; i++)
			{
				segments.AddItem(GetCompressedSegment(i));
			}
			return segments;
		}

		/// <exception cref="System.IO.IOException"/>
		private Merger.Segment<Text, Text> GetUncompressedSegment(int i)
		{
			return new Merger.Segment<Text, Text>(GetReader(i, false), false);
		}

		/// <exception cref="System.IO.IOException"/>
		private Merger.Segment<Text, Text> GetCompressedSegment(int i)
		{
			return new Merger.Segment<Text, Text>(GetReader(i, true), false, 3000l);
		}

		/// <exception cref="System.IO.IOException"/>
		private IFile.Reader<Text, Text> GetReader(int i, bool isCompressedInput)
		{
			IFile.Reader<Text, Text> readerMock = Org.Mockito.Mockito.Mock<IFile.Reader>();
			Org.Mockito.Mockito.When(readerMock.GetLength()).ThenReturn(30l);
			Org.Mockito.Mockito.When(readerMock.GetPosition()).ThenReturn(0l).ThenReturn(10l)
				.ThenReturn(20l);
			Org.Mockito.Mockito.When(readerMock.NextRawKey(Matchers.Any<DataInputBuffer>())).
				ThenAnswer(GetKeyAnswer("Segment" + i, isCompressedInput));
			Org.Mockito.Mockito.DoAnswer(GetValueAnswer("Segment" + i)).When(readerMock).NextRawValue
				(Matchers.Any<DataInputBuffer>());
			return readerMock;
		}

		private Answer<object> GetKeyAnswer(string segmentName, bool isCompressedInput)
		{
			return new _Answer_352(isCompressedInput, segmentName);
		}

		private sealed class _Answer_352 : Answer<object>
		{
			public _Answer_352(bool isCompressedInput, string segmentName)
			{
				this.isCompressedInput = isCompressedInput;
				this.segmentName = segmentName;
				this.i = 0;
			}

			internal int i;

			public bool Answer(InvocationOnMock invocation)
			{
				if (this.i++ == 3)
				{
					return false;
				}
				IFile.Reader<Text, Text> mock = (IFile.Reader<Text, Text>)invocation.GetMock();
				int multiplier = isCompressedInput ? 100 : 1;
				mock.bytesRead += 10 * multiplier;
				object[] args = invocation.GetArguments();
				DataInputBuffer key = (DataInputBuffer)args[0];
				key.Reset(Sharpen.Runtime.GetBytesForString(("Segment Key " + segmentName + this.
					i)), 20);
				return true;
			}

			private readonly bool isCompressedInput;

			private readonly string segmentName;
		}

		private Answer<object> GetValueAnswer(string segmentName)
		{
			return new _Answer_372(segmentName);
		}

		private sealed class _Answer_372 : Answer<Void>
		{
			public _Answer_372(string segmentName)
			{
				this.segmentName = segmentName;
				this.i = 0;
			}

			internal int i;

			public Void Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				DataInputBuffer key = (DataInputBuffer)args[0];
				key.Reset(Sharpen.Runtime.GetBytesForString(("Segment Value " + segmentName + this
					.i)), 20);
				return null;
			}

			private readonly string segmentName;
		}
	}
}
