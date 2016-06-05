using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	public class TestConfigurationDeprecation
	{
		private Configuration conf;

		internal static readonly string Config = new FilePath("./test-config-TestConfigurationDeprecation.xml"
			).GetAbsolutePath();

		internal static readonly string Config2 = new FilePath("./test-config2-TestConfigurationDeprecation.xml"
			).GetAbsolutePath();

		internal static readonly string Config3 = new FilePath("./test-config3-TestConfigurationDeprecation.xml"
			).GetAbsolutePath();

		internal BufferedWriter @out;

		static TestConfigurationDeprecation()
		{
			Configuration.AddDefaultResource("test-fake-default.xml");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			new FilePath(Config).Delete();
			new FilePath(Config2).Delete();
			new FilePath(Config3).Delete();
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartConfig()
		{
			@out.Write("<?xml version=\"1.0\"?>\n");
			@out.Write("<configuration>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private void EndConfig()
		{
			@out.Write("</configuration>\n");
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AppendProperty(string name, string val)
		{
			AppendProperty(name, val, false);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AppendProperty(string name, string val, bool isFinal)
		{
			@out.Write("<property>");
			@out.Write("<name>");
			@out.Write(name);
			@out.Write("</name>");
			@out.Write("<value>");
			@out.Write(val);
			@out.Write("</value>");
			if (isFinal)
			{
				@out.Write("<final>true</final>");
			}
			@out.Write("</property>\n");
		}

		private void AddDeprecationToConfiguration()
		{
			Configuration.AddDeprecation("A", new string[] { "B" });
			Configuration.AddDeprecation("C", new string[] { "D" });
			Configuration.AddDeprecation("E", new string[] { "F" });
			Configuration.AddDeprecation("G", new string[] { "H" });
			Configuration.AddDeprecation("I", new string[] { "J" });
			Configuration.AddDeprecation("M", new string[] { "N" });
			Configuration.AddDeprecation("X", new string[] { "Y", "Z" });
			Configuration.AddDeprecation("P", new string[] { "Q", "R" });
		}

		/// <summary>
		/// This test checks the correctness of loading/setting the properties in terms
		/// of occurrence of deprecated keys.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		[Fact]
		public virtual void TestDeprecation()
		{
			AddDeprecationToConfiguration();
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			// load an old key and a new key.
			AppendProperty("A", "a");
			AppendProperty("D", "d");
			// load an old key with multiple new-key mappings
			AppendProperty("P", "p");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			// check if loading of old key with multiple new-key mappings actually loads
			// the corresponding new keys. 
			Assert.Equal("p", conf.Get("P"));
			Assert.Equal("p", conf.Get("Q"));
			Assert.Equal("p", conf.Get("R"));
			Assert.Equal("a", conf.Get("A"));
			Assert.Equal("a", conf.Get("B"));
			Assert.Equal("d", conf.Get("C"));
			Assert.Equal("d", conf.Get("D"));
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			// load the old/new keys corresponding to the keys loaded before.
			AppendProperty("B", "b");
			AppendProperty("C", "c");
			EndConfig();
			Path fileResource1 = new Path(Config2);
			conf.AddResource(fileResource1);
			Assert.Equal("b", conf.Get("A"));
			Assert.Equal("b", conf.Get("B"));
			Assert.Equal("c", conf.Get("C"));
			Assert.Equal("c", conf.Get("D"));
			// set new key
			conf.Set("N", "n");
			// get old key
			Assert.Equal("n", conf.Get("M"));
			// check consistency in get of old and new keys
			Assert.Equal(conf.Get("M"), conf.Get("N"));
			// set old key and then get new key(s).
			conf.Set("M", "m");
			Assert.Equal("m", conf.Get("N"));
			conf.Set("X", "x");
			Assert.Equal("x", conf.Get("X"));
			Assert.Equal("x", conf.Get("Y"));
			Assert.Equal("x", conf.Get("Z"));
			// set new keys to different values
			conf.Set("Y", "y");
			conf.Set("Z", "z");
			// get old key
			Assert.Equal("z", conf.Get("X"));
		}

		/// <summary>
		/// This test is to ensure the correctness of loading of keys with respect to
		/// being marked as final and that are related to deprecation.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeprecationForFinalParameters()
		{
			AddDeprecationToConfiguration();
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			// set the following keys:
			// 1.old key and final
			// 2.new key whose corresponding old key is final
			// 3.old key whose corresponding new key is final
			// 4.new key and final
			// 5.new key which is final and has null value.
			AppendProperty("A", "a", true);
			AppendProperty("D", "d");
			AppendProperty("E", "e");
			AppendProperty("H", "h", true);
			AppendProperty("J", string.Empty, true);
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal("a", conf.Get("A"));
			Assert.Equal("a", conf.Get("B"));
			Assert.Equal("d", conf.Get("C"));
			Assert.Equal("d", conf.Get("D"));
			Assert.Equal("e", conf.Get("E"));
			Assert.Equal("e", conf.Get("F"));
			Assert.Equal("h", conf.Get("G"));
			Assert.Equal("h", conf.Get("H"));
			NUnit.Framework.Assert.IsNull(conf.Get("I"));
			NUnit.Framework.Assert.IsNull(conf.Get("J"));
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			// add the corresponding old/new keys of those added to CONFIG1
			AppendProperty("B", "b");
			AppendProperty("C", "c", true);
			AppendProperty("F", "f", true);
			AppendProperty("G", "g");
			AppendProperty("I", "i");
			EndConfig();
			Path fileResource1 = new Path(Config2);
			conf.AddResource(fileResource1);
			Assert.Equal("a", conf.Get("A"));
			Assert.Equal("a", conf.Get("B"));
			Assert.Equal("c", conf.Get("C"));
			Assert.Equal("c", conf.Get("D"));
			Assert.Equal("f", conf.Get("E"));
			Assert.Equal("f", conf.Get("F"));
			Assert.Equal("h", conf.Get("G"));
			Assert.Equal("h", conf.Get("H"));
			NUnit.Framework.Assert.IsNull(conf.Get("I"));
			NUnit.Framework.Assert.IsNull(conf.Get("J"));
			@out = new BufferedWriter(new FileWriter(Config3));
			StartConfig();
			// change the values of all the previously loaded 
			// keys (both deprecated and new)
			AppendProperty("A", "a1");
			AppendProperty("B", "b1");
			AppendProperty("C", "c1");
			AppendProperty("D", "d1");
			AppendProperty("E", "e1");
			AppendProperty("F", "f1");
			AppendProperty("G", "g1");
			AppendProperty("H", "h1");
			AppendProperty("I", "i1");
			AppendProperty("J", "j1");
			EndConfig();
			fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal("a", conf.Get("A"));
			Assert.Equal("a", conf.Get("B"));
			Assert.Equal("c", conf.Get("C"));
			Assert.Equal("c", conf.Get("D"));
			Assert.Equal("f", conf.Get("E"));
			Assert.Equal("f", conf.Get("F"));
			Assert.Equal("h", conf.Get("G"));
			Assert.Equal("h", conf.Get("H"));
			NUnit.Framework.Assert.IsNull(conf.Get("I"));
			NUnit.Framework.Assert.IsNull(conf.Get("J"));
		}

		[Fact]
		public virtual void TestSetBeforeAndGetAfterDeprecation()
		{
			Configuration conf = new Configuration();
			conf.Set("oldkey", "hello");
			Configuration.AddDeprecation("oldkey", new string[] { "newkey" });
			Assert.Equal("hello", conf.Get("newkey"));
		}

		[Fact]
		public virtual void TestSetBeforeAndGetAfterDeprecationAndDefaults()
		{
			Configuration conf = new Configuration();
			conf.Set("tests.fake-default.old-key", "hello");
			Configuration.AddDeprecation("tests.fake-default.old-key", new string[] { "tests.fake-default.new-key"
				 });
			Assert.Equal("hello", conf.Get("tests.fake-default.new-key"));
		}

		[Fact]
		public virtual void TestIteratorWithDeprecatedKeys()
		{
			Configuration conf = new Configuration();
			Configuration.AddDeprecation("dK", new string[] { "nK" });
			conf.Set("k", "v");
			conf.Set("dK", "V");
			Assert.Equal("V", conf.Get("dK"));
			Assert.Equal("V", conf.Get("nK"));
			conf.Set("nK", "VV");
			Assert.Equal("VV", conf.Get("dK"));
			Assert.Equal("VV", conf.Get("nK"));
			bool kFound = false;
			bool dKFound = false;
			bool nKFound = false;
			foreach (KeyValuePair<string, string> entry in conf)
			{
				if (entry.Key.Equals("k"))
				{
					Assert.Equal("v", entry.Value);
					kFound = true;
				}
				if (entry.Key.Equals("dK"))
				{
					Assert.Equal("VV", entry.Value);
					dKFound = true;
				}
				if (entry.Key.Equals("nK"))
				{
					Assert.Equal("VV", entry.Value);
					nKFound = true;
				}
			}
			Assert.True("regular Key not found", kFound);
			Assert.True("deprecated Key not found", dKFound);
			Assert.True("new Key not found", nKFound);
		}

		[Fact]
		public virtual void TestUnsetWithDeprecatedKeys()
		{
			Configuration conf = new Configuration();
			Configuration.AddDeprecation("dK", new string[] { "nK" });
			conf.Set("nK", "VV");
			Assert.Equal("VV", conf.Get("dK"));
			Assert.Equal("VV", conf.Get("nK"));
			conf.Unset("dK");
			NUnit.Framework.Assert.IsNull(conf.Get("dK"));
			NUnit.Framework.Assert.IsNull(conf.Get("nK"));
			conf.Set("nK", "VV");
			Assert.Equal("VV", conf.Get("dK"));
			Assert.Equal("VV", conf.Get("nK"));
			conf.Unset("nK");
			NUnit.Framework.Assert.IsNull(conf.Get("dK"));
			NUnit.Framework.Assert.IsNull(conf.Get("nK"));
		}

		private static string GetTestKeyName(int threadIndex, int testIndex)
		{
			return "testConcurrentDeprecateAndManipulate.testKey." + threadIndex + "." + testIndex;
		}

		/// <summary>
		/// Run a set of threads making changes to the deprecations
		/// concurrently with another set of threads calling get()
		/// and set() on Configuration objects.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConcurrentDeprecateAndManipulate()
		{
			int NumThreadIds = 10;
			int NumKeysPerThread = 1000;
			ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2 * NumThreadIds
				, new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat("testConcurrentDeprecateAndManipulate modification thread %d"
				).Build());
			CountDownLatch latch = new CountDownLatch(1);
			AtomicInteger highestModificationThreadId = new AtomicInteger(1);
			IList<Future<Void>> futures = new List<Future<Void>>();
			for (int i = 0; i < NumThreadIds; i++)
			{
				futures.AddItem(executor.Schedule(new _Callable_363(latch, highestModificationThreadId
					, NumKeysPerThread), 0, TimeUnit.Seconds));
			}
			AtomicInteger highestAccessThreadId = new AtomicInteger(1);
			for (int i_1 = 0; i_1 < NumThreadIds; i_1++)
			{
				futures.AddItem(executor.Schedule(new _Callable_382(latch, highestAccessThreadId, 
					NumKeysPerThread), 0, TimeUnit.Seconds));
			}
			latch.CountDown();
			// allow all threads to proceed
			foreach (Future<Void> future in futures)
			{
				Uninterruptibles.GetUninterruptibly(future);
			}
		}

		private sealed class _Callable_363 : Callable<Void>
		{
			public _Callable_363(CountDownLatch latch, AtomicInteger highestModificationThreadId
				, int NumKeysPerThread)
			{
				this.latch = latch;
				this.highestModificationThreadId = highestModificationThreadId;
				this.NumKeysPerThread = NumKeysPerThread;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				latch.Await();
				int threadIndex = highestModificationThreadId.AddAndGet(1);
				for (int i = 0; i < NumKeysPerThread; i++)
				{
					string testKey = TestConfigurationDeprecation.GetTestKeyName(threadIndex, i);
					string testNewKey = testKey + ".new";
					Configuration.AddDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta
						(testKey, testNewKey) });
				}
				return null;
			}

			private readonly CountDownLatch latch;

			private readonly AtomicInteger highestModificationThreadId;

			private readonly int NumKeysPerThread;
		}

		private sealed class _Callable_382 : Callable<Void>
		{
			public _Callable_382(CountDownLatch latch, AtomicInteger highestAccessThreadId, int
				 NumKeysPerThread)
			{
				this.latch = latch;
				this.highestAccessThreadId = highestAccessThreadId;
				this.NumKeysPerThread = NumKeysPerThread;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				Configuration conf = new Configuration();
				latch.Await();
				int threadIndex = highestAccessThreadId.AddAndGet(1);
				for (int i = 0; i < NumKeysPerThread; i++)
				{
					string testNewKey = TestConfigurationDeprecation.GetTestKeyName(threadIndex, i) +
						 ".new";
					string value = "value." + threadIndex + "." + i;
					conf.Set(testNewKey, value);
					Assert.Equal(value, conf.Get(testNewKey));
				}
				return null;
			}

			private readonly CountDownLatch latch;

			private readonly AtomicInteger highestAccessThreadId;

			private readonly int NumKeysPerThread;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestNoFalseDeprecationWarning()
		{
			Configuration conf = new Configuration();
			Configuration.AddDeprecation("AA", "BB");
			conf.Set("BB", "bb");
			conf.Get("BB");
			conf.WriteXml(new ByteArrayOutputStream());
			Assert.Equal(false, Configuration.HasWarnedDeprecation("AA"));
			conf.Set("AA", "aa");
			Assert.Equal(true, Configuration.HasWarnedDeprecation("AA"));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeprecationSetUnset()
		{
			AddDeprecationToConfiguration();
			Configuration conf = new Configuration();
			//"X" is deprecated by "Y" and "Z"
			conf.Set("Y", "y");
			Assert.Equal("y", conf.Get("Z"));
			conf.Set("X", "x");
			Assert.Equal("x", conf.Get("Z"));
			conf.Unset("Y");
			Assert.Equal(null, conf.Get("Z"));
			Assert.Equal(null, conf.Get("X"));
		}
	}
}
