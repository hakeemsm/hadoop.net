using Sharpen;

namespace org.apache.hadoop.conf
{
	public class TestConfigurationDeprecation
	{
		private org.apache.hadoop.conf.Configuration conf;

		internal static readonly string CONFIG = new java.io.File("./test-config-TestConfigurationDeprecation.xml"
			).getAbsolutePath();

		internal static readonly string CONFIG2 = new java.io.File("./test-config2-TestConfigurationDeprecation.xml"
			).getAbsolutePath();

		internal static readonly string CONFIG3 = new java.io.File("./test-config3-TestConfigurationDeprecation.xml"
			).getAbsolutePath();

		internal java.io.BufferedWriter @out;

		static TestConfigurationDeprecation()
		{
			org.apache.hadoop.conf.Configuration.addDefaultResource("test-fake-default.xml");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			new java.io.File(CONFIG).delete();
			new java.io.File(CONFIG2).delete();
			new java.io.File(CONFIG3).delete();
		}

		/// <exception cref="System.IO.IOException"/>
		private void startConfig()
		{
			@out.write("<?xml version=\"1.0\"?>\n");
			@out.write("<configuration>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private void endConfig()
		{
			@out.write("</configuration>\n");
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void appendProperty(string name, string val)
		{
			appendProperty(name, val, false);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void appendProperty(string name, string val, bool isFinal)
		{
			@out.write("<property>");
			@out.write("<name>");
			@out.write(name);
			@out.write("</name>");
			@out.write("<value>");
			@out.write(val);
			@out.write("</value>");
			if (isFinal)
			{
				@out.write("<final>true</final>");
			}
			@out.write("</property>\n");
		}

		private void addDeprecationToConfiguration()
		{
			org.apache.hadoop.conf.Configuration.addDeprecation("A", new string[] { "B" });
			org.apache.hadoop.conf.Configuration.addDeprecation("C", new string[] { "D" });
			org.apache.hadoop.conf.Configuration.addDeprecation("E", new string[] { "F" });
			org.apache.hadoop.conf.Configuration.addDeprecation("G", new string[] { "H" });
			org.apache.hadoop.conf.Configuration.addDeprecation("I", new string[] { "J" });
			org.apache.hadoop.conf.Configuration.addDeprecation("M", new string[] { "N" });
			org.apache.hadoop.conf.Configuration.addDeprecation("X", new string[] { "Y", "Z" }
				);
			org.apache.hadoop.conf.Configuration.addDeprecation("P", new string[] { "Q", "R" }
				);
		}

		/// <summary>
		/// This test checks the correctness of loading/setting the properties in terms
		/// of occurrence of deprecated keys.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		[NUnit.Framework.Test]
		public virtual void testDeprecation()
		{
			addDeprecationToConfiguration();
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			// load an old key and a new key.
			appendProperty("A", "a");
			appendProperty("D", "d");
			// load an old key with multiple new-key mappings
			appendProperty("P", "p");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			// check if loading of old key with multiple new-key mappings actually loads
			// the corresponding new keys. 
			NUnit.Framework.Assert.AreEqual("p", conf.get("P"));
			NUnit.Framework.Assert.AreEqual("p", conf.get("Q"));
			NUnit.Framework.Assert.AreEqual("p", conf.get("R"));
			NUnit.Framework.Assert.AreEqual("a", conf.get("A"));
			NUnit.Framework.Assert.AreEqual("a", conf.get("B"));
			NUnit.Framework.Assert.AreEqual("d", conf.get("C"));
			NUnit.Framework.Assert.AreEqual("d", conf.get("D"));
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			// load the old/new keys corresponding to the keys loaded before.
			appendProperty("B", "b");
			appendProperty("C", "c");
			endConfig();
			org.apache.hadoop.fs.Path fileResource1 = new org.apache.hadoop.fs.Path(CONFIG2);
			conf.addResource(fileResource1);
			NUnit.Framework.Assert.AreEqual("b", conf.get("A"));
			NUnit.Framework.Assert.AreEqual("b", conf.get("B"));
			NUnit.Framework.Assert.AreEqual("c", conf.get("C"));
			NUnit.Framework.Assert.AreEqual("c", conf.get("D"));
			// set new key
			conf.set("N", "n");
			// get old key
			NUnit.Framework.Assert.AreEqual("n", conf.get("M"));
			// check consistency in get of old and new keys
			NUnit.Framework.Assert.AreEqual(conf.get("M"), conf.get("N"));
			// set old key and then get new key(s).
			conf.set("M", "m");
			NUnit.Framework.Assert.AreEqual("m", conf.get("N"));
			conf.set("X", "x");
			NUnit.Framework.Assert.AreEqual("x", conf.get("X"));
			NUnit.Framework.Assert.AreEqual("x", conf.get("Y"));
			NUnit.Framework.Assert.AreEqual("x", conf.get("Z"));
			// set new keys to different values
			conf.set("Y", "y");
			conf.set("Z", "z");
			// get old key
			NUnit.Framework.Assert.AreEqual("z", conf.get("X"));
		}

		/// <summary>
		/// This test is to ensure the correctness of loading of keys with respect to
		/// being marked as final and that are related to deprecation.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeprecationForFinalParameters()
		{
			addDeprecationToConfiguration();
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			// set the following keys:
			// 1.old key and final
			// 2.new key whose corresponding old key is final
			// 3.old key whose corresponding new key is final
			// 4.new key and final
			// 5.new key which is final and has null value.
			appendProperty("A", "a", true);
			appendProperty("D", "d");
			appendProperty("E", "e");
			appendProperty("H", "h", true);
			appendProperty("J", string.Empty, true);
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual("a", conf.get("A"));
			NUnit.Framework.Assert.AreEqual("a", conf.get("B"));
			NUnit.Framework.Assert.AreEqual("d", conf.get("C"));
			NUnit.Framework.Assert.AreEqual("d", conf.get("D"));
			NUnit.Framework.Assert.AreEqual("e", conf.get("E"));
			NUnit.Framework.Assert.AreEqual("e", conf.get("F"));
			NUnit.Framework.Assert.AreEqual("h", conf.get("G"));
			NUnit.Framework.Assert.AreEqual("h", conf.get("H"));
			NUnit.Framework.Assert.IsNull(conf.get("I"));
			NUnit.Framework.Assert.IsNull(conf.get("J"));
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			// add the corresponding old/new keys of those added to CONFIG1
			appendProperty("B", "b");
			appendProperty("C", "c", true);
			appendProperty("F", "f", true);
			appendProperty("G", "g");
			appendProperty("I", "i");
			endConfig();
			org.apache.hadoop.fs.Path fileResource1 = new org.apache.hadoop.fs.Path(CONFIG2);
			conf.addResource(fileResource1);
			NUnit.Framework.Assert.AreEqual("a", conf.get("A"));
			NUnit.Framework.Assert.AreEqual("a", conf.get("B"));
			NUnit.Framework.Assert.AreEqual("c", conf.get("C"));
			NUnit.Framework.Assert.AreEqual("c", conf.get("D"));
			NUnit.Framework.Assert.AreEqual("f", conf.get("E"));
			NUnit.Framework.Assert.AreEqual("f", conf.get("F"));
			NUnit.Framework.Assert.AreEqual("h", conf.get("G"));
			NUnit.Framework.Assert.AreEqual("h", conf.get("H"));
			NUnit.Framework.Assert.IsNull(conf.get("I"));
			NUnit.Framework.Assert.IsNull(conf.get("J"));
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG3));
			startConfig();
			// change the values of all the previously loaded 
			// keys (both deprecated and new)
			appendProperty("A", "a1");
			appendProperty("B", "b1");
			appendProperty("C", "c1");
			appendProperty("D", "d1");
			appendProperty("E", "e1");
			appendProperty("F", "f1");
			appendProperty("G", "g1");
			appendProperty("H", "h1");
			appendProperty("I", "i1");
			appendProperty("J", "j1");
			endConfig();
			fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual("a", conf.get("A"));
			NUnit.Framework.Assert.AreEqual("a", conf.get("B"));
			NUnit.Framework.Assert.AreEqual("c", conf.get("C"));
			NUnit.Framework.Assert.AreEqual("c", conf.get("D"));
			NUnit.Framework.Assert.AreEqual("f", conf.get("E"));
			NUnit.Framework.Assert.AreEqual("f", conf.get("F"));
			NUnit.Framework.Assert.AreEqual("h", conf.get("G"));
			NUnit.Framework.Assert.AreEqual("h", conf.get("H"));
			NUnit.Framework.Assert.IsNull(conf.get("I"));
			NUnit.Framework.Assert.IsNull(conf.get("J"));
		}

		[NUnit.Framework.Test]
		public virtual void testSetBeforeAndGetAfterDeprecation()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("oldkey", "hello");
			org.apache.hadoop.conf.Configuration.addDeprecation("oldkey", new string[] { "newkey"
				 });
			NUnit.Framework.Assert.AreEqual("hello", conf.get("newkey"));
		}

		[NUnit.Framework.Test]
		public virtual void testSetBeforeAndGetAfterDeprecationAndDefaults()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("tests.fake-default.old-key", "hello");
			org.apache.hadoop.conf.Configuration.addDeprecation("tests.fake-default.old-key", 
				new string[] { "tests.fake-default.new-key" });
			NUnit.Framework.Assert.AreEqual("hello", conf.get("tests.fake-default.new-key"));
		}

		[NUnit.Framework.Test]
		public virtual void testIteratorWithDeprecatedKeys()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.conf.Configuration.addDeprecation("dK", new string[] { "nK" });
			conf.set("k", "v");
			conf.set("dK", "V");
			NUnit.Framework.Assert.AreEqual("V", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("V", conf.get("nK"));
			conf.set("nK", "VV");
			NUnit.Framework.Assert.AreEqual("VV", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("VV", conf.get("nK"));
			bool kFound = false;
			bool dKFound = false;
			bool nKFound = false;
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry in conf)
			{
				if (entry.Key.Equals("k"))
				{
					NUnit.Framework.Assert.AreEqual("v", entry.Value);
					kFound = true;
				}
				if (entry.Key.Equals("dK"))
				{
					NUnit.Framework.Assert.AreEqual("VV", entry.Value);
					dKFound = true;
				}
				if (entry.Key.Equals("nK"))
				{
					NUnit.Framework.Assert.AreEqual("VV", entry.Value);
					nKFound = true;
				}
			}
			NUnit.Framework.Assert.IsTrue("regular Key not found", kFound);
			NUnit.Framework.Assert.IsTrue("deprecated Key not found", dKFound);
			NUnit.Framework.Assert.IsTrue("new Key not found", nKFound);
		}

		[NUnit.Framework.Test]
		public virtual void testUnsetWithDeprecatedKeys()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.conf.Configuration.addDeprecation("dK", new string[] { "nK" });
			conf.set("nK", "VV");
			NUnit.Framework.Assert.AreEqual("VV", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("VV", conf.get("nK"));
			conf.unset("dK");
			NUnit.Framework.Assert.IsNull(conf.get("dK"));
			NUnit.Framework.Assert.IsNull(conf.get("nK"));
			conf.set("nK", "VV");
			NUnit.Framework.Assert.AreEqual("VV", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("VV", conf.get("nK"));
			conf.unset("nK");
			NUnit.Framework.Assert.IsNull(conf.get("dK"));
			NUnit.Framework.Assert.IsNull(conf.get("nK"));
		}

		private static string getTestKeyName(int threadIndex, int testIndex)
		{
			return "testConcurrentDeprecateAndManipulate.testKey." + threadIndex + "." + testIndex;
		}

		/// <summary>
		/// Run a set of threads making changes to the deprecations
		/// concurrently with another set of threads calling get()
		/// and set() on Configuration objects.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testConcurrentDeprecateAndManipulate()
		{
			int NUM_THREAD_IDS = 10;
			int NUM_KEYS_PER_THREAD = 1000;
			java.util.concurrent.ScheduledThreadPoolExecutor executor = new java.util.concurrent.ScheduledThreadPoolExecutor
				(2 * NUM_THREAD_IDS, new com.google.common.util.concurrent.ThreadFactoryBuilder(
				).setDaemon(true).setNameFormat("testConcurrentDeprecateAndManipulate modification thread %d"
				).build());
			java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch
				(1);
			java.util.concurrent.atomic.AtomicInteger highestModificationThreadId = new java.util.concurrent.atomic.AtomicInteger
				(1);
			System.Collections.Generic.IList<java.util.concurrent.Future<java.lang.Void>> futures
				 = new System.Collections.Generic.LinkedList<java.util.concurrent.Future<java.lang.Void
				>>();
			for (int i = 0; i < NUM_THREAD_IDS; i++)
			{
				futures.add(executor.schedule(new _Callable_363(latch, highestModificationThreadId
					, NUM_KEYS_PER_THREAD), 0, java.util.concurrent.TimeUnit.SECONDS));
			}
			java.util.concurrent.atomic.AtomicInteger highestAccessThreadId = new java.util.concurrent.atomic.AtomicInteger
				(1);
			for (int i_1 = 0; i_1 < NUM_THREAD_IDS; i_1++)
			{
				futures.add(executor.schedule(new _Callable_382(latch, highestAccessThreadId, NUM_KEYS_PER_THREAD
					), 0, java.util.concurrent.TimeUnit.SECONDS));
			}
			latch.countDown();
			// allow all threads to proceed
			foreach (java.util.concurrent.Future<java.lang.Void> future in futures)
			{
				com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly(future);
			}
		}

		private sealed class _Callable_363 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_363(java.util.concurrent.CountDownLatch latch, java.util.concurrent.atomic.AtomicInteger
				 highestModificationThreadId, int NUM_KEYS_PER_THREAD)
			{
				this.latch = latch;
				this.highestModificationThreadId = highestModificationThreadId;
				this.NUM_KEYS_PER_THREAD = NUM_KEYS_PER_THREAD;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				latch.await();
				int threadIndex = highestModificationThreadId.addAndGet(1);
				for (int i = 0; i < NUM_KEYS_PER_THREAD; i++)
				{
					string testKey = org.apache.hadoop.conf.TestConfigurationDeprecation.getTestKeyName
						(threadIndex, i);
					string testNewKey = testKey + ".new";
					org.apache.hadoop.conf.Configuration.addDeprecations(new org.apache.hadoop.conf.Configuration.DeprecationDelta
						[] { new org.apache.hadoop.conf.Configuration.DeprecationDelta(testKey, testNewKey
						) });
				}
				return null;
			}

			private readonly java.util.concurrent.CountDownLatch latch;

			private readonly java.util.concurrent.atomic.AtomicInteger highestModificationThreadId;

			private readonly int NUM_KEYS_PER_THREAD;
		}

		private sealed class _Callable_382 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_382(java.util.concurrent.CountDownLatch latch, java.util.concurrent.atomic.AtomicInteger
				 highestAccessThreadId, int NUM_KEYS_PER_THREAD)
			{
				this.latch = latch;
				this.highestAccessThreadId = highestAccessThreadId;
				this.NUM_KEYS_PER_THREAD = NUM_KEYS_PER_THREAD;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					();
				latch.await();
				int threadIndex = highestAccessThreadId.addAndGet(1);
				for (int i = 0; i < NUM_KEYS_PER_THREAD; i++)
				{
					string testNewKey = org.apache.hadoop.conf.TestConfigurationDeprecation.getTestKeyName
						(threadIndex, i) + ".new";
					string value = "value." + threadIndex + "." + i;
					conf.set(testNewKey, value);
					NUnit.Framework.Assert.AreEqual(value, conf.get(testNewKey));
				}
				return null;
			}

			private readonly java.util.concurrent.CountDownLatch latch;

			private readonly java.util.concurrent.atomic.AtomicInteger highestAccessThreadId;

			private readonly int NUM_KEYS_PER_THREAD;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testNoFalseDeprecationWarning()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.conf.Configuration.addDeprecation("AA", "BB");
			conf.set("BB", "bb");
			conf.get("BB");
			conf.writeXml(new java.io.ByteArrayOutputStream());
			NUnit.Framework.Assert.AreEqual(false, org.apache.hadoop.conf.Configuration.hasWarnedDeprecation
				("AA"));
			conf.set("AA", "aa");
			NUnit.Framework.Assert.AreEqual(true, org.apache.hadoop.conf.Configuration.hasWarnedDeprecation
				("AA"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeprecationSetUnset()
		{
			addDeprecationToConfiguration();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			//"X" is deprecated by "Y" and "Z"
			conf.set("Y", "y");
			NUnit.Framework.Assert.AreEqual("y", conf.get("Z"));
			conf.set("X", "x");
			NUnit.Framework.Assert.AreEqual("x", conf.get("Z"));
			conf.unset("Y");
			NUnit.Framework.Assert.AreEqual(null, conf.get("Z"));
			NUnit.Framework.Assert.AreEqual(null, conf.get("X"));
		}
	}
}
