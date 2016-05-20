using Sharpen;

namespace org.apache.hadoop.conf
{
	public class TestConfiguration : NUnit.Framework.TestCase
	{
		private org.apache.hadoop.conf.Configuration conf;

		internal static readonly string CONFIG = new java.io.File("./test-config-TestConfiguration.xml"
			).getAbsolutePath();

		internal static readonly string CONFIG2 = new java.io.File("./test-config2-TestConfiguration.xml"
			).getAbsolutePath();

		internal static readonly string CONFIG_FOR_ENUM = new java.io.File("./test-config-enum-TestConfiguration.xml"
			).getAbsolutePath();

		private static readonly string CONFIG_MULTI_BYTE = new java.io.File("./test-config-multi-byte-TestConfiguration.xml"
			).getAbsolutePath();

		private static readonly string CONFIG_MULTI_BYTE_SAVED = new java.io.File("./test-config-multi-byte-saved-TestConfiguration.xml"
			).getAbsolutePath();

		internal static readonly java.util.Random RAN = new java.util.Random();

		internal static readonly string XMLHEADER = org.apache.hadoop.util.PlatformName.IBM_JAVA
			 ? "<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>" : "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><configuration>";

		/// <exception cref="System.Exception"/>
		protected override void setUp()
		{
			base.setUp();
			conf = new org.apache.hadoop.conf.Configuration();
		}

		/// <exception cref="System.Exception"/>
		protected override void tearDown()
		{
			base.tearDown();
			new java.io.File(CONFIG).delete();
			new java.io.File(CONFIG2).delete();
			new java.io.File(CONFIG_FOR_ENUM).delete();
			new java.io.File(CONFIG_MULTI_BYTE).delete();
			new java.io.File(CONFIG_MULTI_BYTE_SAVED).delete();
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
		private void addInclude(string filename)
		{
			@out.write("<xi:include href=\"" + filename + "\" xmlns:xi=\"http://www.w3.org/2001/XInclude\"  />\n "
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testInputStreamResource()
		{
			System.IO.StringWriter writer = new System.IO.StringWriter();
			@out = new java.io.BufferedWriter(writer);
			startConfig();
			declareProperty("prop", "A", "A");
			endConfig();
			java.io.InputStream in1 = new java.io.ByteArrayInputStream(Sharpen.Runtime.getBytesForString
				(writer.ToString()));
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.addResource(in1);
			NUnit.Framework.Assert.AreEqual("A", conf.get("prop"));
			java.io.InputStream in2 = new java.io.ByteArrayInputStream(Sharpen.Runtime.getBytesForString
				(writer.ToString()));
			conf.addResource(in2);
			NUnit.Framework.Assert.AreEqual("A", conf.get("prop"));
		}

		/// <summary>Tests use of multi-byte characters in property names and values.</summary>
		/// <remarks>
		/// Tests use of multi-byte characters in property names and values.  This test
		/// round-trips multi-byte string literals through saving and loading of config
		/// and asserts that the same values were read.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testMultiByteCharacters()
		{
			string priorDefaultEncoding = Sharpen.Runtime.getProperty("file.encoding");
			try
			{
				Sharpen.Runtime.setProperty("file.encoding", "US-ASCII");
				string name = "multi_byte_\u611b_name";
				string value = "multi_byte_\u0641_value";
				@out = new java.io.BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream
					(CONFIG_MULTI_BYTE), "UTF-8"));
				startConfig();
				declareProperty(name, value, value);
				endConfig();
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					(false);
				conf.addResource(new org.apache.hadoop.fs.Path(CONFIG_MULTI_BYTE));
				NUnit.Framework.Assert.AreEqual(value, conf.get(name));
				java.io.FileOutputStream fos = new java.io.FileOutputStream(CONFIG_MULTI_BYTE_SAVED
					);
				try
				{
					conf.writeXml(fos);
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.closeStream(fos);
				}
				conf = new org.apache.hadoop.conf.Configuration(false);
				conf.addResource(new org.apache.hadoop.fs.Path(CONFIG_MULTI_BYTE_SAVED));
				NUnit.Framework.Assert.AreEqual(value, conf.get(name));
			}
			finally
			{
				Sharpen.Runtime.setProperty("file.encoding", priorDefaultEncoding);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVariableSubstitution()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			declareProperty("my.int", "${intvar}", "42");
			declareProperty("intvar", "42", "42");
			declareProperty("my.base", "/tmp/${user.name}", UNSPEC);
			declareProperty("my.file", "hello", "hello");
			declareProperty("my.suffix", ".txt", ".txt");
			declareProperty("my.relfile", "${my.file}${my.suffix}", "hello.txt");
			declareProperty("my.fullfile", "${my.base}/${my.file}${my.suffix}", UNSPEC);
			// check that undefined variables are returned as-is
			declareProperty("my.failsexpand", "a${my.undefvar}b", "a${my.undefvar}b");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			foreach (org.apache.hadoop.conf.TestConfiguration.Prop p in props)
			{
				System.Console.Out.WriteLine("p=" + p.name);
				string gotVal = conf.get(p.name);
				string gotRawVal = conf.getRaw(p.name);
				assertEq(p.val, gotRawVal);
				if (p.expectEval == UNSPEC)
				{
					// expansion is system-dependent (uses System properties)
					// can't do exact match so just check that all variables got expanded
					NUnit.Framework.Assert.IsTrue(gotVal != null && -1 == gotVal.IndexOf("${"));
				}
				else
				{
					assertEq(p.expectEval, gotVal);
				}
			}
			// check that expansion also occurs for getInt()
			NUnit.Framework.Assert.IsTrue(conf.getInt("intvar", -1) == 42);
			NUnit.Framework.Assert.IsTrue(conf.getInt("my.int", -1) == 42);
			System.Collections.Generic.IDictionary<string, string> results = conf.getValByRegex
				("^my.*file$");
			NUnit.Framework.Assert.IsTrue(results.Keys.contains("my.relfile"));
			NUnit.Framework.Assert.IsTrue(results.Keys.contains("my.fullfile"));
			NUnit.Framework.Assert.IsTrue(results.Keys.contains("my.file"));
			NUnit.Framework.Assert.AreEqual(-1, results["my.relfile"].IndexOf("${"));
			NUnit.Framework.Assert.AreEqual(-1, results["my.fullfile"].IndexOf("${"));
			NUnit.Framework.Assert.AreEqual(-1, results["my.file"].IndexOf("${"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFinalParam()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			declareProperty("my.var", string.Empty, string.Empty, true);
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			org.apache.hadoop.conf.Configuration conf1 = new org.apache.hadoop.conf.Configuration
				();
			conf1.addResource(fileResource);
			NUnit.Framework.Assert.IsNull("my var is not null", conf1.get("my.var"));
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			declareProperty("my.var", "myval", "myval", false);
			endConfig();
			fileResource = new org.apache.hadoop.fs.Path(CONFIG2);
			org.apache.hadoop.conf.Configuration conf2 = new org.apache.hadoop.conf.Configuration
				(conf1);
			conf2.addResource(fileResource);
			NUnit.Framework.Assert.IsNull("my var is not final", conf2.get("my.var"));
		}

		public static void assertEq(object a, object b)
		{
			System.Console.Out.WriteLine("assertEq: " + a + ", " + b);
			NUnit.Framework.Assert.AreEqual(a, b);
		}

		internal class Prop
		{
			internal string name;

			internal string val;

			internal string expectEval;
		}

		internal readonly string UNSPEC = null;

		internal System.Collections.Generic.List<org.apache.hadoop.conf.TestConfiguration.Prop
			> props = new System.Collections.Generic.List<org.apache.hadoop.conf.TestConfiguration.Prop
			>();

		/// <exception cref="System.IO.IOException"/>
		internal virtual void declareProperty(string name, string val, string expectEval)
		{
			declareProperty(name, val, expectEval, false);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void declareProperty(string name, string val, string expectEval, 
			bool isFinal)
		{
			appendProperty(name, val, isFinal);
			org.apache.hadoop.conf.TestConfiguration.Prop p = new org.apache.hadoop.conf.TestConfiguration.Prop
				();
			p.name = name;
			p.val = val;
			p.expectEval = expectEval;
			props.add(p);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void appendProperty(string name, string val)
		{
			appendProperty(name, val, false);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void appendProperty(string name, string val, bool isFinal, params 
			string[] sources)
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
			foreach (string s in sources)
			{
				@out.write("<source>");
				@out.write(s);
				@out.write("</source>");
			}
			@out.write("</property>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testOverlay()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("a", "b");
			appendProperty("b", "c");
			appendProperty("d", "e");
			appendProperty("e", "f", true);
			endConfig();
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			appendProperty("a", "b");
			appendProperty("b", "d");
			appendProperty("e", "e");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			//set dynamically something
			conf.set("c", "d");
			conf.set("a", "d");
			org.apache.hadoop.conf.Configuration clone = new org.apache.hadoop.conf.Configuration
				(conf);
			clone.addResource(new org.apache.hadoop.fs.Path(CONFIG2));
			NUnit.Framework.Assert.AreEqual(clone.get("a"), "d");
			NUnit.Framework.Assert.AreEqual(clone.get("b"), "d");
			NUnit.Framework.Assert.AreEqual(clone.get("c"), "d");
			NUnit.Framework.Assert.AreEqual(clone.get("d"), "e");
			NUnit.Framework.Assert.AreEqual(clone.get("e"), "f");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCommentsInValue()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("my.comment", "this <!--comment here--> contains a comment");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			//two spaces one after "this", one before "contains"
			NUnit.Framework.Assert.AreEqual("this  contains a comment", conf.get("my.comment"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testTrim()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			string[] whitespaces = new string[] { string.Empty, " ", "\n", "\t" };
			string[] name = new string[100];
			for (int i = 0; i < name.Length; i++)
			{
				name[i] = "foo" + i;
				java.lang.StringBuilder prefix = new java.lang.StringBuilder();
				java.lang.StringBuilder postfix = new java.lang.StringBuilder();
				for (int j = 0; j < 3; j++)
				{
					prefix.Append(whitespaces[RAN.nextInt(whitespaces.Length)]);
					postfix.Append(whitespaces[RAN.nextInt(whitespaces.Length)]);
				}
				appendProperty(prefix + name[i] + postfix, name[i] + ".value");
			}
			endConfig();
			conf.addResource(new org.apache.hadoop.fs.Path(CONFIG));
			foreach (string n in name)
			{
				NUnit.Framework.Assert.AreEqual(n + ".value", conf.get(n));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetLocalPath()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string[] dirs = new string[] { "a", "b", "c" };
			for (int i = 0; i < dirs.Length; i++)
			{
				dirs[i] = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty("test.build.data"
					), dirs[i]).ToString();
			}
			conf.set("dirs", org.apache.commons.lang.StringUtils.join(dirs, ","));
			for (int i_1 = 0; i_1 < 1000; i_1++)
			{
				string localPath = conf.getLocalPath("dirs", "dir" + i_1).ToString();
				NUnit.Framework.Assert.IsTrue("Path doesn't end in specified dir: " + localPath, 
					localPath.EndsWith("dir" + i_1));
				NUnit.Framework.Assert.IsFalse("Path has internal whitespace: " + localPath, localPath
					.contains(" "));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetFile()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string[] dirs = new string[] { "a", "b", "c" };
			for (int i = 0; i < dirs.Length; i++)
			{
				dirs[i] = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty("test.build.data"
					), dirs[i]).ToString();
			}
			conf.set("dirs", org.apache.commons.lang.StringUtils.join(dirs, ","));
			for (int i_1 = 0; i_1 < 1000; i_1++)
			{
				string localPath = conf.getFile("dirs", "dir" + i_1).ToString();
				NUnit.Framework.Assert.IsTrue("Path doesn't end in specified dir: " + localPath, 
					localPath.EndsWith("dir" + i_1));
				NUnit.Framework.Assert.IsFalse("Path has internal whitespace: " + localPath, localPath
					.contains(" "));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testToString()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			string expectedOutput = "Configuration: core-default.xml, core-site.xml, " + fileResource
				.ToString();
			NUnit.Framework.Assert.AreEqual(expectedOutput, conf.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testWriteXml()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			conf.writeXml(baos);
			string result = baos.ToString();
			NUnit.Framework.Assert.IsTrue("Result has proper header", result.StartsWith(XMLHEADER
				));
			NUnit.Framework.Assert.IsTrue("Result has proper footer", result.EndsWith("</configuration>"
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIncludes()
		{
			tearDown();
			System.Console.Out.WriteLine("XXX testIncludes");
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			appendProperty("a", "b");
			appendProperty("c", "d");
			endConfig();
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			addInclude(CONFIG2);
			appendProperty("e", "f");
			appendProperty("g", "h");
			endConfig();
			// verify that the includes file contains all properties
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(conf.get("a"), "b");
			NUnit.Framework.Assert.AreEqual(conf.get("c"), "d");
			NUnit.Framework.Assert.AreEqual(conf.get("e"), "f");
			NUnit.Framework.Assert.AreEqual(conf.get("g"), "h");
			tearDown();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRelativeIncludes()
		{
			tearDown();
			string relConfig = new java.io.File("./tmp/test-config.xml").getAbsolutePath();
			string relConfig2 = new java.io.File("./tmp/test-config2.xml").getAbsolutePath();
			new java.io.File(new java.io.File(relConfig).getParent()).mkdirs();
			@out = new java.io.BufferedWriter(new java.io.FileWriter(relConfig2));
			startConfig();
			appendProperty("a", "b");
			endConfig();
			@out = new java.io.BufferedWriter(new java.io.FileWriter(relConfig));
			startConfig();
			// Add the relative path instead of the absolute one.
			addInclude(new java.io.File(relConfig2).getName());
			appendProperty("c", "d");
			endConfig();
			// verify that the includes file contains all properties
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(relConfig);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(conf.get("a"), "b");
			NUnit.Framework.Assert.AreEqual(conf.get("c"), "d");
			// Cleanup
			new java.io.File(relConfig).delete();
			new java.io.File(relConfig2).delete();
			new java.io.File(new java.io.File(relConfig).getParent()).delete();
		}

		internal java.io.BufferedWriter @out;

		public virtual void testIntegerRanges()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("first", "-100");
			conf.set("second", "4-6,9-10,27");
			conf.set("third", "34-");
			org.apache.hadoop.conf.Configuration.IntegerRanges range = conf.getRange("first", 
				null);
			System.Console.Out.WriteLine("first = " + range);
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(0));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(1));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(100));
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(101));
			range = conf.getRange("second", null);
			System.Console.Out.WriteLine("second = " + range);
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(3));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(4));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(6));
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(7));
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(8));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(9));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(10));
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(11));
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(26));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(27));
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(28));
			range = conf.getRange("third", null);
			System.Console.Out.WriteLine("third = " + range);
			NUnit.Framework.Assert.AreEqual(false, range.isIncluded(33));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(34));
			NUnit.Framework.Assert.AreEqual(true, range.isIncluded(100000000));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetRangeIterator()
		{
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				(false);
			org.apache.hadoop.conf.Configuration.IntegerRanges ranges = config.getRange("Test"
				, string.Empty);
			NUnit.Framework.Assert.IsFalse("Empty range has values", ranges.GetEnumerator().MoveNext
				());
			ranges = config.getRange("Test", "5");
			System.Collections.Generic.ICollection<int> expected = new java.util.HashSet<int>
				(java.util.Arrays.asList(5));
			System.Collections.Generic.ICollection<int> found = new java.util.HashSet<int>();
			foreach (int i in ranges)
			{
				found.add(i);
			}
			NUnit.Framework.Assert.AreEqual(expected, found);
			ranges = config.getRange("Test", "5-10,13-14");
			expected = new java.util.HashSet<int>(java.util.Arrays.asList(5, 6, 7, 8, 9, 10, 
				13, 14));
			found = new java.util.HashSet<int>();
			foreach (int i_1 in ranges)
			{
				found.add(i_1);
			}
			NUnit.Framework.Assert.AreEqual(expected, found);
			ranges = config.getRange("Test", "8-12, 5- 7");
			expected = new java.util.HashSet<int>(java.util.Arrays.asList(5, 6, 7, 8, 9, 10, 
				11, 12));
			found = new java.util.HashSet<int>();
			foreach (int i_2 in ranges)
			{
				found.add(i_2);
			}
			NUnit.Framework.Assert.AreEqual(expected, found);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testHexValues()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.hex1", "0x10");
			appendProperty("test.hex2", "0xF");
			appendProperty("test.hex3", "-0x10");
			// Invalid?
			appendProperty("test.hex4", "-0x10xyz");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(16, conf.getInt("test.hex1", 0));
			NUnit.Framework.Assert.AreEqual(16, conf.getLong("test.hex1", 0));
			NUnit.Framework.Assert.AreEqual(15, conf.getInt("test.hex2", 0));
			NUnit.Framework.Assert.AreEqual(15, conf.getLong("test.hex2", 0));
			NUnit.Framework.Assert.AreEqual(-16, conf.getInt("test.hex3", 0));
			NUnit.Framework.Assert.AreEqual(-16, conf.getLong("test.hex3", 0));
			try
			{
				conf.getLong("test.hex4", 0);
				fail("Property had invalid long value, but was read successfully.");
			}
			catch (java.lang.NumberFormatException)
			{
			}
			// pass
			try
			{
				conf.getInt("test.hex4", 0);
				fail("Property had invalid int value, but was read successfully.");
			}
			catch (java.lang.NumberFormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void testIntegerValues()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.int1", "20");
			appendProperty("test.int2", "020");
			appendProperty("test.int3", "-20");
			appendProperty("test.int4", " -20 ");
			appendProperty("test.int5", " -20xyz ");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(20, conf.getInt("test.int1", 0));
			NUnit.Framework.Assert.AreEqual(20, conf.getLong("test.int1", 0));
			NUnit.Framework.Assert.AreEqual(20, conf.getLongBytes("test.int1", 0));
			NUnit.Framework.Assert.AreEqual(20, conf.getInt("test.int2", 0));
			NUnit.Framework.Assert.AreEqual(20, conf.getLong("test.int2", 0));
			NUnit.Framework.Assert.AreEqual(20, conf.getLongBytes("test.int2", 0));
			NUnit.Framework.Assert.AreEqual(-20, conf.getInt("test.int3", 0));
			NUnit.Framework.Assert.AreEqual(-20, conf.getLong("test.int3", 0));
			NUnit.Framework.Assert.AreEqual(-20, conf.getLongBytes("test.int3", 0));
			NUnit.Framework.Assert.AreEqual(-20, conf.getInt("test.int4", 0));
			NUnit.Framework.Assert.AreEqual(-20, conf.getLong("test.int4", 0));
			NUnit.Framework.Assert.AreEqual(-20, conf.getLongBytes("test.int4", 0));
			try
			{
				conf.getInt("test.int5", 0);
				fail("Property had invalid int value, but was read successfully.");
			}
			catch (java.lang.NumberFormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void testHumanReadableValues()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.humanReadableValue1", "1m");
			appendProperty("test.humanReadableValue2", "1M");
			appendProperty("test.humanReadableValue5", "1MBCDE");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(1048576, conf.getLongBytes("test.humanReadableValue1"
				, 0));
			NUnit.Framework.Assert.AreEqual(1048576, conf.getLongBytes("test.humanReadableValue2"
				, 0));
			try
			{
				conf.getLongBytes("test.humanReadableValue5", 0);
				fail("Property had invalid human readable value, but was read successfully.");
			}
			catch (java.lang.NumberFormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void testBooleanValues()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.bool1", "true");
			appendProperty("test.bool2", "false");
			appendProperty("test.bool3", "  true ");
			appendProperty("test.bool4", " false ");
			appendProperty("test.bool5", "foo");
			appendProperty("test.bool6", "TRUE");
			appendProperty("test.bool7", "FALSE");
			appendProperty("test.bool8", string.Empty);
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(true, conf.getBoolean("test.bool1", false));
			NUnit.Framework.Assert.AreEqual(false, conf.getBoolean("test.bool2", true));
			NUnit.Framework.Assert.AreEqual(true, conf.getBoolean("test.bool3", false));
			NUnit.Framework.Assert.AreEqual(false, conf.getBoolean("test.bool4", true));
			NUnit.Framework.Assert.AreEqual(true, conf.getBoolean("test.bool5", true));
			NUnit.Framework.Assert.AreEqual(true, conf.getBoolean("test.bool6", false));
			NUnit.Framework.Assert.AreEqual(false, conf.getBoolean("test.bool7", true));
			NUnit.Framework.Assert.AreEqual(false, conf.getBoolean("test.bool8", false));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFloatValues()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.float1", "3.1415");
			appendProperty("test.float2", "003.1415");
			appendProperty("test.float3", "-3.1415");
			appendProperty("test.float4", " -3.1415 ");
			appendProperty("test.float5", "xyz-3.1415xyz");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(3.1415f, conf.getFloat("test.float1", 0.0f));
			NUnit.Framework.Assert.AreEqual(3.1415f, conf.getFloat("test.float2", 0.0f));
			NUnit.Framework.Assert.AreEqual(-3.1415f, conf.getFloat("test.float3", 0.0f));
			NUnit.Framework.Assert.AreEqual(-3.1415f, conf.getFloat("test.float4", 0.0f));
			try
			{
				conf.getFloat("test.float5", 0.0f);
				fail("Property had invalid float value, but was read successfully.");
			}
			catch (java.lang.NumberFormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void testDoubleValues()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.double1", "3.1415");
			appendProperty("test.double2", "003.1415");
			appendProperty("test.double3", "-3.1415");
			appendProperty("test.double4", " -3.1415 ");
			appendProperty("test.double5", "xyz-3.1415xyz");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual(3.1415, conf.getDouble("test.double1", 0.0));
			NUnit.Framework.Assert.AreEqual(3.1415, conf.getDouble("test.double2", 0.0));
			NUnit.Framework.Assert.AreEqual(-3.1415, conf.getDouble("test.double3", 0.0));
			NUnit.Framework.Assert.AreEqual(-3.1415, conf.getDouble("test.double4", 0.0));
			try
			{
				conf.getDouble("test.double5", 0.0);
				fail("Property had invalid double value, but was read successfully.");
			}
			catch (java.lang.NumberFormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetClass()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.class1", "java.lang.Integer");
			appendProperty("test.class2", " java.lang.Integer ");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual("java.lang.Integer", conf.getClass("test.class1", 
				null).getCanonicalName());
			NUnit.Framework.Assert.AreEqual("java.lang.Integer", conf.getClass("test.class2", 
				null).getCanonicalName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetClasses()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.classes1", "java.lang.Integer,java.lang.String");
			appendProperty("test.classes2", " java.lang.Integer , java.lang.String ");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			string[] expectedNames = new string[] { "java.lang.Integer", "java.lang.String" };
			java.lang.Class[] defaultClasses = new java.lang.Class[] {  };
			java.lang.Class[] classes1 = conf.getClasses("test.classes1", defaultClasses);
			java.lang.Class[] classes2 = conf.getClasses("test.classes2", defaultClasses);
			NUnit.Framework.Assert.assertArrayEquals(expectedNames, extractClassNames(classes1
				));
			NUnit.Framework.Assert.assertArrayEquals(expectedNames, extractClassNames(classes2
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetStringCollection()
		{
			org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration
				();
			c.set("x", " a, b\n,\nc ");
			System.Collections.Generic.ICollection<string> strs = c.getTrimmedStringCollection
				("x");
			NUnit.Framework.Assert.AreEqual(3, strs.Count);
			NUnit.Framework.Assert.assertArrayEquals(new string[] { "a", "b", "c" }, Sharpen.Collections.ToArray
				(strs, new string[0]));
			// Check that the result is mutable
			strs.add("z");
			// Make sure same is true for missing config
			strs = c.getStringCollection("does-not-exist");
			NUnit.Framework.Assert.AreEqual(0, strs.Count);
			strs.add("z");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetTrimmedStringCollection()
		{
			org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration
				();
			c.set("x", "a, b, c");
			System.Collections.Generic.ICollection<string> strs = c.getStringCollection("x");
			NUnit.Framework.Assert.AreEqual(3, strs.Count);
			NUnit.Framework.Assert.assertArrayEquals(new string[] { "a", " b", " c" }, Sharpen.Collections.ToArray
				(strs, new string[0]));
			// Check that the result is mutable
			strs.add("z");
			// Make sure same is true for missing config
			strs = c.getStringCollection("does-not-exist");
			NUnit.Framework.Assert.AreEqual(0, strs.Count);
			strs.add("z");
		}

		private static string[] extractClassNames(java.lang.Class[] classes)
		{
			string[] classNames = new string[classes.Length];
			for (int i = 0; i < classNames.Length; i++)
			{
				classNames[i] = classes[i].getCanonicalName();
			}
			return classNames;
		}

		internal enum Dingo
		{
			FOO,
			BAR
		}

		internal enum Yak
		{
			RAB,
			FOO
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testEnum()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Dingo.FOO);
			NUnit.Framework.Assert.AreSame(org.apache.hadoop.conf.TestConfiguration.Dingo.FOO
				, conf.getEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Dingo.BAR));
			NUnit.Framework.Assert.AreSame(org.apache.hadoop.conf.TestConfiguration.Yak.FOO, 
				conf.getEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Yak.RAB));
			conf.setEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Dingo.FOO);
			bool fail = false;
			try
			{
				conf.setEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Dingo.BAR);
				org.apache.hadoop.conf.TestConfiguration.Yak y = conf.getEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Yak
					.FOO);
			}
			catch (System.ArgumentException)
			{
				fail = true;
			}
			NUnit.Framework.Assert.IsTrue(fail);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testEnumFromXml()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG_FOR_ENUM));
			startConfig();
			appendProperty("test.enum", " \t \n   FOO \t \n");
			appendProperty("test.enum2", " \t \n   Yak.FOO \t \n");
			endConfig();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG_FOR_ENUM
				);
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreSame(org.apache.hadoop.conf.TestConfiguration.Yak.FOO, 
				conf.getEnum("test.enum", org.apache.hadoop.conf.TestConfiguration.Yak.FOO));
			bool fail = false;
			try
			{
				conf.getEnum("test.enum2", org.apache.hadoop.conf.TestConfiguration.Yak.FOO);
			}
			catch (System.ArgumentException)
			{
				fail = true;
			}
			NUnit.Framework.Assert.IsTrue(fail);
		}

		public virtual void testTimeDuration()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.setTimeDuration("test.time.a", 7L, java.util.concurrent.TimeUnit.SECONDS);
			NUnit.Framework.Assert.AreEqual("7s", conf.get("test.time.a"));
			NUnit.Framework.Assert.AreEqual(0L, conf.getTimeDuration("test.time.a", 30, java.util.concurrent.TimeUnit
				.MINUTES));
			NUnit.Framework.Assert.AreEqual(7L, conf.getTimeDuration("test.time.a", 30, java.util.concurrent.TimeUnit
				.SECONDS));
			NUnit.Framework.Assert.AreEqual(7000L, conf.getTimeDuration("test.time.a", 30, java.util.concurrent.TimeUnit
				.MILLISECONDS));
			NUnit.Framework.Assert.AreEqual(7000000L, conf.getTimeDuration("test.time.a", 30, 
				java.util.concurrent.TimeUnit.MICROSECONDS));
			NUnit.Framework.Assert.AreEqual(7000000000L, conf.getTimeDuration("test.time.a", 
				30, java.util.concurrent.TimeUnit.NANOSECONDS));
			conf.setTimeDuration("test.time.b", 1, java.util.concurrent.TimeUnit.DAYS);
			NUnit.Framework.Assert.AreEqual("1d", conf.get("test.time.b"));
			NUnit.Framework.Assert.AreEqual(1, conf.getTimeDuration("test.time.b", 1, java.util.concurrent.TimeUnit
				.DAYS));
			NUnit.Framework.Assert.AreEqual(24, conf.getTimeDuration("test.time.b", 1, java.util.concurrent.TimeUnit
				.HOURS));
			NUnit.Framework.Assert.AreEqual(java.util.concurrent.TimeUnit.MINUTES.convert(1, 
				java.util.concurrent.TimeUnit.DAYS), conf.getTimeDuration("test.time.b", 1, java.util.concurrent.TimeUnit
				.MINUTES));
			// check default
			NUnit.Framework.Assert.AreEqual(30L, conf.getTimeDuration("test.time.X", 30, java.util.concurrent.TimeUnit
				.SECONDS));
			conf.set("test.time.X", "30");
			NUnit.Framework.Assert.AreEqual(30L, conf.getTimeDuration("test.time.X", 40, java.util.concurrent.TimeUnit
				.SECONDS));
			foreach (org.apache.hadoop.conf.Configuration.ParsedTimeDuration ptd in org.apache.hadoop.conf.Configuration.ParsedTimeDuration
				.values())
			{
				conf.setTimeDuration("test.time.unit", 1, ptd.unit());
				NUnit.Framework.Assert.AreEqual(1 + ptd.suffix(), conf.get("test.time.unit"));
				NUnit.Framework.Assert.AreEqual(1, conf.getTimeDuration("test.time.unit", 2, ptd.
					unit()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testPattern()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.pattern1", string.Empty);
			appendProperty("test.pattern2", "(");
			appendProperty("test.pattern3", "a+b");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			java.util.regex.Pattern defaultPattern = java.util.regex.Pattern.compile("x+");
			// Return default if missing
			NUnit.Framework.Assert.AreEqual(defaultPattern.pattern(), conf.getPattern("xxxxx"
				, defaultPattern).pattern());
			// Return null if empty and default is null
			NUnit.Framework.Assert.IsNull(conf.getPattern("test.pattern1", null));
			// Return default for empty
			NUnit.Framework.Assert.AreEqual(defaultPattern.pattern(), conf.getPattern("test.pattern1"
				, defaultPattern).pattern());
			// Return default for malformed
			NUnit.Framework.Assert.AreEqual(defaultPattern.pattern(), conf.getPattern("test.pattern2"
				, defaultPattern).pattern());
			// Works for correct patterns
			NUnit.Framework.Assert.AreEqual("a+b", conf.getPattern("test.pattern3", defaultPattern
				).pattern());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testPropertySource()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.foo", "bar");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			conf.set("fs.defaultFS", "value");
			string[] sources = conf.getPropertySources("test.foo");
			NUnit.Framework.Assert.AreEqual(1, sources.Length);
			NUnit.Framework.Assert.AreEqual("Resource string returned for a file-loaded property"
				 + " must be a proper absolute path", fileResource, new org.apache.hadoop.fs.Path
				(sources[0]));
			NUnit.Framework.Assert.assertArrayEquals("Resource string returned for a set() property must be "
				 + "\"programatically\"", new string[] { "programatically" }, conf.getPropertySources
				("fs.defaultFS"));
			NUnit.Framework.Assert.AreEqual("Resource string returned for an unset property must be null"
				, null, conf.getPropertySources("fs.defaultFoo"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testMultiplePropertySource()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.foo", "bar", false, "a", "b", "c");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			string[] sources = conf.getPropertySources("test.foo");
			NUnit.Framework.Assert.AreEqual(4, sources.Length);
			NUnit.Framework.Assert.AreEqual("a", sources[0]);
			NUnit.Framework.Assert.AreEqual("b", sources[1]);
			NUnit.Framework.Assert.AreEqual("c", sources[2]);
			NUnit.Framework.Assert.AreEqual("Resource string returned for a file-loaded property"
				 + " must be a proper absolute path", fileResource, new org.apache.hadoop.fs.Path
				(sources[3]));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSocketAddress()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string defaultAddr = "host:1";
			int defaultPort = 2;
			java.net.InetSocketAddress addr = null;
			addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
			NUnit.Framework.Assert.AreEqual(defaultAddr, org.apache.hadoop.net.NetUtils.getHostPortString
				(addr));
			conf.set("myAddress", "host2");
			addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
			NUnit.Framework.Assert.AreEqual("host2:" + defaultPort, org.apache.hadoop.net.NetUtils
				.getHostPortString(addr));
			conf.set("myAddress", "host2:3");
			addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
			NUnit.Framework.Assert.AreEqual("host2:3", org.apache.hadoop.net.NetUtils.getHostPortString
				(addr));
			conf.set("myAddress", " \n \t    host4:5     \t \n   ");
			addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
			NUnit.Framework.Assert.AreEqual("host4:5", org.apache.hadoop.net.NetUtils.getHostPortString
				(addr));
			bool threwException = false;
			conf.set("myAddress", "bad:-port");
			try
			{
				addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
			}
			catch (System.ArgumentException iae)
			{
				threwException = true;
				NUnit.Framework.Assert.AreEqual("Does not contain a valid host:port authority: " 
					+ "bad:-port (configuration property 'myAddress')", iae.Message);
			}
			finally
			{
				NUnit.Framework.Assert.IsTrue(threwException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSetSocketAddress()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.net.NetUtils.addStaticResolution("host", "127.0.0.1");
			string defaultAddr = "host:1";
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.createSocketAddr
				(defaultAddr);
			conf.setSocketAddr("myAddress", addr);
			NUnit.Framework.Assert.AreEqual(defaultAddr, org.apache.hadoop.net.NetUtils.getHostPortString
				(addr));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testUpdateSocketAddress()
		{
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.createSocketAddrForHost
				("host", 1);
			java.net.InetSocketAddress connectAddr = conf.updateConnectAddr("myAddress", addr
				);
			NUnit.Framework.Assert.AreEqual(connectAddr.getHostName(), addr.getHostName());
			addr = new java.net.InetSocketAddress(1);
			connectAddr = conf.updateConnectAddr("myAddress", addr);
			NUnit.Framework.Assert.AreEqual(connectAddr.getHostName(), java.net.InetAddress.getLocalHost
				().getHostName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testReload()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.key1", "final-value1", true);
			appendProperty("test.key2", "value2");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			appendProperty("test.key1", "value1");
			appendProperty("test.key3", "value3");
			endConfig();
			org.apache.hadoop.fs.Path fileResource1 = new org.apache.hadoop.fs.Path(CONFIG2);
			conf.addResource(fileResource1);
			// add a few values via set.
			conf.set("test.key3", "value4");
			conf.set("test.key4", "value5");
			NUnit.Framework.Assert.AreEqual("final-value1", conf.get("test.key1"));
			NUnit.Framework.Assert.AreEqual("value2", conf.get("test.key2"));
			NUnit.Framework.Assert.AreEqual("value4", conf.get("test.key3"));
			NUnit.Framework.Assert.AreEqual("value5", conf.get("test.key4"));
			// change values in the test file...
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.key1", "final-value1");
			appendProperty("test.key3", "final-value3", true);
			endConfig();
			conf.reloadConfiguration();
			NUnit.Framework.Assert.AreEqual("value1", conf.get("test.key1"));
			// overlayed property overrides.
			NUnit.Framework.Assert.AreEqual("value4", conf.get("test.key3"));
			NUnit.Framework.Assert.AreEqual(null, conf.get("test.key2"));
			NUnit.Framework.Assert.AreEqual("value5", conf.get("test.key4"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSize()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.set("a", "A");
			conf.set("b", "B");
			NUnit.Framework.Assert.AreEqual(2, conf.size());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testClear()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.set("a", "A");
			conf.set("b", "B");
			conf.clear();
			NUnit.Framework.Assert.AreEqual(0, conf.size());
			NUnit.Framework.Assert.IsFalse(conf.GetEnumerator().MoveNext());
		}

		public class Fake_ClassLoader : java.lang.ClassLoader
		{
		}

		public virtual void testClassLoader()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.setQuietMode(false);
			conf.setClassLoader(new org.apache.hadoop.conf.TestConfiguration.Fake_ClassLoader
				());
			org.apache.hadoop.conf.Configuration other = new org.apache.hadoop.conf.Configuration
				(conf);
			NUnit.Framework.Assert.IsTrue(other.getClassLoader() is org.apache.hadoop.conf.TestConfiguration.Fake_ClassLoader
				);
		}

		internal class JsonConfiguration
		{
			internal org.apache.hadoop.conf.TestConfiguration.JsonProperty[] properties;

			public virtual org.apache.hadoop.conf.TestConfiguration.JsonProperty[] getProperties
				()
			{
				return properties;
			}

			public virtual void setProperties(org.apache.hadoop.conf.TestConfiguration.JsonProperty
				[] properties)
			{
				this.properties = properties;
			}
		}

		internal class JsonProperty
		{
			internal string key;

			public virtual string getKey()
			{
				return key;
			}

			public virtual void setKey(string key)
			{
				this.key = key;
			}

			public virtual string getValue()
			{
				return value;
			}

			public virtual void setValue(string value)
			{
				this.value = value;
			}

			public virtual bool getIsFinal()
			{
				return isFinal;
			}

			public virtual void setIsFinal(bool isFinal)
			{
				this.isFinal = isFinal;
			}

			public virtual string getResource()
			{
				return resource;
			}

			public virtual void setResource(string resource)
			{
				this.resource = resource;
			}

			internal string value;

			internal bool isFinal;

			internal string resource;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetSetTrimmedNames()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.set(" name", "value");
			NUnit.Framework.Assert.AreEqual("value", conf.get("name"));
			NUnit.Framework.Assert.AreEqual("value", conf.get(" name"));
			NUnit.Framework.Assert.AreEqual("value", conf.getRaw("  name  "));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDumpConfiguration()
		{
			System.IO.StringWriter outWriter = new System.IO.StringWriter();
			org.apache.hadoop.conf.Configuration.dumpConfiguration(conf, outWriter);
			string jsonStr = outWriter.ToString();
			org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper
				();
			org.apache.hadoop.conf.TestConfiguration.JsonConfiguration jconf = mapper.readValue
				<org.apache.hadoop.conf.TestConfiguration.JsonConfiguration>(jsonStr);
			int defaultLength = jconf.getProperties().Length;
			// add 3 keys to the existing configuration properties
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.key1", "value1");
			appendProperty("test.key2", "value2", true);
			appendProperty("test.key3", "value3");
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			conf.addResource(fileResource);
			@out.close();
			outWriter = new System.IO.StringWriter();
			org.apache.hadoop.conf.Configuration.dumpConfiguration(conf, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new org.codehaus.jackson.map.ObjectMapper();
			jconf = mapper.readValue<org.apache.hadoop.conf.TestConfiguration.JsonConfiguration
				>(jsonStr);
			int length = jconf.getProperties().Length;
			// check for consistency in the number of properties parsed in Json format.
			NUnit.Framework.Assert.AreEqual(length, defaultLength + 3);
			//change few keys in another resource file
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG2));
			startConfig();
			appendProperty("test.key1", "newValue1");
			appendProperty("test.key2", "newValue2");
			endConfig();
			org.apache.hadoop.fs.Path fileResource1 = new org.apache.hadoop.fs.Path(CONFIG2);
			conf.addResource(fileResource1);
			@out.close();
			outWriter = new System.IO.StringWriter();
			org.apache.hadoop.conf.Configuration.dumpConfiguration(conf, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new org.codehaus.jackson.map.ObjectMapper();
			jconf = mapper.readValue<org.apache.hadoop.conf.TestConfiguration.JsonConfiguration
				>(jsonStr);
			// put the keys and their corresponding attributes into a hashmap for their 
			// efficient retrieval
			System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.TestConfiguration.JsonProperty
				> confDump = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.TestConfiguration.JsonProperty
				>();
			foreach (org.apache.hadoop.conf.TestConfiguration.JsonProperty prop in jconf.getProperties
				())
			{
				confDump[prop.getKey()] = prop;
			}
			// check if the value and resource of test.key1 is changed
			NUnit.Framework.Assert.AreEqual("newValue1", confDump["test.key1"].getValue());
			NUnit.Framework.Assert.AreEqual(false, confDump["test.key1"].getIsFinal());
			NUnit.Framework.Assert.AreEqual(fileResource1.ToString(), confDump["test.key1"].getResource
				());
			// check if final parameter test.key2 is not changed, since it is first 
			// loaded as final parameter
			NUnit.Framework.Assert.AreEqual("value2", confDump["test.key2"].getValue());
			NUnit.Framework.Assert.AreEqual(true, confDump["test.key2"].getIsFinal());
			NUnit.Framework.Assert.AreEqual(fileResource.ToString(), confDump["test.key2"].getResource
				());
			// check for other keys which are not modified later
			NUnit.Framework.Assert.AreEqual("value3", confDump["test.key3"].getValue());
			NUnit.Framework.Assert.AreEqual(false, confDump["test.key3"].getIsFinal());
			NUnit.Framework.Assert.AreEqual(fileResource.ToString(), confDump["test.key3"].getResource
				());
			// check for resource to be "Unknown" for keys which are loaded using 'set' 
			// and expansion of properties
			conf.set("test.key4", "value4");
			conf.set("test.key5", "value5");
			conf.set("test.key6", "${test.key5}");
			outWriter = new System.IO.StringWriter();
			org.apache.hadoop.conf.Configuration.dumpConfiguration(conf, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new org.codehaus.jackson.map.ObjectMapper();
			jconf = mapper.readValue<org.apache.hadoop.conf.TestConfiguration.JsonConfiguration
				>(jsonStr);
			confDump = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.TestConfiguration.JsonProperty
				>();
			foreach (org.apache.hadoop.conf.TestConfiguration.JsonProperty prop_1 in jconf.getProperties
				())
			{
				confDump[prop_1.getKey()] = prop_1;
			}
			NUnit.Framework.Assert.AreEqual("value5", confDump["test.key6"].getValue());
			NUnit.Framework.Assert.AreEqual("programatically", confDump["test.key4"].getResource
				());
			outWriter.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDumpConfiguratioWithoutDefaults()
		{
			// check for case when default resources are not loaded
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				(false);
			System.IO.StringWriter outWriter = new System.IO.StringWriter();
			org.apache.hadoop.conf.Configuration.dumpConfiguration(config, outWriter);
			string jsonStr = outWriter.ToString();
			org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper
				();
			org.apache.hadoop.conf.TestConfiguration.JsonConfiguration jconf = mapper.readValue
				<org.apache.hadoop.conf.TestConfiguration.JsonConfiguration>(jsonStr);
			//ensure that no properties are loaded.
			NUnit.Framework.Assert.AreEqual(0, jconf.getProperties().Length);
			// add 2 keys
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			appendProperty("test.key1", "value1");
			appendProperty("test.key2", "value2", true);
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			config.addResource(fileResource);
			@out.close();
			outWriter = new System.IO.StringWriter();
			org.apache.hadoop.conf.Configuration.dumpConfiguration(config, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new org.codehaus.jackson.map.ObjectMapper();
			jconf = mapper.readValue<org.apache.hadoop.conf.TestConfiguration.JsonConfiguration
				>(jsonStr);
			System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.TestConfiguration.JsonProperty
				> confDump = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.conf.TestConfiguration.JsonProperty
				>();
			foreach (org.apache.hadoop.conf.TestConfiguration.JsonProperty prop in jconf.getProperties
				())
			{
				confDump[prop.getKey()] = prop;
			}
			//ensure only 2 keys are loaded
			NUnit.Framework.Assert.AreEqual(2, jconf.getProperties().Length);
			//ensure the values are consistent
			NUnit.Framework.Assert.AreEqual(confDump["test.key1"].getValue(), "value1");
			NUnit.Framework.Assert.AreEqual(confDump["test.key2"].getValue(), "value2");
			//check the final tag
			NUnit.Framework.Assert.AreEqual(false, confDump["test.key1"].getIsFinal());
			NUnit.Framework.Assert.AreEqual(true, confDump["test.key2"].getIsFinal());
			//check the resource for each property
			foreach (org.apache.hadoop.conf.TestConfiguration.JsonProperty prop_1 in jconf.getProperties
				())
			{
				NUnit.Framework.Assert.AreEqual(fileResource.ToString(), prop_1.getResource());
			}
		}

		public virtual void testGetValByRegex()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string key1 = "t.abc.key1";
			string key2 = "t.abc.key2";
			string key3 = "tt.abc.key3";
			string key4 = "t.abc.ey3";
			conf.set(key1, "value1");
			conf.set(key2, "value2");
			conf.set(key3, "value3");
			conf.set(key4, "value3");
			System.Collections.Generic.IDictionary<string, string> res = conf.getValByRegex("^t\\..*\\.key\\d"
				);
			NUnit.Framework.Assert.IsTrue("Conf didn't get key " + key1, res.Contains(key1));
			NUnit.Framework.Assert.IsTrue("Conf didn't get key " + key2, res.Contains(key2));
			NUnit.Framework.Assert.IsTrue("Picked out wrong key " + key3, !res.Contains(key3)
				);
			NUnit.Framework.Assert.IsTrue("Picked out wrong key " + key4, !res.Contains(key4)
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSettingValueNull()
		{
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				();
			try
			{
				config.set("testClassName", null);
				fail("Should throw an IllegalArgumentException exception ");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e is System.ArgumentException);
				NUnit.Framework.Assert.AreEqual(e.Message, "The value of property testClassName must not be null"
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSettingKeyNull()
		{
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				();
			try
			{
				config.set(null, "test");
				fail("Should throw an IllegalArgumentException exception ");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e is System.ArgumentException);
				NUnit.Framework.Assert.AreEqual(e.Message, "Property name must not be null");
			}
		}

		public virtual void testInvalidSubstitutation()
		{
			org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration
				(false);
			// 2-var loops
			//
			string key = "test.random.key";
			foreach (string keyExpression in java.util.Arrays.asList("${" + key + "}", "foo${"
				 + key + "}", "foo${" + key + "}bar", "${" + key + "}bar"))
			{
				configuration.set(key, keyExpression);
				checkSubDepthException(configuration, key);
			}
			//
			// 3-variable loops
			//
			string expVal1 = "${test.var2}";
			string testVar1 = "test.var1";
			configuration.set(testVar1, expVal1);
			configuration.set("test.var2", "${test.var3}");
			configuration.set("test.var3", "${test.var1}");
			checkSubDepthException(configuration, testVar1);
			// 3-variable loop with non-empty value prefix/suffix
			//
			string expVal2 = "foo2${test.var2}bar2";
			configuration.set(testVar1, expVal2);
			configuration.set("test.var2", "foo3${test.var3}bar3");
			configuration.set("test.var3", "foo1${test.var1}bar1");
			checkSubDepthException(configuration, testVar1);
		}

		private static void checkSubDepthException(org.apache.hadoop.conf.Configuration configuration
			, string key)
		{
			try
			{
				configuration.get(key);
				fail("IllegalStateException depth too large not thrown");
			}
			catch (System.InvalidOperationException e)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected exception text: " + e, e.Message.contains
					("substitution depth"));
			}
		}

		public virtual void testIncompleteSubbing()
		{
			org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration
				(false);
			string key = "test.random.key";
			foreach (string keyExpression in java.util.Arrays.asList("{}", "${}", "{" + key, 
				"${" + key, "foo${" + key, "foo${" + key + "bar", "foo{" + key + "}bar", "${" + 
				key + "bar"))
			{
				configuration.set(key, keyExpression);
				string value = configuration.get(key);
				NUnit.Framework.Assert.IsTrue("Unexpected value " + value, value.Equals(keyExpression
					));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetClassByNameOrNull()
		{
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				();
			java.lang.Class clazz = config.getClassByNameOrNull("java.lang.Object");
			NUnit.Framework.Assert.IsNotNull(clazz);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetFinalParameters()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			declareProperty("my.var", "x", "x", true);
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			System.Collections.Generic.ICollection<string> finalParameters = conf.getFinalParameters
				();
			NUnit.Framework.Assert.IsFalse("my.var already exists", finalParameters.contains(
				"my.var"));
			conf.addResource(fileResource);
			NUnit.Framework.Assert.AreEqual("my.var is undefined", "x", conf.get("my.var"));
			NUnit.Framework.Assert.IsFalse("finalparams not copied", finalParameters.contains
				("my.var"));
			finalParameters = conf.getFinalParameters();
			NUnit.Framework.Assert.IsTrue("my.var is not final", finalParameters.contains("my.var"
				));
		}

		/// <summary>
		/// A test to check whether this thread goes into infinite loop because of
		/// destruction of data structure by resize of Map.
		/// </summary>
		/// <remarks>
		/// A test to check whether this thread goes into infinite loop because of
		/// destruction of data structure by resize of Map. This problem was reported
		/// by SPARK-2546.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testConcurrentAccesses()
		{
			@out = new java.io.BufferedWriter(new java.io.FileWriter(CONFIG));
			startConfig();
			declareProperty("some.config", "xyz", "xyz", false);
			endConfig();
			org.apache.hadoop.fs.Path fileResource = new org.apache.hadoop.fs.Path(CONFIG);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.addResource(fileResource);
			System.Collections.Generic.List<_T560292964> threads = new System.Collections.Generic.List
				<_T560292964>();
			for (int i = 0; i < 100; i++)
			{
				threads.add(new _T560292964(this, conf, Sharpen.Runtime.getStringValueOf(i)));
			}
			foreach (java.lang.Thread t in threads)
			{
				t.start();
			}
			foreach (java.lang.Thread t_1 in threads)
			{
				t_1.join();
			}
		}

		internal class _T560292964 : java.lang.Thread
		{
			private readonly org.apache.hadoop.conf.Configuration config;

			private readonly string prefix;

			public _T560292964(TestConfiguration _enclosing, org.apache.hadoop.conf.Configuration
				 conf, string prefix)
			{
				this._enclosing = _enclosing;
				this.config = conf;
				this.prefix = prefix;
			}

			public override void run()
			{
				for (int i = 0; i < 100000; i++)
				{
					this.config.set("some.config.value-" + this.prefix + i, "value");
				}
			}

			private readonly TestConfiguration _enclosing;
		}

		// If this test completes without going into infinite loop,
		// it's expected behaviour.
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			junit.textui.TestRunner.Main(new string[] { Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.conf.TestConfiguration)).getName() });
		}
	}
}
