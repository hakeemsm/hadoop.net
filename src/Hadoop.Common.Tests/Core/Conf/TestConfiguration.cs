using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Hadoop.Common.Core.IO;
using Junit.Textui;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;


namespace Org.Apache.Hadoop.Conf
{
	public class TestConfiguration : TestCase
	{
		private Configuration conf;

		internal static readonly string Config = new FilePath("./test-config-TestConfiguration.xml"
			).GetAbsolutePath();

		internal static readonly string Config2 = new FilePath("./test-config2-TestConfiguration.xml"
			).GetAbsolutePath();

		internal static readonly string ConfigForEnum = new FilePath("./test-config-enum-TestConfiguration.xml"
			).GetAbsolutePath();

		private static readonly string ConfigMultiByte = new FilePath("./test-config-multi-byte-TestConfiguration.xml"
			).GetAbsolutePath();

		private static readonly string ConfigMultiByteSaved = new FilePath("./test-config-multi-byte-saved-TestConfiguration.xml"
			).GetAbsolutePath();

		internal static readonly Random Ran = new Random();

		internal static readonly string Xmlheader = PlatformName.IbmJava ? "<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>"
			 : "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><configuration>";

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			conf = new Configuration();
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			base.TearDown();
			new FilePath(Config).Delete();
			new FilePath(Config2).Delete();
			new FilePath(ConfigForEnum).Delete();
			new FilePath(ConfigMultiByte).Delete();
			new FilePath(ConfigMultiByteSaved).Delete();
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
		private void AddInclude(string filename)
		{
			@out.Write("<xi:include href=\"" + filename + "\" xmlns:xi=\"http://www.w3.org/2001/XInclude\"  />\n "
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInputStreamResource()
		{
			StringWriter writer = new StringWriter();
			@out = new BufferedWriter(writer);
			StartConfig();
			DeclareProperty("prop", "A", "A");
			EndConfig();
			InputStream in1 = new ByteArrayInputStream(Runtime.GetBytesForString(writer
				.ToString()));
			Configuration conf = new Configuration(false);
			conf.AddResource(in1);
			Assert.Equal("A", conf.Get("prop"));
			InputStream in2 = new ByteArrayInputStream(Runtime.GetBytesForString(writer
				.ToString()));
			conf.AddResource(in2);
			Assert.Equal("A", conf.Get("prop"));
		}

		/// <summary>Tests use of multi-byte characters in property names and values.</summary>
		/// <remarks>
		/// Tests use of multi-byte characters in property names and values.  This test
		/// round-trips multi-byte string literals through saving and loading of config
		/// and asserts that the same values were read.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMultiByteCharacters()
		{
			string priorDefaultEncoding = Runtime.GetProperty("file.encoding");
			try
			{
				Runtime.SetProperty("file.encoding", "US-ASCII");
				string name = "multi_byte_\u611b_name";
				string value = "multi_byte_\u0641_value";
				@out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ConfigMultiByte
					), "UTF-8"));
				StartConfig();
				DeclareProperty(name, value, value);
				EndConfig();
				Configuration conf = new Configuration(false);
				conf.AddResource(new Path(ConfigMultiByte));
				Assert.Equal(value, conf.Get(name));
				FileOutputStream fos = new FileOutputStream(ConfigMultiByteSaved);
				try
				{
					conf.WriteXml(fos);
				}
				finally
				{
					IOUtils.CloseStream(fos);
				}
				conf = new Configuration(false);
				conf.AddResource(new Path(ConfigMultiByteSaved));
				Assert.Equal(value, conf.Get(name));
			}
			finally
			{
				Runtime.SetProperty("file.encoding", priorDefaultEncoding);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVariableSubstitution()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			DeclareProperty("my.int", "${intvar}", "42");
			DeclareProperty("intvar", "42", "42");
			DeclareProperty("my.base", "/tmp/${user.name}", Unspec);
			DeclareProperty("my.file", "hello", "hello");
			DeclareProperty("my.suffix", ".txt", ".txt");
			DeclareProperty("my.relfile", "${my.file}${my.suffix}", "hello.txt");
			DeclareProperty("my.fullfile", "${my.base}/${my.file}${my.suffix}", Unspec);
			// check that undefined variables are returned as-is
			DeclareProperty("my.failsexpand", "a${my.undefvar}b", "a${my.undefvar}b");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			foreach (TestConfiguration.Prop p in props)
			{
				System.Console.Out.WriteLine("p=" + p.name);
				string gotVal = conf.Get(p.name);
				string gotRawVal = conf.GetRaw(p.name);
				AssertEq(p.val, gotRawVal);
				if (p.expectEval == Unspec)
				{
					// expansion is system-dependent (uses System properties)
					// can't do exact match so just check that all variables got expanded
					Assert.True(gotVal != null && -1 == gotVal.IndexOf("${"));
				}
				else
				{
					AssertEq(p.expectEval, gotVal);
				}
			}
			// check that expansion also occurs for getInt()
			Assert.True(conf.GetInt("intvar", -1) == 42);
			Assert.True(conf.GetInt("my.int", -1) == 42);
			IDictionary<string, string> results = conf.GetValByRegex("^my.*file$");
			Assert.True(results.Keys.Contains("my.relfile"));
			Assert.True(results.Keys.Contains("my.fullfile"));
			Assert.True(results.Keys.Contains("my.file"));
			Assert.Equal(-1, results["my.relfile"].IndexOf("${"));
			Assert.Equal(-1, results["my.fullfile"].IndexOf("${"));
			Assert.Equal(-1, results["my.file"].IndexOf("${"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFinalParam()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			DeclareProperty("my.var", string.Empty, string.Empty, true);
			EndConfig();
			Path fileResource = new Path(Config);
			Configuration conf1 = new Configuration();
			conf1.AddResource(fileResource);
			NUnit.Framework.Assert.IsNull("my var is not null", conf1.Get("my.var"));
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			DeclareProperty("my.var", "myval", "myval", false);
			EndConfig();
			fileResource = new Path(Config2);
			Configuration conf2 = new Configuration(conf1);
			conf2.AddResource(fileResource);
			NUnit.Framework.Assert.IsNull("my var is not final", conf2.Get("my.var"));
		}

		public static void AssertEq(object a, object b)
		{
			System.Console.Out.WriteLine("assertEq: " + a + ", " + b);
			Assert.Equal(a, b);
		}

		internal class Prop
		{
			internal string name;

			internal string val;

			internal string expectEval;
		}

		internal readonly string Unspec = null;

		internal AList<TestConfiguration.Prop> props = new AList<TestConfiguration.Prop>(
			);

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DeclareProperty(string name, string val, string expectEval)
		{
			DeclareProperty(name, val, expectEval, false);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DeclareProperty(string name, string val, string expectEval, 
			bool isFinal)
		{
			AppendProperty(name, val, isFinal);
			TestConfiguration.Prop p = new TestConfiguration.Prop();
			p.name = name;
			p.val = val;
			p.expectEval = expectEval;
			props.AddItem(p);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AppendProperty(string name, string val)
		{
			AppendProperty(name, val, false);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AppendProperty(string name, string val, bool isFinal, params 
			string[] sources)
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
			foreach (string s in sources)
			{
				@out.Write("<source>");
				@out.Write(s);
				@out.Write("</source>");
			}
			@out.Write("</property>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOverlay()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("a", "b");
			AppendProperty("b", "c");
			AppendProperty("d", "e");
			AppendProperty("e", "f", true);
			EndConfig();
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			AppendProperty("a", "b");
			AppendProperty("b", "d");
			AppendProperty("e", "e");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			//set dynamically something
			conf.Set("c", "d");
			conf.Set("a", "d");
			Configuration clone = new Configuration(conf);
			clone.AddResource(new Path(Config2));
			Assert.Equal(clone.Get("a"), "d");
			Assert.Equal(clone.Get("b"), "d");
			Assert.Equal(clone.Get("c"), "d");
			Assert.Equal(clone.Get("d"), "e");
			Assert.Equal(clone.Get("e"), "f");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCommentsInValue()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("my.comment", "this <!--comment here--> contains a comment");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			//two spaces one after "this", one before "contains"
			Assert.Equal("this  contains a comment", conf.Get("my.comment"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTrim()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			string[] whitespaces = new string[] { string.Empty, " ", "\n", "\t" };
			string[] name = new string[100];
			for (int i = 0; i < name.Length; i++)
			{
				name[i] = "foo" + i;
				StringBuilder prefix = new StringBuilder();
				StringBuilder postfix = new StringBuilder();
				for (int j = 0; j < 3; j++)
				{
					prefix.Append(whitespaces[Ran.Next(whitespaces.Length)]);
					postfix.Append(whitespaces[Ran.Next(whitespaces.Length)]);
				}
				AppendProperty(prefix + name[i] + postfix, name[i] + ".value");
			}
			EndConfig();
			conf.AddResource(new Path(Config));
			foreach (string n in name)
			{
				Assert.Equal(n + ".value", conf.Get(n));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetLocalPath()
		{
			Configuration conf = new Configuration();
			string[] dirs = new string[] { "a", "b", "c" };
			for (int i = 0; i < dirs.Length; i++)
			{
				dirs[i] = new Path(Runtime.GetProperty("test.build.data"), dirs[i]).ToString();
			}
			conf.Set("dirs", StringUtils.Join(dirs, ","));
			for (int i_1 = 0; i_1 < 1000; i_1++)
			{
				string localPath = conf.GetLocalPath("dirs", "dir" + i_1).ToString();
				Assert.True("Path doesn't end in specified dir: " + localPath, 
					localPath.EndsWith("dir" + i_1));
				NUnit.Framework.Assert.IsFalse("Path has internal whitespace: " + localPath, localPath
					.Contains(" "));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetFile()
		{
			Configuration conf = new Configuration();
			string[] dirs = new string[] { "a", "b", "c" };
			for (int i = 0; i < dirs.Length; i++)
			{
				dirs[i] = new Path(Runtime.GetProperty("test.build.data"), dirs[i]).ToString();
			}
			conf.Set("dirs", StringUtils.Join(dirs, ","));
			for (int i_1 = 0; i_1 < 1000; i_1++)
			{
				string localPath = conf.GetFile("dirs", "dir" + i_1).ToString();
				Assert.True("Path doesn't end in specified dir: " + localPath, 
					localPath.EndsWith("dir" + i_1));
				NUnit.Framework.Assert.IsFalse("Path has internal whitespace: " + localPath, localPath
					.Contains(" "));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestToString()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			string expectedOutput = "Configuration: core-default.xml, core-site.xml, " + fileResource
				.ToString();
			Assert.Equal(expectedOutput, conf.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWriteXml()
		{
			Configuration conf = new Configuration();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			conf.WriteXml(baos);
			string result = baos.ToString();
			Assert.True("Result has proper header", result.StartsWith(Xmlheader
				));
			Assert.True("Result has proper footer", result.EndsWith("</configuration>"
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIncludes()
		{
			TearDown();
			System.Console.Out.WriteLine("XXX testIncludes");
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			AppendProperty("a", "b");
			AppendProperty("c", "d");
			EndConfig();
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AddInclude(Config2);
			AppendProperty("e", "f");
			AppendProperty("g", "h");
			EndConfig();
			// verify that the includes file contains all properties
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(conf.Get("a"), "b");
			Assert.Equal(conf.Get("c"), "d");
			Assert.Equal(conf.Get("e"), "f");
			Assert.Equal(conf.Get("g"), "h");
			TearDown();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRelativeIncludes()
		{
			TearDown();
			string relConfig = new FilePath("./tmp/test-config.xml").GetAbsolutePath();
			string relConfig2 = new FilePath("./tmp/test-config2.xml").GetAbsolutePath();
			new FilePath(new FilePath(relConfig).GetParent()).Mkdirs();
			@out = new BufferedWriter(new FileWriter(relConfig2));
			StartConfig();
			AppendProperty("a", "b");
			EndConfig();
			@out = new BufferedWriter(new FileWriter(relConfig));
			StartConfig();
			// Add the relative path instead of the absolute one.
			AddInclude(new FilePath(relConfig2).GetName());
			AppendProperty("c", "d");
			EndConfig();
			// verify that the includes file contains all properties
			Path fileResource = new Path(relConfig);
			conf.AddResource(fileResource);
			Assert.Equal(conf.Get("a"), "b");
			Assert.Equal(conf.Get("c"), "d");
			// Cleanup
			new FilePath(relConfig).Delete();
			new FilePath(relConfig2).Delete();
			new FilePath(new FilePath(relConfig).GetParent()).Delete();
		}

		internal BufferedWriter @out;

		public virtual void TestIntegerRanges()
		{
			Configuration conf = new Configuration();
			conf.Set("first", "-100");
			conf.Set("second", "4-6,9-10,27");
			conf.Set("third", "34-");
			Configuration.IntegerRanges range = conf.GetRange("first", null);
			System.Console.Out.WriteLine("first = " + range);
			Assert.Equal(true, range.IsIncluded(0));
			Assert.Equal(true, range.IsIncluded(1));
			Assert.Equal(true, range.IsIncluded(100));
			Assert.Equal(false, range.IsIncluded(101));
			range = conf.GetRange("second", null);
			System.Console.Out.WriteLine("second = " + range);
			Assert.Equal(false, range.IsIncluded(3));
			Assert.Equal(true, range.IsIncluded(4));
			Assert.Equal(true, range.IsIncluded(6));
			Assert.Equal(false, range.IsIncluded(7));
			Assert.Equal(false, range.IsIncluded(8));
			Assert.Equal(true, range.IsIncluded(9));
			Assert.Equal(true, range.IsIncluded(10));
			Assert.Equal(false, range.IsIncluded(11));
			Assert.Equal(false, range.IsIncluded(26));
			Assert.Equal(true, range.IsIncluded(27));
			Assert.Equal(false, range.IsIncluded(28));
			range = conf.GetRange("third", null);
			System.Console.Out.WriteLine("third = " + range);
			Assert.Equal(false, range.IsIncluded(33));
			Assert.Equal(true, range.IsIncluded(34));
			Assert.Equal(true, range.IsIncluded(100000000));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetRangeIterator()
		{
			Configuration config = new Configuration(false);
			Configuration.IntegerRanges ranges = config.GetRange("Test", string.Empty);
			NUnit.Framework.Assert.IsFalse("Empty range has values", ranges.GetEnumerator().HasNext
				());
			ranges = config.GetRange("Test", "5");
			ICollection<int> expected = new HashSet<int>(Arrays.AsList(5));
			ICollection<int> found = new HashSet<int>();
			foreach (int i in ranges)
			{
				found.AddItem(i);
			}
			Assert.Equal(expected, found);
			ranges = config.GetRange("Test", "5-10,13-14");
			expected = new HashSet<int>(Arrays.AsList(5, 6, 7, 8, 9, 10, 13, 14));
			found = new HashSet<int>();
			foreach (int i_1 in ranges)
			{
				found.AddItem(i_1);
			}
			Assert.Equal(expected, found);
			ranges = config.GetRange("Test", "8-12, 5- 7");
			expected = new HashSet<int>(Arrays.AsList(5, 6, 7, 8, 9, 10, 11, 12));
			found = new HashSet<int>();
			foreach (int i_2 in ranges)
			{
				found.AddItem(i_2);
			}
			Assert.Equal(expected, found);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHexValues()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.hex1", "0x10");
			AppendProperty("test.hex2", "0xF");
			AppendProperty("test.hex3", "-0x10");
			// Invalid?
			AppendProperty("test.hex4", "-0x10xyz");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(16, conf.GetInt("test.hex1", 0));
			Assert.Equal(16, conf.GetLong("test.hex1", 0));
			Assert.Equal(15, conf.GetInt("test.hex2", 0));
			Assert.Equal(15, conf.GetLong("test.hex2", 0));
			Assert.Equal(-16, conf.GetInt("test.hex3", 0));
			Assert.Equal(-16, conf.GetLong("test.hex3", 0));
			try
			{
				conf.GetLong("test.hex4", 0);
				Fail("Property had invalid long value, but was read successfully.");
			}
			catch (FormatException)
			{
			}
			// pass
			try
			{
				conf.GetInt("test.hex4", 0);
				Fail("Property had invalid int value, but was read successfully.");
			}
			catch (FormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIntegerValues()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.int1", "20");
			AppendProperty("test.int2", "020");
			AppendProperty("test.int3", "-20");
			AppendProperty("test.int4", " -20 ");
			AppendProperty("test.int5", " -20xyz ");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(20, conf.GetInt("test.int1", 0));
			Assert.Equal(20, conf.GetLong("test.int1", 0));
			Assert.Equal(20, conf.GetLongBytes("test.int1", 0));
			Assert.Equal(20, conf.GetInt("test.int2", 0));
			Assert.Equal(20, conf.GetLong("test.int2", 0));
			Assert.Equal(20, conf.GetLongBytes("test.int2", 0));
			Assert.Equal(-20, conf.GetInt("test.int3", 0));
			Assert.Equal(-20, conf.GetLong("test.int3", 0));
			Assert.Equal(-20, conf.GetLongBytes("test.int3", 0));
			Assert.Equal(-20, conf.GetInt("test.int4", 0));
			Assert.Equal(-20, conf.GetLong("test.int4", 0));
			Assert.Equal(-20, conf.GetLongBytes("test.int4", 0));
			try
			{
				conf.GetInt("test.int5", 0);
				Fail("Property had invalid int value, but was read successfully.");
			}
			catch (FormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHumanReadableValues()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.humanReadableValue1", "1m");
			AppendProperty("test.humanReadableValue2", "1M");
			AppendProperty("test.humanReadableValue5", "1MBCDE");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(1048576, conf.GetLongBytes("test.humanReadableValue1"
				, 0));
			Assert.Equal(1048576, conf.GetLongBytes("test.humanReadableValue2"
				, 0));
			try
			{
				conf.GetLongBytes("test.humanReadableValue5", 0);
				Fail("Property had invalid human readable value, but was read successfully.");
			}
			catch (FormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBooleanValues()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.bool1", "true");
			AppendProperty("test.bool2", "false");
			AppendProperty("test.bool3", "  true ");
			AppendProperty("test.bool4", " false ");
			AppendProperty("test.bool5", "foo");
			AppendProperty("test.bool6", "TRUE");
			AppendProperty("test.bool7", "FALSE");
			AppendProperty("test.bool8", string.Empty);
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(true, conf.GetBoolean("test.bool1", false));
			Assert.Equal(false, conf.GetBoolean("test.bool2", true));
			Assert.Equal(true, conf.GetBoolean("test.bool3", false));
			Assert.Equal(false, conf.GetBoolean("test.bool4", true));
			Assert.Equal(true, conf.GetBoolean("test.bool5", true));
			Assert.Equal(true, conf.GetBoolean("test.bool6", false));
			Assert.Equal(false, conf.GetBoolean("test.bool7", true));
			Assert.Equal(false, conf.GetBoolean("test.bool8", false));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFloatValues()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.float1", "3.1415");
			AppendProperty("test.float2", "003.1415");
			AppendProperty("test.float3", "-3.1415");
			AppendProperty("test.float4", " -3.1415 ");
			AppendProperty("test.float5", "xyz-3.1415xyz");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(3.1415f, conf.GetFloat("test.float1", 0.0f));
			Assert.Equal(3.1415f, conf.GetFloat("test.float2", 0.0f));
			Assert.Equal(-3.1415f, conf.GetFloat("test.float3", 0.0f));
			Assert.Equal(-3.1415f, conf.GetFloat("test.float4", 0.0f));
			try
			{
				conf.GetFloat("test.float5", 0.0f);
				Fail("Property had invalid float value, but was read successfully.");
			}
			catch (FormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDoubleValues()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.double1", "3.1415");
			AppendProperty("test.double2", "003.1415");
			AppendProperty("test.double3", "-3.1415");
			AppendProperty("test.double4", " -3.1415 ");
			AppendProperty("test.double5", "xyz-3.1415xyz");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal(3.1415, conf.GetDouble("test.double1", 0.0));
			Assert.Equal(3.1415, conf.GetDouble("test.double2", 0.0));
			Assert.Equal(-3.1415, conf.GetDouble("test.double3", 0.0));
			Assert.Equal(-3.1415, conf.GetDouble("test.double4", 0.0));
			try
			{
				conf.GetDouble("test.double5", 0.0);
				Fail("Property had invalid double value, but was read successfully.");
			}
			catch (FormatException)
			{
			}
		}

		// pass
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetClass()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.class1", "java.lang.Integer");
			AppendProperty("test.class2", " java.lang.Integer ");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Assert.Equal("java.lang.Integer", conf.GetClass("test.class1", 
				null).GetCanonicalName());
			Assert.Equal("java.lang.Integer", conf.GetClass("test.class2", 
				null).GetCanonicalName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetClasses()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.classes1", "java.lang.Integer,java.lang.String");
			AppendProperty("test.classes2", " java.lang.Integer , java.lang.String ");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			string[] expectedNames = new string[] { "java.lang.Integer", "java.lang.String" };
			Type[] defaultClasses = new Type[] {  };
			Type[] classes1 = conf.GetClasses("test.classes1", defaultClasses);
			Type[] classes2 = conf.GetClasses("test.classes2", defaultClasses);
			Assert.AssertArrayEquals(expectedNames, ExtractClassNames(classes1));
			Assert.AssertArrayEquals(expectedNames, ExtractClassNames(classes2));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetStringCollection()
		{
			Configuration c = new Configuration();
			c.Set("x", " a, b\n,\nc ");
			ICollection<string> strs = c.GetTrimmedStringCollection("x");
			Assert.Equal(3, strs.Count);
			Assert.AssertArrayEquals(new string[] { "a", "b", "c" }, Collections.ToArray
				(strs, new string[0]));
			// Check that the result is mutable
			strs.AddItem("z");
			// Make sure same is true for missing config
			strs = c.GetStringCollection("does-not-exist");
			Assert.Equal(0, strs.Count);
			strs.AddItem("z");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetTrimmedStringCollection()
		{
			Configuration c = new Configuration();
			c.Set("x", "a, b, c");
			ICollection<string> strs = c.GetStringCollection("x");
			Assert.Equal(3, strs.Count);
			Assert.AssertArrayEquals(new string[] { "a", " b", " c" }, Collections.ToArray
				(strs, new string[0]));
			// Check that the result is mutable
			strs.AddItem("z");
			// Make sure same is true for missing config
			strs = c.GetStringCollection("does-not-exist");
			Assert.Equal(0, strs.Count);
			strs.AddItem("z");
		}

		private static string[] ExtractClassNames(Type[] classes)
		{
			string[] classNames = new string[classes.Length];
			for (int i = 0; i < classNames.Length; i++)
			{
				classNames[i] = classes[i].GetCanonicalName();
			}
			return classNames;
		}

		internal enum Dingo
		{
			Foo,
			Bar
		}

		internal enum Yak
		{
			Rab,
			Foo
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEnum()
		{
			Configuration conf = new Configuration();
			conf.SetEnum("test.enum", TestConfiguration.Dingo.Foo);
			NUnit.Framework.Assert.AreSame(TestConfiguration.Dingo.Foo, conf.GetEnum("test.enum"
				, TestConfiguration.Dingo.Bar));
			NUnit.Framework.Assert.AreSame(TestConfiguration.Yak.Foo, conf.GetEnum("test.enum"
				, TestConfiguration.Yak.Rab));
			conf.SetEnum("test.enum", TestConfiguration.Dingo.Foo);
			bool fail = false;
			try
			{
				conf.SetEnum("test.enum", TestConfiguration.Dingo.Bar);
				TestConfiguration.Yak y = conf.GetEnum("test.enum", TestConfiguration.Yak.Foo);
			}
			catch (ArgumentException)
			{
				fail = true;
			}
			Assert.True(fail);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEnumFromXml()
		{
			@out = new BufferedWriter(new FileWriter(ConfigForEnum));
			StartConfig();
			AppendProperty("test.enum", " \t \n   FOO \t \n");
			AppendProperty("test.enum2", " \t \n   Yak.FOO \t \n");
			EndConfig();
			Configuration conf = new Configuration();
			Path fileResource = new Path(ConfigForEnum);
			conf.AddResource(fileResource);
			NUnit.Framework.Assert.AreSame(TestConfiguration.Yak.Foo, conf.GetEnum("test.enum"
				, TestConfiguration.Yak.Foo));
			bool fail = false;
			try
			{
				conf.GetEnum("test.enum2", TestConfiguration.Yak.Foo);
			}
			catch (ArgumentException)
			{
				fail = true;
			}
			Assert.True(fail);
		}

		public virtual void TestTimeDuration()
		{
			Configuration conf = new Configuration(false);
			conf.SetTimeDuration("test.time.a", 7L, TimeUnit.Seconds);
			Assert.Equal("7s", conf.Get("test.time.a"));
			Assert.Equal(0L, conf.GetTimeDuration("test.time.a", 30, TimeUnit
				.Minutes));
			Assert.Equal(7L, conf.GetTimeDuration("test.time.a", 30, TimeUnit
				.Seconds));
			Assert.Equal(7000L, conf.GetTimeDuration("test.time.a", 30, TimeUnit
				.Milliseconds));
			Assert.Equal(7000000L, conf.GetTimeDuration("test.time.a", 30, 
				TimeUnit.Microseconds));
			Assert.Equal(7000000000L, conf.GetTimeDuration("test.time.a", 
				30, TimeUnit.Nanoseconds));
			conf.SetTimeDuration("test.time.b", 1, TimeUnit.Days);
			Assert.Equal("1d", conf.Get("test.time.b"));
			Assert.Equal(1, conf.GetTimeDuration("test.time.b", 1, TimeUnit
				.Days));
			Assert.Equal(24, conf.GetTimeDuration("test.time.b", 1, TimeUnit
				.Hours));
			Assert.Equal(TimeUnit.Minutes.Convert(1, TimeUnit.Days), conf.
				GetTimeDuration("test.time.b", 1, TimeUnit.Minutes));
			// check default
			Assert.Equal(30L, conf.GetTimeDuration("test.time.X", 30, TimeUnit
				.Seconds));
			conf.Set("test.time.X", "30");
			Assert.Equal(30L, conf.GetTimeDuration("test.time.X", 40, TimeUnit
				.Seconds));
			foreach (Configuration.ParsedTimeDuration ptd in Configuration.ParsedTimeDuration
				.Values())
			{
				conf.SetTimeDuration("test.time.unit", 1, ptd.Unit());
				Assert.Equal(1 + ptd.Suffix(), conf.Get("test.time.unit"));
				Assert.Equal(1, conf.GetTimeDuration("test.time.unit", 2, ptd.
					Unit()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPattern()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.pattern1", string.Empty);
			AppendProperty("test.pattern2", "(");
			AppendProperty("test.pattern3", "a+b");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			Pattern defaultPattern = Pattern.Compile("x+");
			// Return default if missing
			Assert.Equal(defaultPattern.Pattern(), conf.GetPattern("xxxxx"
				, defaultPattern).Pattern());
			// Return null if empty and default is null
			NUnit.Framework.Assert.IsNull(conf.GetPattern("test.pattern1", null));
			// Return default for empty
			Assert.Equal(defaultPattern.Pattern(), conf.GetPattern("test.pattern1"
				, defaultPattern).Pattern());
			// Return default for malformed
			Assert.Equal(defaultPattern.Pattern(), conf.GetPattern("test.pattern2"
				, defaultPattern).Pattern());
			// Works for correct patterns
			Assert.Equal("a+b", conf.GetPattern("test.pattern3", defaultPattern
				).Pattern());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPropertySource()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.foo", "bar");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			conf.Set("fs.defaultFS", "value");
			string[] sources = conf.GetPropertySources("test.foo");
			Assert.Equal(1, sources.Length);
			Assert.Equal("Resource string returned for a file-loaded property"
				 + " must be a proper absolute path", fileResource, new Path(sources[0]));
			Assert.AssertArrayEquals("Resource string returned for a set() property must be "
				 + "\"programatically\"", new string[] { "programatically" }, conf.GetPropertySources
				("fs.defaultFS"));
			Assert.Equal("Resource string returned for an unset property must be null"
				, null, conf.GetPropertySources("fs.defaultFoo"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMultiplePropertySource()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.foo", "bar", false, "a", "b", "c");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			string[] sources = conf.GetPropertySources("test.foo");
			Assert.Equal(4, sources.Length);
			Assert.Equal("a", sources[0]);
			Assert.Equal("b", sources[1]);
			Assert.Equal("c", sources[2]);
			Assert.Equal("Resource string returned for a file-loaded property"
				 + " must be a proper absolute path", fileResource, new Path(sources[3]));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSocketAddress()
		{
			Configuration conf = new Configuration();
			string defaultAddr = "host:1";
			int defaultPort = 2;
			IPEndPoint addr = null;
			addr = conf.GetSocketAddr("myAddress", defaultAddr, defaultPort);
			Assert.Equal(defaultAddr, NetUtils.GetHostPortString(addr));
			conf.Set("myAddress", "host2");
			addr = conf.GetSocketAddr("myAddress", defaultAddr, defaultPort);
			Assert.Equal("host2:" + defaultPort, NetUtils.GetHostPortString
				(addr));
			conf.Set("myAddress", "host2:3");
			addr = conf.GetSocketAddr("myAddress", defaultAddr, defaultPort);
			Assert.Equal("host2:3", NetUtils.GetHostPortString(addr));
			conf.Set("myAddress", " \n \t    host4:5     \t \n   ");
			addr = conf.GetSocketAddr("myAddress", defaultAddr, defaultPort);
			Assert.Equal("host4:5", NetUtils.GetHostPortString(addr));
			bool threwException = false;
			conf.Set("myAddress", "bad:-port");
			try
			{
				addr = conf.GetSocketAddr("myAddress", defaultAddr, defaultPort);
			}
			catch (ArgumentException iae)
			{
				threwException = true;
				Assert.Equal("Does not contain a valid host:port authority: " 
					+ "bad:-port (configuration property 'myAddress')", iae.Message);
			}
			finally
			{
				Assert.True(threwException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetSocketAddress()
		{
			Configuration conf = new Configuration();
			NetUtils.AddStaticResolution("host", "127.0.0.1");
			string defaultAddr = "host:1";
			IPEndPoint addr = NetUtils.CreateSocketAddr(defaultAddr);
			conf.SetSocketAddr("myAddress", addr);
			Assert.Equal(defaultAddr, NetUtils.GetHostPortString(addr));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUpdateSocketAddress()
		{
			IPEndPoint addr = NetUtils.CreateSocketAddrForHost("host", 1);
			IPEndPoint connectAddr = conf.UpdateConnectAddr("myAddress", addr);
			Assert.Equal(connectAddr.GetHostName(), addr.GetHostName());
			addr = new IPEndPoint(1);
			connectAddr = conf.UpdateConnectAddr("myAddress", addr);
			Assert.Equal(connectAddr.GetHostName(), Runtime.GetLocalHost
				().GetHostName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReload()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.key1", "final-value1", true);
			AppendProperty("test.key2", "value2");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			AppendProperty("test.key1", "value1");
			AppendProperty("test.key3", "value3");
			EndConfig();
			Path fileResource1 = new Path(Config2);
			conf.AddResource(fileResource1);
			// add a few values via set.
			conf.Set("test.key3", "value4");
			conf.Set("test.key4", "value5");
			Assert.Equal("final-value1", conf.Get("test.key1"));
			Assert.Equal("value2", conf.Get("test.key2"));
			Assert.Equal("value4", conf.Get("test.key3"));
			Assert.Equal("value5", conf.Get("test.key4"));
			// change values in the test file...
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.key1", "final-value1");
			AppendProperty("test.key3", "final-value3", true);
			EndConfig();
			conf.ReloadConfiguration();
			Assert.Equal("value1", conf.Get("test.key1"));
			// overlayed property overrides.
			Assert.Equal("value4", conf.Get("test.key3"));
			Assert.Equal(null, conf.Get("test.key2"));
			Assert.Equal("value5", conf.Get("test.key4"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSize()
		{
			Configuration conf = new Configuration(false);
			conf.Set("a", "A");
			conf.Set("b", "B");
			Assert.Equal(2, conf.Size());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestClear()
		{
			Configuration conf = new Configuration(false);
			conf.Set("a", "A");
			conf.Set("b", "B");
			conf.Clear();
			Assert.Equal(0, conf.Size());
			NUnit.Framework.Assert.IsFalse(conf.GetEnumerator().HasNext());
		}

		public class Fake_ClassLoader : ClassLoader
		{
		}

		public virtual void TestClassLoader()
		{
			Configuration conf = new Configuration(false);
			conf.SetQuietMode(false);
			conf.SetClassLoader(new TestConfiguration.Fake_ClassLoader());
			Configuration other = new Configuration(conf);
			Assert.True(other.GetClassLoader() is TestConfiguration.Fake_ClassLoader
				);
		}

		internal class JsonConfiguration
		{
			internal TestConfiguration.JsonProperty[] properties;

			public virtual TestConfiguration.JsonProperty[] GetProperties()
			{
				return properties;
			}

			public virtual void SetProperties(TestConfiguration.JsonProperty[] properties)
			{
				this.properties = properties;
			}
		}

		internal class JsonProperty
		{
			internal string key;

			public virtual string GetKey()
			{
				return key;
			}

			public virtual void SetKey(string key)
			{
				this.key = key;
			}

			public virtual string GetValue()
			{
				return value;
			}

			public virtual void SetValue(string value)
			{
				this.value = value;
			}

			public virtual bool GetIsFinal()
			{
				return isFinal;
			}

			public virtual void SetIsFinal(bool isFinal)
			{
				this.isFinal = isFinal;
			}

			public virtual string GetResource()
			{
				return resource;
			}

			public virtual void SetResource(string resource)
			{
				this.resource = resource;
			}

			internal string value;

			internal bool isFinal;

			internal string resource;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetSetTrimmedNames()
		{
			Configuration conf = new Configuration(false);
			conf.Set(" name", "value");
			Assert.Equal("value", conf.Get("name"));
			Assert.Equal("value", conf.Get(" name"));
			Assert.Equal("value", conf.GetRaw("  name  "));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDumpConfiguration()
		{
			StringWriter outWriter = new StringWriter();
			Configuration.DumpConfiguration(conf, outWriter);
			string jsonStr = outWriter.ToString();
			ObjectMapper mapper = new ObjectMapper();
			TestConfiguration.JsonConfiguration jconf = mapper.ReadValue<TestConfiguration.JsonConfiguration
				>(jsonStr);
			int defaultLength = jconf.GetProperties().Length;
			// add 3 keys to the existing configuration properties
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.key1", "value1");
			AppendProperty("test.key2", "value2", true);
			AppendProperty("test.key3", "value3");
			EndConfig();
			Path fileResource = new Path(Config);
			conf.AddResource(fileResource);
			@out.Close();
			outWriter = new StringWriter();
			Configuration.DumpConfiguration(conf, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new ObjectMapper();
			jconf = mapper.ReadValue<TestConfiguration.JsonConfiguration>(jsonStr);
			int length = jconf.GetProperties().Length;
			// check for consistency in the number of properties parsed in Json format.
			Assert.Equal(length, defaultLength + 3);
			//change few keys in another resource file
			@out = new BufferedWriter(new FileWriter(Config2));
			StartConfig();
			AppendProperty("test.key1", "newValue1");
			AppendProperty("test.key2", "newValue2");
			EndConfig();
			Path fileResource1 = new Path(Config2);
			conf.AddResource(fileResource1);
			@out.Close();
			outWriter = new StringWriter();
			Configuration.DumpConfiguration(conf, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new ObjectMapper();
			jconf = mapper.ReadValue<TestConfiguration.JsonConfiguration>(jsonStr);
			// put the keys and their corresponding attributes into a hashmap for their 
			// efficient retrieval
			Dictionary<string, TestConfiguration.JsonProperty> confDump = new Dictionary<string
				, TestConfiguration.JsonProperty>();
			foreach (TestConfiguration.JsonProperty prop in jconf.GetProperties())
			{
				confDump[prop.GetKey()] = prop;
			}
			// check if the value and resource of test.key1 is changed
			Assert.Equal("newValue1", confDump["test.key1"].GetValue());
			Assert.Equal(false, confDump["test.key1"].GetIsFinal());
			Assert.Equal(fileResource1.ToString(), confDump["test.key1"].GetResource
				());
			// check if final parameter test.key2 is not changed, since it is first 
			// loaded as final parameter
			Assert.Equal("value2", confDump["test.key2"].GetValue());
			Assert.Equal(true, confDump["test.key2"].GetIsFinal());
			Assert.Equal(fileResource.ToString(), confDump["test.key2"].GetResource
				());
			// check for other keys which are not modified later
			Assert.Equal("value3", confDump["test.key3"].GetValue());
			Assert.Equal(false, confDump["test.key3"].GetIsFinal());
			Assert.Equal(fileResource.ToString(), confDump["test.key3"].GetResource
				());
			// check for resource to be "Unknown" for keys which are loaded using 'set' 
			// and expansion of properties
			conf.Set("test.key4", "value4");
			conf.Set("test.key5", "value5");
			conf.Set("test.key6", "${test.key5}");
			outWriter = new StringWriter();
			Configuration.DumpConfiguration(conf, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new ObjectMapper();
			jconf = mapper.ReadValue<TestConfiguration.JsonConfiguration>(jsonStr);
			confDump = new Dictionary<string, TestConfiguration.JsonProperty>();
			foreach (TestConfiguration.JsonProperty prop_1 in jconf.GetProperties())
			{
				confDump[prop_1.GetKey()] = prop_1;
			}
			Assert.Equal("value5", confDump["test.key6"].GetValue());
			Assert.Equal("programatically", confDump["test.key4"].GetResource
				());
			outWriter.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDumpConfiguratioWithoutDefaults()
		{
			// check for case when default resources are not loaded
			Configuration config = new Configuration(false);
			StringWriter outWriter = new StringWriter();
			Configuration.DumpConfiguration(config, outWriter);
			string jsonStr = outWriter.ToString();
			ObjectMapper mapper = new ObjectMapper();
			TestConfiguration.JsonConfiguration jconf = mapper.ReadValue<TestConfiguration.JsonConfiguration
				>(jsonStr);
			//ensure that no properties are loaded.
			Assert.Equal(0, jconf.GetProperties().Length);
			// add 2 keys
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			AppendProperty("test.key1", "value1");
			AppendProperty("test.key2", "value2", true);
			EndConfig();
			Path fileResource = new Path(Config);
			config.AddResource(fileResource);
			@out.Close();
			outWriter = new StringWriter();
			Configuration.DumpConfiguration(config, outWriter);
			jsonStr = outWriter.ToString();
			mapper = new ObjectMapper();
			jconf = mapper.ReadValue<TestConfiguration.JsonConfiguration>(jsonStr);
			Dictionary<string, TestConfiguration.JsonProperty> confDump = new Dictionary<string
				, TestConfiguration.JsonProperty>();
			foreach (TestConfiguration.JsonProperty prop in jconf.GetProperties())
			{
				confDump[prop.GetKey()] = prop;
			}
			//ensure only 2 keys are loaded
			Assert.Equal(2, jconf.GetProperties().Length);
			//ensure the values are consistent
			Assert.Equal(confDump["test.key1"].GetValue(), "value1");
			Assert.Equal(confDump["test.key2"].GetValue(), "value2");
			//check the final tag
			Assert.Equal(false, confDump["test.key1"].GetIsFinal());
			Assert.Equal(true, confDump["test.key2"].GetIsFinal());
			//check the resource for each property
			foreach (TestConfiguration.JsonProperty prop_1 in jconf.GetProperties())
			{
				Assert.Equal(fileResource.ToString(), prop_1.GetResource());
			}
		}

		public virtual void TestGetValByRegex()
		{
			Configuration conf = new Configuration();
			string key1 = "t.abc.key1";
			string key2 = "t.abc.key2";
			string key3 = "tt.abc.key3";
			string key4 = "t.abc.ey3";
			conf.Set(key1, "value1");
			conf.Set(key2, "value2");
			conf.Set(key3, "value3");
			conf.Set(key4, "value3");
			IDictionary<string, string> res = conf.GetValByRegex("^t\\..*\\.key\\d");
			Assert.True("Conf didn't get key " + key1, res.Contains(key1));
			Assert.True("Conf didn't get key " + key2, res.Contains(key2));
			Assert.True("Picked out wrong key " + key3, !res.Contains(key3)
				);
			Assert.True("Picked out wrong key " + key4, !res.Contains(key4)
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSettingValueNull()
		{
			Configuration config = new Configuration();
			try
			{
				config.Set("testClassName", null);
				Fail("Should throw an IllegalArgumentException exception ");
			}
			catch (Exception e)
			{
				Assert.True(e is ArgumentException);
				Assert.Equal(e.Message, "The value of property testClassName must not be null"
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSettingKeyNull()
		{
			Configuration config = new Configuration();
			try
			{
				config.Set(null, "test");
				Fail("Should throw an IllegalArgumentException exception ");
			}
			catch (Exception e)
			{
				Assert.True(e is ArgumentException);
				Assert.Equal(e.Message, "Property name must not be null");
			}
		}

		public virtual void TestInvalidSubstitutation()
		{
			Configuration configuration = new Configuration(false);
			// 2-var loops
			//
			string key = "test.random.key";
			foreach (string keyExpression in Arrays.AsList("${" + key + "}", "foo${" + key + 
				"}", "foo${" + key + "}bar", "${" + key + "}bar"))
			{
				configuration.Set(key, keyExpression);
				CheckSubDepthException(configuration, key);
			}
			//
			// 3-variable loops
			//
			string expVal1 = "${test.var2}";
			string testVar1 = "test.var1";
			configuration.Set(testVar1, expVal1);
			configuration.Set("test.var2", "${test.var3}");
			configuration.Set("test.var3", "${test.var1}");
			CheckSubDepthException(configuration, testVar1);
			// 3-variable loop with non-empty value prefix/suffix
			//
			string expVal2 = "foo2${test.var2}bar2";
			configuration.Set(testVar1, expVal2);
			configuration.Set("test.var2", "foo3${test.var3}bar3");
			configuration.Set("test.var3", "foo1${test.var1}bar1");
			CheckSubDepthException(configuration, testVar1);
		}

		private static void CheckSubDepthException(Configuration configuration, string key
			)
		{
			try
			{
				configuration.Get(key);
				Fail("IllegalStateException depth too large not thrown");
			}
			catch (InvalidOperationException e)
			{
				Assert.True("Unexpected exception text: " + e, e.Message.Contains
					("substitution depth"));
			}
		}

		public virtual void TestIncompleteSubbing()
		{
			Configuration configuration = new Configuration(false);
			string key = "test.random.key";
			foreach (string keyExpression in Arrays.AsList("{}", "${}", "{" + key, "${" + key
				, "foo${" + key, "foo${" + key + "bar", "foo{" + key + "}bar", "${" + key + "bar"
				))
			{
				configuration.Set(key, keyExpression);
				string value = configuration.Get(key);
				Assert.True("Unexpected value " + value, value.Equals(keyExpression
					));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetClassByNameOrNull()
		{
			Configuration config = new Configuration();
			Type clazz = config.GetClassByNameOrNull("java.lang.Object");
			NUnit.Framework.Assert.IsNotNull(clazz);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetFinalParameters()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			DeclareProperty("my.var", "x", "x", true);
			EndConfig();
			Path fileResource = new Path(Config);
			Configuration conf = new Configuration();
			ICollection<string> finalParameters = conf.GetFinalParameters();
			NUnit.Framework.Assert.IsFalse("my.var already exists", finalParameters.Contains(
				"my.var"));
			conf.AddResource(fileResource);
			Assert.Equal("my.var is undefined", "x", conf.Get("my.var"));
			NUnit.Framework.Assert.IsFalse("finalparams not copied", finalParameters.Contains
				("my.var"));
			finalParameters = conf.GetFinalParameters();
			Assert.True("my.var is not final", finalParameters.Contains("my.var"
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
		public virtual void TestConcurrentAccesses()
		{
			@out = new BufferedWriter(new FileWriter(Config));
			StartConfig();
			DeclareProperty("some.config", "xyz", "xyz", false);
			EndConfig();
			Path fileResource = new Path(Config);
			Configuration conf = new Configuration();
			conf.AddResource(fileResource);
			AList<_T560292964> threads = new AList<_T560292964>();
			for (int i = 0; i < 100; i++)
			{
				threads.AddItem(new _T560292964(this, conf, i.ToString()));
			}
			foreach (Thread t in threads)
			{
				t.Start();
			}
			foreach (Thread t_1 in threads)
			{
				t_1.Join();
			}
		}

		internal class _T560292964 : Thread
		{
			private readonly Configuration config;

			private readonly string prefix;

			public _T560292964(TestConfiguration _enclosing, Configuration conf, string prefix
				)
			{
				this._enclosing = _enclosing;
				this.config = conf;
				this.prefix = prefix;
			}

			public override void Run()
			{
				for (int i = 0; i < 100000; i++)
				{
					this.config.Set("some.config.value-" + this.prefix + i, "value");
				}
			}

			private readonly TestConfiguration _enclosing;
		}

		// If this test completes without going into infinite loop,
		// it's expected behaviour.
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			TestRunner.Main(new string[] { typeof(TestConfiguration).FullName });
		}
	}
}
