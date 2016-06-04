using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for WritableName.</summary>
	public class TestWritableName : TestCase
	{
		public TestWritableName(string name)
			: base(name)
		{
		}

		/// <summary>Example class used in test cases below.</summary>
		public class SimpleWritable : Writable
		{
			private static readonly Random Random = new Random();

			internal int state = Random.Next();

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteInt(state);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader @in)
			{
				this.state = @in.ReadInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public static TestWritableName.SimpleWritable Read(BinaryReader @in)
			{
				TestWritableName.SimpleWritable result = new TestWritableName.SimpleWritable();
				result.ReadFields(@in);
				return result;
			}

			/// <summary>Required by test code, below.</summary>
			public override bool Equals(object o)
			{
				if (!(o is TestWritableName.SimpleWritable))
				{
					return false;
				}
				TestWritableName.SimpleWritable other = (TestWritableName.SimpleWritable)o;
				return this.state == other.state;
			}
		}

		private const string testName = "mystring";

		/// <exception cref="System.Exception"/>
		public virtual void TestGoodName()
		{
			Configuration conf = new Configuration();
			Type test = WritableName.GetClass("long", conf);
			NUnit.Framework.Assert.IsTrue(test != null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetName()
		{
			Configuration conf = new Configuration();
			WritableName.SetName(typeof(TestWritableName.SimpleWritable), testName);
			Type test = WritableName.GetClass(testName, conf);
			NUnit.Framework.Assert.IsTrue(test.Equals(typeof(TestWritableName.SimpleWritable)
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddName()
		{
			Configuration conf = new Configuration();
			string altName = testName + ".alt";
			WritableName.SetName(typeof(TestWritableName.SimpleWritable), testName);
			WritableName.AddName(typeof(TestWritableName.SimpleWritable), altName);
			Type test = WritableName.GetClass(altName, conf);
			NUnit.Framework.Assert.IsTrue(test.Equals(typeof(TestWritableName.SimpleWritable)
				));
			// check original name still works
			test = WritableName.GetClass(testName, conf);
			NUnit.Framework.Assert.IsTrue(test.Equals(typeof(TestWritableName.SimpleWritable)
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBadName()
		{
			Configuration conf = new Configuration();
			try
			{
				WritableName.GetClass("unknown_junk", conf);
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Matches(".*unknown_junk.*"));
			}
		}
	}
}
