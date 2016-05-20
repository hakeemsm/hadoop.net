using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for WritableName.</summary>
	public class TestWritableName : NUnit.Framework.TestCase
	{
		public TestWritableName(string name)
			: base(name)
		{
		}

		/// <summary>Example class used in test cases below.</summary>
		public class SimpleWritable : org.apache.hadoop.io.Writable
		{
			private static readonly java.util.Random RANDOM = new java.util.Random();

			internal int state = RANDOM.nextInt();

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				@out.writeInt(state);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				this.state = @in.readInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public static org.apache.hadoop.io.TestWritableName.SimpleWritable read(java.io.DataInput
				 @in)
			{
				org.apache.hadoop.io.TestWritableName.SimpleWritable result = new org.apache.hadoop.io.TestWritableName.SimpleWritable
					();
				result.readFields(@in);
				return result;
			}

			/// <summary>Required by test code, below.</summary>
			public override bool Equals(object o)
			{
				if (!(o is org.apache.hadoop.io.TestWritableName.SimpleWritable))
				{
					return false;
				}
				org.apache.hadoop.io.TestWritableName.SimpleWritable other = (org.apache.hadoop.io.TestWritableName.SimpleWritable
					)o;
				return this.state == other.state;
			}
		}

		private const string testName = "mystring";

		/// <exception cref="System.Exception"/>
		public virtual void testGoodName()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			java.lang.Class test = org.apache.hadoop.io.WritableName.getClass("long", conf);
			NUnit.Framework.Assert.IsTrue(test != null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSetName()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.WritableName.setName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.TestWritableName.SimpleWritable)), testName);
			java.lang.Class test = org.apache.hadoop.io.WritableName.getClass(testName, conf);
			NUnit.Framework.Assert.IsTrue(test.Equals(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.TestWritableName.SimpleWritable))));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAddName()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string altName = testName + ".alt";
			org.apache.hadoop.io.WritableName.setName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.TestWritableName.SimpleWritable)), testName);
			org.apache.hadoop.io.WritableName.addName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.TestWritableName.SimpleWritable)), altName);
			java.lang.Class test = org.apache.hadoop.io.WritableName.getClass(altName, conf);
			NUnit.Framework.Assert.IsTrue(test.Equals(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.TestWritableName.SimpleWritable))));
			// check original name still works
			test = org.apache.hadoop.io.WritableName.getClass(testName, conf);
			NUnit.Framework.Assert.IsTrue(test.Equals(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.TestWritableName.SimpleWritable))));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testBadName()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			try
			{
				org.apache.hadoop.io.WritableName.getClass("unknown_junk", conf);
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.matches(".*unknown_junk.*"));
			}
		}
	}
}
