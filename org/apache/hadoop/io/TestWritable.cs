using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for Writable.</summary>
	public class TestWritable : NUnit.Framework.TestCase
	{
		private const string TEST_CONFIG_PARAM = "frob.test";

		private const string TEST_CONFIG_VALUE = "test";

		private const string TEST_WRITABLE_CONFIG_PARAM = "test.writable";

		private const string TEST_WRITABLE_CONFIG_VALUE = TEST_CONFIG_VALUE;

		public TestWritable(string name)
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
			public static org.apache.hadoop.io.TestWritable.SimpleWritable read(java.io.DataInput
				 @in)
			{
				org.apache.hadoop.io.TestWritable.SimpleWritable result = new org.apache.hadoop.io.TestWritable.SimpleWritable
					();
				result.readFields(@in);
				return result;
			}

			/// <summary>Required by test code, below.</summary>
			public override bool Equals(object o)
			{
				if (!(o is org.apache.hadoop.io.TestWritable.SimpleWritable))
				{
					return false;
				}
				org.apache.hadoop.io.TestWritable.SimpleWritable other = (org.apache.hadoop.io.TestWritable.SimpleWritable
					)o;
				return this.state == other.state;
			}
		}

		public class SimpleWritableComparable : org.apache.hadoop.io.TestWritable.SimpleWritable
			, org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.TestWritable.SimpleWritableComparable
			>, org.apache.hadoop.conf.Configurable
		{
			private org.apache.hadoop.conf.Configuration conf;

			public SimpleWritableComparable()
			{
			}

			public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			public virtual org.apache.hadoop.conf.Configuration getConf()
			{
				return this.conf;
			}

			public virtual int compareTo(org.apache.hadoop.io.TestWritable.SimpleWritableComparable
				 o)
			{
				return this.state - o.state;
			}
		}

		/// <summary>Test 1: Check that SimpleWritable.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSimpleWritable()
		{
			testWritable(new org.apache.hadoop.io.TestWritable.SimpleWritable());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testByteWritable()
		{
			testWritable(new org.apache.hadoop.io.ByteWritable(unchecked((byte)128)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testShortWritable()
		{
			testWritable(new org.apache.hadoop.io.ShortWritable(unchecked((byte)256)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDoubleWritable()
		{
			testWritable(new org.apache.hadoop.io.DoubleWritable(1.0));
		}

		/// <summary>Utility method for testing writables.</summary>
		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.io.Writable testWritable(org.apache.hadoop.io.Writable
			 before)
		{
			return testWritable(before, null);
		}

		/// <summary>Utility method for testing writables.</summary>
		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.io.Writable testWritable(org.apache.hadoop.io.Writable
			 before, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			before.write(dob);
			org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer
				();
			dib.reset(dob.getData(), dob.getLength());
			org.apache.hadoop.io.Writable after = (org.apache.hadoop.io.Writable)org.apache.hadoop.util.ReflectionUtils
				.newInstance(Sharpen.Runtime.getClassForObject(before), conf);
			after.readFields(dib);
			NUnit.Framework.Assert.AreEqual(before, after);
			return after;
		}

		private class FrobComparator : org.apache.hadoop.io.WritableComparator
		{
			public FrobComparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestWritable.Frob
					)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return 0;
			}
		}

		private class Frob : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.TestWritable.Frob
			>
		{
			static Frob()
			{
				// register default comparator
				org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.TestWritable.Frob)), new org.apache.hadoop.io.TestWritable.FrobComparator
					());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
			}

			public virtual int compareTo(org.apache.hadoop.io.TestWritable.Frob o)
			{
				return 0;
			}
		}

		/// <summary>Test that comparator is defined and configured.</summary>
		/// <exception cref="System.Exception"/>
		public static void testGetComparator()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// Without conf.
			org.apache.hadoop.io.WritableComparator frobComparator = org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestWritable.Frob
				)));
			System.Diagnostics.Debug.Assert((frobComparator is org.apache.hadoop.io.TestWritable.FrobComparator
				));
			NUnit.Framework.Assert.IsNotNull(frobComparator.getConf());
			NUnit.Framework.Assert.IsNull(frobComparator.getConf().get(TEST_CONFIG_PARAM));
			// With conf.
			conf.set(TEST_CONFIG_PARAM, TEST_CONFIG_VALUE);
			frobComparator = org.apache.hadoop.io.WritableComparator.get(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestWritable.Frob)), conf);
			System.Diagnostics.Debug.Assert((frobComparator is org.apache.hadoop.io.TestWritable.FrobComparator
				));
			NUnit.Framework.Assert.IsNotNull(frobComparator.getConf());
			NUnit.Framework.Assert.AreEqual(conf.get(TEST_CONFIG_PARAM), TEST_CONFIG_VALUE);
			// Without conf. should reuse configuration.
			frobComparator = org.apache.hadoop.io.WritableComparator.get(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestWritable.Frob)));
			System.Diagnostics.Debug.Assert((frobComparator is org.apache.hadoop.io.TestWritable.FrobComparator
				));
			NUnit.Framework.Assert.IsNotNull(frobComparator.getConf());
			NUnit.Framework.Assert.AreEqual(conf.get(TEST_CONFIG_PARAM), TEST_CONFIG_VALUE);
			// New conf. should use new configuration.
			frobComparator = org.apache.hadoop.io.WritableComparator.get(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestWritable.Frob)), new org.apache.hadoop.conf.Configuration
				());
			System.Diagnostics.Debug.Assert((frobComparator is org.apache.hadoop.io.TestWritable.FrobComparator
				));
			NUnit.Framework.Assert.IsNotNull(frobComparator.getConf());
			NUnit.Framework.Assert.IsNull(frobComparator.getConf().get(TEST_CONFIG_PARAM));
		}

		/// <summary>
		/// Test a user comparator that relies on deserializing both arguments for each
		/// compare.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testShortWritableComparator()
		{
			org.apache.hadoop.io.ShortWritable writable1 = new org.apache.hadoop.io.ShortWritable
				((short)256);
			org.apache.hadoop.io.ShortWritable writable2 = new org.apache.hadoop.io.ShortWritable
				((short)128);
			org.apache.hadoop.io.ShortWritable writable3 = new org.apache.hadoop.io.ShortWritable
				((short)256);
			string SHOULD_NOT_MATCH_WITH_RESULT_ONE = "Result should be 1, should not match the writables";
			NUnit.Framework.Assert.IsTrue(SHOULD_NOT_MATCH_WITH_RESULT_ONE, writable1.compareTo
				(writable2) == 1);
			NUnit.Framework.Assert.IsTrue(SHOULD_NOT_MATCH_WITH_RESULT_ONE, org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ShortWritable))
				).compare(writable1, writable2) == 1);
			string SHOULD_NOT_MATCH_WITH_RESULT_MINUS_ONE = "Result should be -1, should not match the writables";
			NUnit.Framework.Assert.IsTrue(SHOULD_NOT_MATCH_WITH_RESULT_MINUS_ONE, writable2.compareTo
				(writable1) == -1);
			NUnit.Framework.Assert.IsTrue(SHOULD_NOT_MATCH_WITH_RESULT_MINUS_ONE, org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ShortWritable))
				).compare(writable2, writable1) == -1);
			string SHOULD_MATCH = "Result should be 0, should match the writables";
			NUnit.Framework.Assert.IsTrue(SHOULD_MATCH, writable1.compareTo(writable1) == 0);
			NUnit.Framework.Assert.IsTrue(SHOULD_MATCH, org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ShortWritable))
				).compare(writable1, writable3) == 0);
		}

		/// <summary>Test that Writable's are configured by Comparator.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testConfigurableWritableComparator()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(TEST_WRITABLE_CONFIG_PARAM, TEST_WRITABLE_CONFIG_VALUE);
			org.apache.hadoop.io.WritableComparator wc = org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestWritable.SimpleWritableComparable
				)), conf);
			org.apache.hadoop.io.TestWritable.SimpleWritableComparable key = ((org.apache.hadoop.io.TestWritable.SimpleWritableComparable
				)wc.newKey());
			NUnit.Framework.Assert.IsNotNull(wc.getConf());
			NUnit.Framework.Assert.IsNotNull(key.getConf());
			NUnit.Framework.Assert.AreEqual(key.getConf().get(TEST_WRITABLE_CONFIG_PARAM), TEST_WRITABLE_CONFIG_VALUE
				);
		}
	}
}
