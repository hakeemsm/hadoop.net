using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for Writable.</summary>
	public class TestWritable : TestCase
	{
		private const string TestConfigParam = "frob.test";

		private const string TestConfigValue = "test";

		private const string TestWritableConfigParam = "test.writable";

		private const string TestWritableConfigValue = TestConfigValue;

		public TestWritable(string name)
			: base(name)
		{
		}

		/// <summary>Example class used in test cases below.</summary>
		public class SimpleWritable : Writable
		{
			private static readonly Random Random = new Random();

			internal int state = Random.Next();

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter writer)
			{
				@out.WriteInt(state);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader reader)
			{
				this.state = @in.ReadInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public static TestWritable.SimpleWritable Read(BinaryReader reader)
			{
				TestWritable.SimpleWritable result = new TestWritable.SimpleWritable();
				result.ReadFields(@in);
				return result;
			}

			/// <summary>Required by test code, below.</summary>
			public override bool Equals(object o)
			{
				if (!(o is TestWritable.SimpleWritable))
				{
					return false;
				}
				TestWritable.SimpleWritable other = (TestWritable.SimpleWritable)o;
				return this.state == other.state;
			}
		}

		public class SimpleWritableComparable : TestWritable.SimpleWritable, IWritableComparable
			<TestWritable.SimpleWritableComparable>, Configurable
		{
			private Configuration conf;

			public SimpleWritableComparable()
			{
			}

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
			}

			public virtual Configuration GetConf()
			{
				return this.conf;
			}

			public virtual int CompareTo(TestWritable.SimpleWritableComparable o)
			{
				return this.state - o.state;
			}
		}

		/// <summary>Test 1: Check that SimpleWritable.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleWritable()
		{
			TestWritable(new TestWritable.SimpleWritable());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestByteWritable()
		{
			TestWritable(new ByteWritable(unchecked((byte)128)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShortWritable()
		{
			TestWritable(new ShortWritable(unchecked((byte)256)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDoubleWritable()
		{
			TestWritable(new DoubleWritable(1.0));
		}

		/// <summary>Utility method for testing writables.</summary>
		/// <exception cref="System.Exception"/>
		public static Writable TestWritable(Writable before)
		{
			return TestWritable(before, null);
		}

		/// <summary>Utility method for testing writables.</summary>
		/// <exception cref="System.Exception"/>
		public static Writable TestWritable(Writable before, Configuration conf)
		{
			DataOutputBuffer dob = new DataOutputBuffer();
			before.Write(dob);
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), dob.GetLength());
			Writable after = (Writable)ReflectionUtils.NewInstance(before.GetType(), conf);
			after.ReadFields(dib);
			Assert.Equal(before, after);
			return after;
		}

		private class FrobComparator : WritableComparator
		{
			public FrobComparator()
				: base(typeof(TestWritable.Frob))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return 0;
			}
		}

		private class Frob : IWritableComparable<TestWritable.Frob>
		{
			static Frob()
			{
				// register default comparator
				WritableComparator.Define(typeof(TestWritable.Frob), new TestWritable.FrobComparator
					());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter writer)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader reader)
			{
			}

			public virtual int CompareTo(TestWritable.Frob o)
			{
				return 0;
			}
		}

		/// <summary>Test that comparator is defined and configured.</summary>
		/// <exception cref="System.Exception"/>
		public static void TestGetComparator()
		{
			Configuration conf = new Configuration();
			// Without conf.
			WritableComparator frobComparator = WritableComparator.Get(typeof(TestWritable.Frob
				));
			System.Diagnostics.Debug.Assert((frobComparator is TestWritable.FrobComparator));
			NUnit.Framework.Assert.IsNotNull(frobComparator.GetConf());
			NUnit.Framework.Assert.IsNull(frobComparator.GetConf().Get(TestConfigParam));
			// With conf.
			conf.Set(TestConfigParam, TestConfigValue);
			frobComparator = WritableComparator.Get(typeof(TestWritable.Frob), conf);
			System.Diagnostics.Debug.Assert((frobComparator is TestWritable.FrobComparator));
			NUnit.Framework.Assert.IsNotNull(frobComparator.GetConf());
			Assert.Equal(conf.Get(TestConfigParam), TestConfigValue);
			// Without conf. should reuse configuration.
			frobComparator = WritableComparator.Get(typeof(TestWritable.Frob));
			System.Diagnostics.Debug.Assert((frobComparator is TestWritable.FrobComparator));
			NUnit.Framework.Assert.IsNotNull(frobComparator.GetConf());
			Assert.Equal(conf.Get(TestConfigParam), TestConfigValue);
			// New conf. should use new configuration.
			frobComparator = WritableComparator.Get(typeof(TestWritable.Frob), new Configuration
				());
			System.Diagnostics.Debug.Assert((frobComparator is TestWritable.FrobComparator));
			NUnit.Framework.Assert.IsNotNull(frobComparator.GetConf());
			NUnit.Framework.Assert.IsNull(frobComparator.GetConf().Get(TestConfigParam));
		}

		/// <summary>
		/// Test a user comparator that relies on deserializing both arguments for each
		/// compare.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestShortWritableComparator()
		{
			ShortWritable writable1 = new ShortWritable((short)256);
			ShortWritable writable2 = new ShortWritable((short)128);
			ShortWritable writable3 = new ShortWritable((short)256);
			string ShouldNotMatchWithResultOne = "Result should be 1, should not match the writables";
			Assert.True(ShouldNotMatchWithResultOne, writable1.CompareTo(writable2
				) == 1);
			Assert.True(ShouldNotMatchWithResultOne, WritableComparator.Get
				(typeof(ShortWritable)).Compare(writable1, writable2) == 1);
			string ShouldNotMatchWithResultMinusOne = "Result should be -1, should not match the writables";
			Assert.True(ShouldNotMatchWithResultMinusOne, writable2.CompareTo
				(writable1) == -1);
			Assert.True(ShouldNotMatchWithResultMinusOne, WritableComparator
				.Get(typeof(ShortWritable)).Compare(writable2, writable1) == -1);
			string ShouldMatch = "Result should be 0, should match the writables";
			Assert.True(ShouldMatch, writable1.CompareTo(writable1) == 0);
			Assert.True(ShouldMatch, WritableComparator.Get(typeof(ShortWritable
				)).Compare(writable1, writable3) == 0);
		}

		/// <summary>Test that Writable's are configured by Comparator.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConfigurableWritableComparator()
		{
			Configuration conf = new Configuration();
			conf.Set(TestWritableConfigParam, TestWritableConfigValue);
			WritableComparator wc = WritableComparator.Get(typeof(TestWritable.SimpleWritableComparable
				), conf);
			TestWritable.SimpleWritableComparable key = ((TestWritable.SimpleWritableComparable
				)wc.NewKey());
			NUnit.Framework.Assert.IsNotNull(wc.GetConf());
			NUnit.Framework.Assert.IsNotNull(key.GetConf());
			Assert.Equal(key.GetConf().Get(TestWritableConfigParam), TestWritableConfigValue
				);
		}
	}
}
