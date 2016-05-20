using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// TestCase for
	/// <see cref="GenericWritable"/>
	/// class.
	/// </summary>
	/// <seealso cref="TestWritable.testWritable(Writable)"/>
	public class TestGenericWritable : NUnit.Framework.TestCase
	{
		private org.apache.hadoop.conf.Configuration conf;

		public const string CONF_TEST_KEY = "test.generic.writable";

		public const string CONF_TEST_VALUE = "dummy";

		/// <exception cref="System.Exception"/>
		protected override void setUp()
		{
			base.setUp();
			conf = new org.apache.hadoop.conf.Configuration();
			//set the configuration parameter
			conf.set(CONF_TEST_KEY, CONF_TEST_VALUE);
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class Foo : org.apache.hadoop.io.Writable
		{
			private string foo = "foo";

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				foo = org.apache.hadoop.io.Text.readString(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.Text.writeString(@out, foo);
			}

			public override bool Equals(object obj)
			{
				if (!(obj is org.apache.hadoop.io.TestGenericWritable.Foo))
				{
					return false;
				}
				return this.foo.Equals(((org.apache.hadoop.io.TestGenericWritable.Foo)obj).foo);
			}
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class Bar : org.apache.hadoop.io.Writable, org.apache.hadoop.conf.Configurable
		{
			private int bar = 42;

			private org.apache.hadoop.conf.Configuration conf = null;

			//The Answer to The Ultimate Question Of Life, the Universe and Everything
			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				bar = @in.readInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				@out.writeInt(bar);
			}

			public virtual org.apache.hadoop.conf.Configuration getConf()
			{
				return conf;
			}

			public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			public override bool Equals(object obj)
			{
				if (!(obj is org.apache.hadoop.io.TestGenericWritable.Bar))
				{
					return false;
				}
				return this.bar == ((org.apache.hadoop.io.TestGenericWritable.Bar)obj).bar;
			}
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class Baz : org.apache.hadoop.io.TestGenericWritable.Bar
		{
			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				base.readFields(@in);
				//needs a configuration parameter
				NUnit.Framework.Assert.AreEqual("Configuration is not set for the wrapped object"
					, CONF_TEST_VALUE, getConf().get(CONF_TEST_KEY));
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(java.io.DataOutput @out)
			{
				base.write(@out);
			}
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class FooGenericWritable : org.apache.hadoop.io.GenericWritable
		{
			protected internal override java.lang.Class[] getTypes()
			{
				return new java.lang.Class[] { Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestGenericWritable.Foo
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestGenericWritable.Bar
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestGenericWritable.Baz
					)) };
			}

			public override bool Equals(object obj)
			{
				if (!(obj is org.apache.hadoop.io.TestGenericWritable.FooGenericWritable))
				{
					return false;
				}
				return get().Equals(((org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
					)obj).get());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFooWritable()
		{
			System.Console.Out.WriteLine("Testing Writable wrapped in GenericWritable");
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				();
			generic.setConf(conf);
			org.apache.hadoop.io.TestGenericWritable.Foo foo = new org.apache.hadoop.io.TestGenericWritable.Foo
				();
			generic.set(foo);
			org.apache.hadoop.io.TestWritable.testWritable(generic);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testBarWritable()
		{
			System.Console.Out.WriteLine("Testing Writable, Configurable wrapped in GenericWritable"
				);
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				();
			generic.setConf(conf);
			org.apache.hadoop.io.TestGenericWritable.Bar bar = new org.apache.hadoop.io.TestGenericWritable.Bar
				();
			bar.setConf(conf);
			generic.set(bar);
			//test writing generic writable
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable after = (org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				)org.apache.hadoop.io.TestWritable.testWritable(generic, conf);
			//test configuration
			System.Console.Out.WriteLine("Testing if Configuration is passed to wrapped classes"
				);
			NUnit.Framework.Assert.IsTrue(after.get() is org.apache.hadoop.conf.Configurable);
			NUnit.Framework.Assert.IsNotNull(((org.apache.hadoop.conf.Configurable)after.get(
				)).getConf());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testBazWritable()
		{
			System.Console.Out.WriteLine("Testing for GenericWritable to find class names");
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				();
			generic.setConf(conf);
			org.apache.hadoop.io.TestGenericWritable.Baz baz = new org.apache.hadoop.io.TestGenericWritable.Baz
				();
			generic.set(baz);
			org.apache.hadoop.io.TestWritable.testWritable(generic, conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSet()
		{
			org.apache.hadoop.io.TestGenericWritable.Foo foo = new org.apache.hadoop.io.TestGenericWritable.Foo
				();
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				();
			//exception should not occur
			generic.set(foo);
			try
			{
				//exception should occur, since IntWritable is not registered
				generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable();
				generic.set(new org.apache.hadoop.io.IntWritable(1));
				fail("Generic writable should have thrown an exception for a Writable not registered"
					);
			}
			catch (System.Exception)
			{
			}
		}

		//ignore
		/// <exception cref="System.Exception"/>
		public virtual void testGet()
		{
			org.apache.hadoop.io.TestGenericWritable.Foo foo = new org.apache.hadoop.io.TestGenericWritable.Foo
				();
			org.apache.hadoop.io.TestGenericWritable.FooGenericWritable generic = new org.apache.hadoop.io.TestGenericWritable.FooGenericWritable
				();
			generic.set(foo);
			NUnit.Framework.Assert.AreEqual(foo, generic.get());
		}
	}
}
