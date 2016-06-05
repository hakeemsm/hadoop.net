using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// TestCase for
	/// <see cref="GenericWritable"/>
	/// class.
	/// </summary>
	/// <seealso cref="TestWritable.TestWritable(Writable)"/>
	public class TestGenericWritable : TestCase
	{
		private Configuration conf;

		public const string ConfTestKey = "test.generic.writable";

		public const string ConfTestValue = "dummy";

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			conf = new Configuration();
			//set the configuration parameter
			conf.Set(ConfTestKey, ConfTestValue);
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class Foo : Writable
		{
			private string foo = "foo";

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader @in)
			{
				foo = Text.ReadString(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter @out)
			{
				Text.WriteString(@out, foo);
			}

			public override bool Equals(object obj)
			{
				if (!(obj is TestGenericWritable.Foo))
				{
					return false;
				}
				return this.foo.Equals(((TestGenericWritable.Foo)obj).foo);
			}
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class Bar : Writable, Configurable
		{
			private int bar = 42;

			private Configuration conf = null;

			//The Answer to The Ultimate Question Of Life, the Universe and Everything
			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader @in)
			{
				bar = @in.ReadInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter @out)
			{
				@out.WriteInt(bar);
			}

			public virtual Configuration GetConf()
			{
				return conf;
			}

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
			}

			public override bool Equals(object obj)
			{
				if (!(obj is TestGenericWritable.Bar))
				{
					return false;
				}
				return this.bar == ((TestGenericWritable.Bar)obj).bar;
			}
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class Baz : TestGenericWritable.Bar
		{
			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				base.ReadFields(@in);
				//needs a configuration parameter
				Assert.Equal("Configuration is not set for the wrapped object"
					, ConfTestValue, GetConf().Get(ConfTestKey));
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(BinaryWriter @out)
			{
				base.Write(@out);
			}
		}

		/// <summary>
		/// Dummy class for testing
		/// <see cref="GenericWritable"/>
		/// 
		/// </summary>
		public class FooGenericWritable : GenericWritable
		{
			protected internal override Type[] GetTypes()
			{
				return new Type[] { typeof(TestGenericWritable.Foo), typeof(TestGenericWritable.Bar
					), typeof(TestGenericWritable.Baz) };
			}

			public override bool Equals(object obj)
			{
				if (!(obj is TestGenericWritable.FooGenericWritable))
				{
					return false;
				}
				return Get().Equals(((TestGenericWritable.FooGenericWritable)obj).Get());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFooWritable()
		{
			System.Console.Out.WriteLine("Testing Writable wrapped in GenericWritable");
			TestGenericWritable.FooGenericWritable generic = new TestGenericWritable.FooGenericWritable
				();
			generic.SetConf(conf);
			TestGenericWritable.Foo foo = new TestGenericWritable.Foo();
			generic.Set(foo);
			TestWritable.TestWritable(generic);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBarWritable()
		{
			System.Console.Out.WriteLine("Testing Writable, Configurable wrapped in GenericWritable"
				);
			TestGenericWritable.FooGenericWritable generic = new TestGenericWritable.FooGenericWritable
				();
			generic.SetConf(conf);
			TestGenericWritable.Bar bar = new TestGenericWritable.Bar();
			bar.SetConf(conf);
			generic.Set(bar);
			//test writing generic writable
			TestGenericWritable.FooGenericWritable after = (TestGenericWritable.FooGenericWritable
				)TestWritable.TestWritable(generic, conf);
			//test configuration
			System.Console.Out.WriteLine("Testing if Configuration is passed to wrapped classes"
				);
			Assert.True(after.Get() is Configurable);
			NUnit.Framework.Assert.IsNotNull(((Configurable)after.Get()).GetConf());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBazWritable()
		{
			System.Console.Out.WriteLine("Testing for GenericWritable to find class names");
			TestGenericWritable.FooGenericWritable generic = new TestGenericWritable.FooGenericWritable
				();
			generic.SetConf(conf);
			TestGenericWritable.Baz baz = new TestGenericWritable.Baz();
			generic.Set(baz);
			TestWritable.TestWritable(generic, conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSet()
		{
			TestGenericWritable.Foo foo = new TestGenericWritable.Foo();
			TestGenericWritable.FooGenericWritable generic = new TestGenericWritable.FooGenericWritable
				();
			//exception should not occur
			generic.Set(foo);
			try
			{
				//exception should occur, since IntWritable is not registered
				generic = new TestGenericWritable.FooGenericWritable();
				generic.Set(new IntWritable(1));
				Fail("Generic writable should have thrown an exception for a Writable not registered"
					);
			}
			catch (RuntimeException)
			{
			}
		}

		//ignore
		/// <exception cref="System.Exception"/>
		public virtual void TestGet()
		{
			TestGenericWritable.Foo foo = new TestGenericWritable.Foo();
			TestGenericWritable.FooGenericWritable generic = new TestGenericWritable.FooGenericWritable
				();
			generic.Set(foo);
			Assert.Equal(foo, generic.Get());
		}
	}
}
