using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <seealso cref="TestDelegatingInputFormat"/>
	public class TestMultipleInputs : TestCase
	{
		public virtual void TestAddInputPathWithFormat()
		{
			JobConf conf = new JobConf();
			MultipleInputs.AddInputPath(conf, new Path("/foo"), typeof(TextInputFormat));
			MultipleInputs.AddInputPath(conf, new Path("/bar"), typeof(KeyValueTextInputFormat
				));
			IDictionary<Path, InputFormat> inputs = MultipleInputs.GetInputFormatMap(conf);
			NUnit.Framework.Assert.AreEqual(typeof(TextInputFormat), inputs[new Path("/foo")]
				.GetType());
			NUnit.Framework.Assert.AreEqual(typeof(KeyValueTextInputFormat), inputs[new Path(
				"/bar")].GetType());
		}

		public virtual void TestAddInputPathWithMapper()
		{
			JobConf conf = new JobConf();
			MultipleInputs.AddInputPath(conf, new Path("/foo"), typeof(TextInputFormat), typeof(
				TestMultipleInputs.MapClass));
			MultipleInputs.AddInputPath(conf, new Path("/bar"), typeof(KeyValueTextInputFormat
				), typeof(TestMultipleInputs.MapClass2));
			IDictionary<Path, InputFormat> inputs = MultipleInputs.GetInputFormatMap(conf);
			IDictionary<Path, Type> maps = MultipleInputs.GetMapperTypeMap(conf);
			NUnit.Framework.Assert.AreEqual(typeof(TextInputFormat), inputs[new Path("/foo")]
				.GetType());
			NUnit.Framework.Assert.AreEqual(typeof(KeyValueTextInputFormat), inputs[new Path(
				"/bar")].GetType());
			NUnit.Framework.Assert.AreEqual(typeof(TestMultipleInputs.MapClass), maps[new Path
				("/foo")]);
			NUnit.Framework.Assert.AreEqual(typeof(TestMultipleInputs.MapClass2), maps[new Path
				("/bar")]);
		}

		internal class MapClass : Mapper<string, string, string, string>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(string key, string value, OutputCollector<string, string>
				 output, Reporter reporter)
			{
			}

			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		internal class MapClass2 : TestMultipleInputs.MapClass
		{
		}
	}
}
