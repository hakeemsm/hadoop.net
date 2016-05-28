using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a wrapper for a user defined value
	/// aggregator descriptor.
	/// </summary>
	/// <remarks>
	/// This class implements a wrapper for a user defined value
	/// aggregator descriptor.
	/// It serves two functions: One is to create an object of
	/// ValueAggregatorDescriptor from the name of a user defined class
	/// that may be dynamically loaded. The other is to
	/// delegate invocations of generateKeyValPairs function to the created object.
	/// </remarks>
	public class UserDefinedValueAggregatorDescriptor : ValueAggregatorDescriptor
	{
		private string className;

		protected internal ValueAggregatorDescriptor theAggregatorDescriptor = null;

		private static readonly Type[] argArray = new Type[] {  };

		/// <summary>Create an instance of the given class</summary>
		/// <param name="className">the name of the class</param>
		/// <returns>a dynamically created instance of the given class</returns>
		public static object CreateInstance(string className)
		{
			object retv = null;
			try
			{
				ClassLoader classLoader = Sharpen.Thread.CurrentThread().GetContextClassLoader();
				Type theFilterClass = Sharpen.Runtime.GetType(className, true, classLoader);
				Constructor<object> meth = theFilterClass.GetDeclaredConstructor(argArray);
				retv = meth.NewInstance();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			return retv;
		}

		private void CreateAggregator(Configuration conf)
		{
			if (theAggregatorDescriptor == null)
			{
				theAggregatorDescriptor = (ValueAggregatorDescriptor)CreateInstance(this.className
					);
				theAggregatorDescriptor.Configure(conf);
			}
		}

		/// <param name="className">the class name of the user defined descriptor class</param>
		/// <param name="conf">a configure object used for decriptor configuration</param>
		public UserDefinedValueAggregatorDescriptor(string className, Configuration conf)
		{
			this.className = className;
			this.CreateAggregator(conf);
		}

		/// <summary>
		/// Generate a list of aggregation-id/value pairs for the given
		/// key/value pairs by delegating the invocation to the real object.
		/// </summary>
		/// <param name="key">input key</param>
		/// <param name="val">input value</param>
		/// <returns>
		/// a list of aggregation id/value pairs. An aggregation id encodes an
		/// aggregation type which is used to guide the way to aggregate the
		/// value in the reduce/combiner phrase of an Aggregate based job.
		/// </returns>
		public override AList<KeyValuePair<Text, Text>> GenerateKeyValPairs(object key, object
			 val)
		{
			AList<KeyValuePair<Text, Text>> retv = new AList<KeyValuePair<Text, Text>>();
			if (this.theAggregatorDescriptor != null)
			{
				retv = this.theAggregatorDescriptor.GenerateKeyValPairs(key, val);
			}
			return retv;
		}

		/// <returns>the string representation of this object.</returns>
		public override string ToString()
		{
			return "UserDefinedValueAggregatorDescriptor with class name:" + "\t" + this.className;
		}

		/// <summary>Do nothing.</summary>
		public override void Configure(Configuration conf)
		{
		}
	}
}
