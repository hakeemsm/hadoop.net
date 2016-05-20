using Sharpen;

namespace org.apache.hadoop.conf
{
	public class TestGetInstances : NUnit.Framework.TestCase
	{
		internal interface SampleInterface
		{
		}

		internal interface ChildInterface : org.apache.hadoop.conf.TestGetInstances.SampleInterface
		{
		}

		internal class SampleClass : org.apache.hadoop.conf.TestGetInstances.SampleInterface
		{
			internal SampleClass()
			{
			}
		}

		internal class AnotherClass : org.apache.hadoop.conf.TestGetInstances.ChildInterface
		{
			internal AnotherClass()
			{
			}
		}

		/// <summary>
		/// Makes sure <code>Configuration.getInstances()</code> returns
		/// instances of the required type.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testGetInstances()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			System.Collections.Generic.IList<org.apache.hadoop.conf.TestGetInstances.SampleInterface
				> classes = conf.getInstances<org.apache.hadoop.conf.TestGetInstances.SampleInterface
				>("no.such.property");
			NUnit.Framework.Assert.IsTrue(classes.isEmpty());
			conf.set("empty.property", string.Empty);
			classes = conf.getInstances<org.apache.hadoop.conf.TestGetInstances.SampleInterface
				>("empty.property");
			NUnit.Framework.Assert.IsTrue(classes.isEmpty());
			conf.setStrings("some.classes", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.TestGetInstances.SampleClass
				)).getName(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.TestGetInstances.AnotherClass
				)).getName());
			classes = conf.getInstances<org.apache.hadoop.conf.TestGetInstances.SampleInterface
				>("some.classes");
			NUnit.Framework.Assert.AreEqual(2, classes.Count);
			try
			{
				conf.setStrings("some.classes", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.TestGetInstances.SampleClass
					)).getName(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.TestGetInstances.AnotherClass
					)).getName(), Sharpen.Runtime.getClassForType(typeof(string)).getName());
				conf.getInstances<org.apache.hadoop.conf.TestGetInstances.SampleInterface>("some.classes"
					);
				fail("java.lang.String does not implement SampleInterface");
			}
			catch (System.Exception)
			{
			}
			try
			{
				conf.setStrings("some.classes", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.TestGetInstances.SampleClass
					)).getName(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.TestGetInstances.AnotherClass
					)).getName(), "no.such.Class");
				conf.getInstances<org.apache.hadoop.conf.TestGetInstances.SampleInterface>("some.classes"
					);
				fail("no.such.Class does not exist");
			}
			catch (System.Exception)
			{
			}
		}
	}
}
