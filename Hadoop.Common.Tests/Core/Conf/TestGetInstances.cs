using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	public class TestGetInstances : TestCase
	{
		internal interface SampleInterface
		{
		}

		internal interface ChildInterface : TestGetInstances.SampleInterface
		{
		}

		internal class SampleClass : TestGetInstances.SampleInterface
		{
			internal SampleClass()
			{
			}
		}

		internal class AnotherClass : TestGetInstances.ChildInterface
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
		public virtual void TestGetInstances()
		{
			Configuration conf = new Configuration();
			IList<TestGetInstances.SampleInterface> classes = conf.GetInstances<TestGetInstances.SampleInterface
				>("no.such.property");
			NUnit.Framework.Assert.IsTrue(classes.IsEmpty());
			conf.Set("empty.property", string.Empty);
			classes = conf.GetInstances<TestGetInstances.SampleInterface>("empty.property");
			NUnit.Framework.Assert.IsTrue(classes.IsEmpty());
			conf.SetStrings("some.classes", typeof(TestGetInstances.SampleClass).FullName, typeof(
				TestGetInstances.AnotherClass).FullName);
			classes = conf.GetInstances<TestGetInstances.SampleInterface>("some.classes");
			NUnit.Framework.Assert.AreEqual(2, classes.Count);
			try
			{
				conf.SetStrings("some.classes", typeof(TestGetInstances.SampleClass).FullName, typeof(
					TestGetInstances.AnotherClass).FullName, typeof(string).FullName);
				conf.GetInstances<TestGetInstances.SampleInterface>("some.classes");
				Fail("java.lang.String does not implement SampleInterface");
			}
			catch (RuntimeException)
			{
			}
			try
			{
				conf.SetStrings("some.classes", typeof(TestGetInstances.SampleClass).FullName, typeof(
					TestGetInstances.AnotherClass).FullName, "no.such.Class");
				conf.GetInstances<TestGetInstances.SampleInterface>("some.classes");
				Fail("no.such.Class does not exist");
			}
			catch (RuntimeException)
			{
			}
		}
	}
}
