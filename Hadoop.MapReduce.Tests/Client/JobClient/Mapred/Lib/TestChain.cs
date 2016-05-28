using NUnit.Framework;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestChain
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetReducerWithReducerByValueAsTrue()
		{
			JobConf jobConf = new JobConf();
			JobConf reducerConf = new JobConf();
			Chain.SetReducer(jobConf, typeof(TestChain.MyReducer), typeof(object), typeof(object
				), typeof(object), typeof(object), true, reducerConf);
			bool reduceByValue = reducerConf.GetBoolean("chain.reducer.byValue", false);
			NUnit.Framework.Assert.AreEqual("It should set chain.reducer.byValue as true " + 
				"in reducerConf when we give value as true", true, reduceByValue);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetReducerWithReducerByValueAsFalse()
		{
			JobConf jobConf = new JobConf();
			JobConf reducerConf = new JobConf();
			Chain.SetReducer(jobConf, typeof(TestChain.MyReducer), typeof(object), typeof(object
				), typeof(object), typeof(object), false, reducerConf);
			bool reduceByValue = reducerConf.GetBoolean("chain.reducer.byValue", true);
			NUnit.Framework.Assert.AreEqual("It should set chain.reducer.byValue as false " +
				 "in reducerConf when we give value as false", false, reduceByValue);
		}

		internal interface MyReducer : Reducer<object, object, object, object>
		{
		}
	}
}
