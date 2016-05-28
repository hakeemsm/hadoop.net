using NUnit.Framework;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestRecordFactory
	{
		[NUnit.Framework.Test]
		public virtual void TestPbRecordFactory()
		{
			RecordFactory pbRecordFactory = RecordFactoryPBImpl.Get();
			try
			{
				CounterGroup response = pbRecordFactory.NewRecordInstance<CounterGroup>();
				NUnit.Framework.Assert.AreEqual(typeof(CounterGroupPBImpl), response.GetType());
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete record");
			}
			try
			{
				GetCountersRequest response = pbRecordFactory.NewRecordInstance<GetCountersRequest
					>();
				NUnit.Framework.Assert.AreEqual(typeof(GetCountersRequestPBImpl), response.GetType
					());
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete record");
			}
		}
	}
}
