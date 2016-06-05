using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestRecordFactory
	{
		[NUnit.Framework.Test]
		public virtual void TestPbRecordFactory()
		{
			RecordFactory pbRecordFactory = RecordFactoryPBImpl.Get();
			try
			{
				AllocateResponse response = pbRecordFactory.NewRecordInstance<AllocateResponse>();
				NUnit.Framework.Assert.AreEqual(typeof(AllocateResponsePBImpl), response.GetType(
					));
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete record");
			}
			try
			{
				AllocateRequest response = pbRecordFactory.NewRecordInstance<AllocateRequest>();
				NUnit.Framework.Assert.AreEqual(typeof(AllocateRequestPBImpl), response.GetType()
					);
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete record");
			}
		}
	}
}
