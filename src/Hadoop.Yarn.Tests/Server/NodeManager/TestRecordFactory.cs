using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestRecordFactory
	{
		[NUnit.Framework.Test]
		public virtual void TestPbRecordFactory()
		{
			RecordFactory pbRecordFactory = RecordFactoryPBImpl.Get();
			try
			{
				LocalizerHeartbeatResponse response = pbRecordFactory.NewRecordInstance<LocalizerHeartbeatResponse
					>();
				NUnit.Framework.Assert.AreEqual(typeof(LocalizerHeartbeatResponsePBImpl), response
					.GetType());
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete record");
			}
		}
	}
}
