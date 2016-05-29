using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	public class TestMemoryTimelineStore : TimelineStoreTestUtils
	{
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			store = new MemoryTimelineStore();
			store.Init(new YarnConfiguration());
			store.Start();
			LoadTestEntityData();
			LoadVerificationEntityData();
			LoadTestDomainData();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			store.Stop();
		}

		public virtual TimelineStore GetTimelineStore()
		{
			return store;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetSingleEntity()
		{
			base.TestGetSingleEntity();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntities()
		{
			base.TestGetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithFromId()
		{
			base.TestGetEntitiesWithFromId();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithFromTs()
		{
			base.TestGetEntitiesWithFromTs();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithPrimaryFilters()
		{
			base.TestGetEntitiesWithPrimaryFilters();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithSecondaryFilters()
		{
			base.TestGetEntitiesWithSecondaryFilters();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEvents()
		{
			base.TestGetEvents();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetDomain()
		{
			base.TestGetDomain();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetDomains()
		{
			base.TestGetDomains();
		}
	}
}
